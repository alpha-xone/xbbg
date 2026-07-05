import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import { nativeBinaryName } from './platform-map';

const packageDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(packageDir, '..');

interface SdkLayout {
  readonly sdkLibDir: string;
  readonly sdkRoot: string;
}

function exists(target: fs.PathLike): boolean {
  try {
    fs.accessSync(target);
    return true;
  } catch {
    return false;
  }
}

function parseVersionParts(name: string): number[] | null {
  const parts = name.split('.').map((part) => Number(part));
  return parts.every((part) => Number.isInteger(part) && part >= 0) ? parts : null;
}

function compareSdkRoots(left: string, right: string): number {
  const leftParts = parseVersionParts(path.basename(left));
  const rightParts = parseVersionParts(path.basename(right));
  if (leftParts && rightParts) {
    const length = Math.max(leftParts.length, rightParts.length);
    for (let index = 0; index < length; index += 1) {
      const leftPart = leftParts[index] ?? 0;
      const rightPart = rightParts[index] ?? 0;
      if (leftPart !== rightPart) {
        return rightPart - leftPart;
      }
    }
  }
  if (leftParts) {
    return -1;
  }
  if (rightParts) {
    return 1;
  }
  return right.localeCompare(left);
}

function resolveSdkLibDir(sdkRoot: string): string | null {
  const candidates =
    process.platform === 'darwin'
      ? [path.join(sdkRoot, 'Darwin'), path.join(sdkRoot, 'lib'), path.join(sdkRoot, 'lib64')]
      : process.platform === 'win32'
        ? [path.join(sdkRoot, 'lib'), path.join(sdkRoot, 'Lib'), path.join(sdkRoot, 'bin')]
        : [path.join(sdkRoot, 'Linux'), path.join(sdkRoot, 'lib64'), path.join(sdkRoot, 'lib')];

  return candidates.find((candidate) => exists(candidate)) ?? null;
}

function resolveSdkLayout(root: string | null | undefined): SdkLayout | null {
  if (root === undefined || root === null || root.length === 0 || !exists(root)) {
    return null;
  }

  const candidates = [path.resolve(root)];
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      candidates.push(path.join(root, entry.name));
    }
  }

  candidates.sort(compareSdkRoots);

  for (const candidate of candidates) {
    const includeDir = exists(path.join(candidate, 'include'))
      ? path.join(candidate, 'include')
      : exists(path.join(candidate, 'Include'))
        ? path.join(candidate, 'Include')
        : null;
    const libDir = resolveSdkLibDir(candidate);
    if (includeDir !== null && libDir !== null) {
      return { sdkLibDir: libDir, sdkRoot: candidate };
    }
  }

  return null;
}

function resolveSdkRoot(): SdkLayout | null {
  const blpapiRoot = process.env.BLPAPI_ROOT;
  if (blpapiRoot !== undefined && blpapiRoot.length > 0) {
    const layout = resolveSdkLayout(path.resolve(blpapiRoot));
    if (layout !== null) {
      return layout;
    }
  }

  const devSdkRoot = process.env.XBBG_DEV_SDK_ROOT;
  if (devSdkRoot !== undefined && devSdkRoot.length > 0) {
    const resolved = path.isAbsolute(devSdkRoot) ? devSdkRoot : path.resolve(repoRoot, devSdkRoot);
    const layout = resolveSdkLayout(resolved);
    if (layout !== null) {
      return layout;
    }
  }

  return resolveSdkLayout(path.join(repoRoot, 'vendor', 'blpapi-sdk'));
}

function resolveBuildArtifact(profile: 'debug' | 'release'): string {
  const ext = process.platform === 'darwin' ? 'dylib' : process.platform === 'win32' ? 'dll' : 'so';
  const prefix = process.platform === 'win32' ? '' : 'lib';
  return path.join(repoRoot, 'target', profile, `${prefix}napi_xbbg.${ext}`);
}

function fail(message: string): never {
  console.error(`js-xbbg build failed: ${message}`);
  process.exit(1);
}

function runTool(command: string, args: readonly string[], context: string): string {
  const result = spawnSync(command, args, { encoding: 'utf8' });
  if (result.error) {
    fail(`${context}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const output = [result.stdout, result.stderr]
      .filter((value): value is string => value !== undefined && value !== null && value.length > 0)
      .join('\n')
      .trim();
    fail(`${context}: ${output || `${command} exited with status ${result.status ?? 'unknown'}`}`);
  }
  return `${result.stdout ?? ''}${result.stderr ?? ''}`;
}

function stripOtoolPathSuffix(value: string): string {
  return value.replace(/\s+\(offset \d+\)$/u, '');
}

function parseDarwinRpaths(loadCommands: string): Set<string> {
  const rpaths = new Set<string>();
  let inRpath = false;

  for (const line of loadCommands.split(/\r?\n/u)) {
    const trimmed = line.trim();
    if (trimmed === 'cmd LC_RPATH') {
      inRpath = true;
      continue;
    }
    if (inRpath && trimmed.startsWith('path ')) {
      rpaths.add(stripOtoolPathSuffix(trimmed.slice('path '.length)));
      inRpath = false;
      continue;
    }
    if (inRpath && trimmed.startsWith('cmd ')) {
      inRpath = false;
    }
  }

  return rpaths;
}

function parseDarwinLinkedLibraries(output: string): string[] {
  return output
    .split(/\r?\n/u)
    .slice(1)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/\s+\([^)]*\).*$/u, ''));
}

function readDarwinLoadCommands(binaryPath: string): string {
  return runTool(
    'otool',
    ['-l', binaryPath],
    `Failed to inspect Mach-O load commands for ${binaryPath}`,
  );
}

function readDarwinLinkedLibraries(binaryPath: string): string {
  return runTool(
    'otool',
    ['-L', binaryPath],
    `Failed to inspect Mach-O linked libraries for ${binaryPath}`,
  );
}

function installNameTool(args: readonly string[], context: string): void {
  runTool('install_name_tool', args, context);
}

function startsWithPath(value: string, parent: string): boolean {
  const normalizedValue = path.resolve(value);
  const normalizedParent = path.resolve(parent);
  return (
    normalizedValue === normalizedParent ||
    normalizedValue.startsWith(`${normalizedParent}${path.sep}`)
  );
}

function isSdkBlpapiLibrary(value: string, sdkLibDir: string): boolean {
  return (
    path.isAbsolute(value) &&
    startsWithPath(value, sdkLibDir) &&
    /^libblpapi3(_64|_32)?\.(so|dylib)$/u.test(path.basename(value))
  );
}

function isForbiddenDarwinPath(value: string): boolean {
  if (!path.isAbsolute(value)) {
    return false;
  }
  if (value.startsWith('/usr/lib/') || value.startsWith('/System/Library/')) {
    return false;
  }
  return true;
}

function verifyDarwinPortableBinary(binaryPath: string): void {
  const loadCommands = readDarwinLoadCommands(binaryPath);
  const linkedLibraries = parseDarwinLinkedLibraries(readDarwinLinkedLibraries(binaryPath));
  const values = [...parseDarwinRpaths(loadCommands), ...linkedLibraries];
  const forbidden = [...new Set(values.filter((value) => isForbiddenDarwinPath(value)))];
  if (forbidden.length > 0) {
    fail(
      `Mach-O load commands for ${binaryPath} contain non-portable build paths: ${forbidden.join(', ')}`,
    );
  }
}

// Keep the published macOS addon relocatable; Bloomberg's runtime remains user-provided.
function patchDarwinNativeAddon(binaryPath: string, sdkLibDir: string): void {
  if (process.platform !== 'darwin') {
    return;
  }

  installNameTool(
    ['-id', `@rpath/${nativeBinaryName}`, binaryPath],
    `Failed to set portable install name for ${binaryPath}`,
  );

  const linkedLibraries = parseDarwinLinkedLibraries(readDarwinLinkedLibraries(binaryPath));
  for (const linkedLibrary of linkedLibraries) {
    if (!isSdkBlpapiLibrary(linkedLibrary, sdkLibDir)) {
      continue;
    }
    installNameTool(
      ['-change', linkedLibrary, `@rpath/${path.basename(linkedLibrary)}`, binaryPath],
      `Failed to rewrite Bloomberg SDK dependency for ${binaryPath}`,
    );
  }

  let rpaths = parseDarwinRpaths(readDarwinLoadCommands(binaryPath));
  for (const rpath of rpaths) {
    if (!isForbiddenDarwinPath(rpath)) {
      continue;
    }
    installNameTool(
      ['-delete_rpath', rpath, binaryPath],
      `Failed to delete non-portable rpath ${rpath} from ${binaryPath}`,
    );
  }

  rpaths = parseDarwinRpaths(readDarwinLoadCommands(binaryPath));
  for (const rpath of ['@loader_path', '@loader_path/lib']) {
    if (rpaths.has(rpath)) {
      continue;
    }
    installNameTool(
      ['-add_rpath', rpath, binaryPath],
      `Failed to add portable rpath ${rpath} to ${binaryPath}`,
    );
  }

  verifyDarwinPortableBinary(binaryPath);
}

// Release by default: this artifact feeds packaging and benchmarks, and a debug
// Addon silently bypasses the workspace release profile (fat LTO, opt-level=3,
// Panic=abort). Pass --debug for local iteration builds.
const profile: 'debug' | 'release' = process.argv.includes('--debug') ? 'debug' : 'release';
const artifactPath = resolveBuildArtifact(profile);
const outputPath = path.join(packageDir, nativeBinaryName);

const sdkLayout = resolveSdkRoot();
if (!sdkLayout) {
  fail(
    'Could not find the Bloomberg SDK. Set BLPAPI_ROOT (or XBBG_DEV_SDK_ROOT) or vendor the SDK under vendor/blpapi-sdk/<version>.',
  );
}

const { sdkRoot } = sdkLayout;
const blpapiLibDir = process.env.BLPAPI_LIB_DIR;
const sdkLibDir =
  blpapiLibDir !== undefined && blpapiLibDir.length > 0
    ? path.resolve(blpapiLibDir)
    : sdkLayout.sdkLibDir;
if (!exists(sdkLibDir)) {
  fail(`Resolved Bloomberg SDK lib dir does not exist: ${sdkLibDir}`);
}

const extraRustFlags: string[] = [];
if (process.argv.includes('--target-cpu-native')) {
  // Host-tuned build for internal deployments/benchmarks; not for published packages.
  extraRustFlags.push('-C target-cpu=native');
}
if (process.platform === 'darwin') {
  extraRustFlags.push('-C link-arg=-Wl,-headerpad_max_install_names');
}

const env: NodeJS.ProcessEnv = { ...process.env };
env.BLPAPI_ROOT = sdkRoot;
env.BLPAPI_LIB_DIR = sdkLibDir;
env.RUSTFLAGS = [process.env.RUSTFLAGS, ...extraRustFlags].filter(Boolean).join(' ').trim();

const cargoArgs = ['build', '-p', 'napi-xbbg'];
if (profile === 'release') {
  cargoArgs.push('--release');
}

const result = spawnSync('cargo', cargoArgs, {
  cwd: repoRoot,
  env,
  stdio: 'inherit',
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

if (!exists(artifactPath)) {
  fail(`Expected build artifact was not produced: ${artifactPath}`);
}

fs.copyFileSync(artifactPath, outputPath);
fs.chmodSync(outputPath, 0o755);
patchDarwinNativeAddon(outputPath, sdkLibDir);
// Record build provenance so packaging can refuse to stage a debug addon.
fs.writeFileSync(path.join(packageDir, '.native-build-profile'), `${profile}\n`);

console.log(
  `Copied ${path.relative(repoRoot, artifactPath)} -> ${path.relative(repoRoot, outputPath)}`,
);
