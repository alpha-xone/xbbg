import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import { createRequire } from 'node:module';
import * as os from 'node:os';
import * as path from 'node:path';

import { resolveNativeAddon } from '../src/native/resolve-native';

const requireHere = createRequire(__filename);
const PACKAGE_DIR = path.resolve(__dirname, '..');

export interface DurationSummary {
  readonly meanMs: number;
  readonly medianMs: number;
  readonly minMs: number;
  readonly maxMs: number;
  readonly p95Ms: number | null;
  readonly p99Ms: number | null;
  readonly sampleCount: number;
}

export interface MemoryUsageMb {
  readonly arrayBuffers: number;
  readonly external: number;
  readonly heapUsed: number;
  readonly rss: number;
}

interface NativeBuildInfo {
  readonly schemaVersion: number;
  readonly artifactSha256?: string;
  readonly [key: string]: unknown;
}

interface AddonCandidate {
  readonly path: string | null;
  readonly status: string;
  readonly source: string | null;
}

function isNativeBuildInfo(value: unknown): value is NativeBuildInfo {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  const artifactSha256: unknown = 'artifactSha256' in value ? value.artifactSha256 : undefined;
  return (
    'schemaVersion' in value &&
    value.schemaVersion === 1 &&
    (artifactSha256 === undefined || typeof artifactSha256 === 'string')
  );
}

export function summarizeDurations(durations: readonly number[]): DurationSummary {
  if (durations.length === 0) {
    return {
      meanMs: 0,
      medianMs: 0,
      minMs: 0,
      maxMs: 0,
      p95Ms: null,
      p99Ms: null,
      sampleCount: 0,
    };
  }
  const sorted = [...durations].toSorted((left, right) => left - right);
  let total = 0;
  for (const duration of durations) {
    total += duration;
  }
  const mean = total / durations.length;
  return {
    meanMs: round(mean),
    medianMs: round(nearestRank(sorted, 50)),
    minMs: round(sorted[0] ?? 0),
    maxMs: round(sorted.at(-1) ?? 0),
    p95Ms: durations.length >= 20 ? round(nearestRank(sorted, 95)) : null,
    p99Ms: durations.length >= 100 ? round(nearestRank(sorted, 99)) : null,
    sampleCount: durations.length,
  };
}

function nearestRank(sorted: readonly number[], percentile: number): number {
  const rank = Math.ceil((percentile / 100) * sorted.length);
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))] ?? 0;
}

export function memoryMb(): MemoryUsageMb {
  const value = process.memoryUsage();
  return {
    arrayBuffers: bytesToMb(value.arrayBuffers),
    external: bytesToMb(value.external),
    heapUsed: bytesToMb(value.heapUsed),
    rss: bytesToMb(value.rss),
  };
}

export function collectProvenance(
  input: unknown,
  benchmarkFile: string,
  nativeAddonLoaded: boolean,
): Record<string, unknown> {
  const buildInfoPath = path.join(PACKAGE_DIR, '.native-build-info.json');
  const buildInfo = readBuildInfo(buildInfoPath);
  const addon = nativeAddonLoaded
    ? resolveAddonCandidate()
    : {
        path: null,
        source: null,
        status: 'not-loaded-by-benchmark',
      };
  const actualArtifactSha256 = addon.path === null ? null : fileSha256(addon.path);
  const expectedArtifactSha256 =
    typeof buildInfo.value === 'string' ? null : (buildInfo.value.artifactSha256 ?? null);
  const arrowVersion = packageVersion('apache-arrow/package.json');
  const packageVersionValue = packageVersion('../package.json');
  const cpu = os.cpus()[0];
  return {
    schemaVersion: 1,
    benchmarkFile,
    runtime: {
      node: process.version,
      v8: process.versions.v8,
      napi: process.versions.napi ?? 'unknown',
      execArgv: process.execArgv,
    },
    system: {
      platform: process.platform,
      arch: process.arch,
      release: os.release(),
      cpuModel: cpu?.model ?? 'unknown',
      logicalCpuCount: os.cpus().length,
    },
    artifact: {
      buildInfoPath,
      buildInfoStatus: buildInfo.status,
      buildInfo: buildInfo.value,
      addonPath: addon.path,
      addonResolutionSource: addon.source,
      attestationStatus: addon.status,
      actualSha256: actualArtifactSha256,
      expectedSha256: expectedArtifactSha256,
      hashMatches:
        actualArtifactSha256 === null || expectedArtifactSha256 === null
          ? 'unknown'
          : actualArtifactSha256 === expectedArtifactSha256,
    },
    dependencies: {
      package: packageVersionValue,
      apacheArrow: arrowVersion,
    },
    environment: {
      rustLog: process.env.RUST_LOG ?? 'unset',
      nodeOptions: process.env.NODE_OPTIONS ?? 'unset',
      blpapiRoot: process.env.BLPAPI_ROOT ?? 'unknown',
    },
    inputSha256: crypto.createHash('sha256').update(stableJson(input)).digest('hex'),
  };
}

function resolveAddonCandidate(): AddonCandidate {
  const candidates = [
    path.join(PACKAGE_DIR, 'dist', 'napi_xbbg.node'),
    path.join(PACKAGE_DIR, 'napi_xbbg.node'),
    path.join(PACKAGE_DIR, 'dist', 'napi-xbbg.node'),
  ];
  const local = candidates.find((candidate) => fs.existsSync(candidate));
  if (local !== undefined) {
    return {
      path: local,
      source: 'dist-loader-local-candidate-order',
      status: 'attested-candidate',
    };
  }
  try {
    const resolution = resolveNativeAddon(path.resolve(PACKAGE_DIR, '..'));
    if (resolution.binaryPath !== null) {
      return {
        path: resolution.binaryPath,
        source: resolution.packageName ?? resolution.key,
        status: 'attested-candidate',
      };
    }
    return { path: null, source: resolution.packageName, status: 'unresolved-explicit-unknown' };
  } catch {
    return { path: null, source: null, status: 'resolution-error-explicit-unknown' };
  }
}

function readBuildInfo(filePath: string): {
  readonly status: string;
  readonly value: NativeBuildInfo | 'unknown';
} {
  if (!fs.existsSync(filePath)) {
    return { status: 'missing-explicit-unknown', value: 'unknown' };
  }
  try {
    const value: unknown = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if (!isNativeBuildInfo(value)) {
      return { status: 'invalid-explicit-unknown', value: 'unknown' };
    }
    return { status: 'present', value };
  } catch {
    return { status: 'invalid-explicit-unknown', value: 'unknown' };
  }
}

function packageVersion(specifier: string): string {
  try {
    const value: unknown = requireHere(specifier);
    if (typeof value !== 'object' || value === null || !('version' in value)) {
      return 'unknown';
    }
    return typeof value.version === 'string' ? value.version : 'unknown';
  } catch {
    return 'not-installed';
  }
}

export function fileSha256(filePath: string): string {
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableJson).join(',')}]`;
  }
  if (typeof value === 'object' && value !== null) {
    return `{${Object.keys(value)
      .toSorted()
      .map((key) => {
        const member: unknown = Reflect.get(value, key);
        return `${JSON.stringify(key)}:${stableJson(member)}`;
      })
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

export function round(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function bytesToMb(bytes: number): number {
  return round(bytes / 1024 / 1024);
}
