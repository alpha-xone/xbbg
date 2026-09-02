#!/usr/bin/env node

import {
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";

import { zipSync } from "fflate";

const FIXED_MTIME = new Date(1980, 0, 1, 0, 0, 0);
const EXECUTABLE_MEMBERS = new Set([
  "server/xbbg-mcp",
  "server/xbbg-mcp.ps1",
  "server/bin/darwin-arm64/xbbg-mcp-real",
  "server/bin/linux-amd64/xbbg-mcp-real",
  "server/bin/windows-amd64/xbbg-mcp.exe",
]);

function listFiles(root, directory = root) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...listFiles(root, path));
    } else if (entry.isFile()) {
      files.push(path);
    } else {
      throw new Error(`MCPB staging contains unsupported entry: ${relative(root, path)}`);
    }
  }
  return files;
}

function isInside(parent, child) {
  const path = relative(parent, child);
  return path === "" || (!path.startsWith(`..${sep}`) && path !== "..");
}

export function packMcpb(stagingDirectory, outputFile) {
  const staging = resolve(stagingDirectory);
  const output = resolve(outputFile);
  if (isInside(staging, output)) {
    throw new Error("MCPB output must be outside its staging directory");
  }

  const entries = Object.create(null);
  const files = listFiles(staging).sort();
  for (const path of files) {
    const member = relative(staging, path).split(sep).join("/");
    const mode = EXECUTABLE_MEMBERS.has(member) ? 0o755 : 0o644;
    entries[member] = [readFileSync(path), { os: 3, attrs: mode << 16 }];
  }

  const archive = zipSync(entries, { level: 9, mtime: FIXED_MTIME });
  mkdirSync(dirname(output), { recursive: true });
  const temporary = `${output}.tmp`;
  rmSync(temporary, { force: true });
  try {
    writeFileSync(temporary, archive);
    rmSync(output, { force: true });
    renameSync(temporary, output);
  } finally {
    rmSync(temporary, { force: true });
  }
  return output;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (process.argv.length !== 4) {
    console.error("usage: node pack.mjs <staging-directory> <output.mcpb>");
    process.exitCode = 2;
  } else {
    console.log(packMcpb(process.argv[2], process.argv[3]));
  }
}
