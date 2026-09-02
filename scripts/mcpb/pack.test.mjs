import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { unzipSync } from "fflate";

import { packMcpb } from "./pack.mjs";

function centralDirectoryEntries(archive) {
  let end = archive.length - 22;
  while (end >= 0 && archive.readUInt32LE(end) !== 0x06054b50) {
    end -= 1;
  }
  assert.notEqual(end, -1, "ZIP end-of-central-directory record");

  const count = archive.readUInt16LE(end + 10);
  let offset = archive.readUInt32LE(end + 16);
  const entries = [];
  for (let index = 0; index < count; index += 1) {
    assert.equal(archive.readUInt32LE(offset), 0x02014b50);
    const nameLength = archive.readUInt16LE(offset + 28);
    const extraLength = archive.readUInt16LE(offset + 30);
    const commentLength = archive.readUInt16LE(offset + 32);
    entries.push({
      name: archive.toString("utf8", offset + 46, offset + 46 + nameLength),
      creatorOs: archive.readUInt16LE(offset + 4) >> 8,
      mode: archive.readUInt32LE(offset + 38) >>> 16,
      modifiedTime: archive.readUInt16LE(offset + 12),
      modifiedDate: archive.readUInt16LE(offset + 14),
    });
    offset += 46 + nameLength + extraLength + commentLength;
  }
  return entries;
}

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "xbbg-mcpb-"));
  const staging = join(root, "staging");
  mkdirSync(join(staging, "server", "bin", "linux-amd64"), { recursive: true });
  writeFileSync(join(staging, "manifest.json"), '{"manifest_version":"0.3"}\n');
  writeFileSync(join(staging, "server", "xbbg-mcp"), "#!/bin/sh\nexit 0\n");
  writeFileSync(join(staging, "server", "bin", "linux-amd64", "xbbg-mcp-real"), "binary");
  return { root, staging };
}

test("identical content produces identical archives despite source mtimes", (context) => {
  const { root, staging } = fixture();
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const first = join(root, "first.mcpb");
  const second = join(root, "second.mcpb");

  packMcpb(staging, first);
  utimesSync(join(staging, "manifest.json"), 123_456_789, 123_456_789);
  utimesSync(join(staging, "server", "xbbg-mcp"), 987_654_321, 987_654_321);
  packMcpb(staging, second);

  assert.deepEqual(readFileSync(first), readFileSync(second));
  const extracted = unzipSync(readFileSync(first));
  assert.equal(Buffer.from(extracted["manifest.json"]).toString(), '{"manifest_version":"0.3"}\n');
});

test("archive metadata preserves fixed Unix executable modes", (context) => {
  const { root, staging } = fixture();
  context.after(() => rmSync(root, { recursive: true, force: true }));
  const output = join(root, "bundle.mcpb");

  packMcpb(staging, output);

  const entries = centralDirectoryEntries(readFileSync(output));
  assert.deepEqual(
    entries.map(({ name }) => name),
    ["manifest.json", "server/bin/linux-amd64/xbbg-mcp-real", "server/xbbg-mcp"],
  );
  for (const entry of entries) {
    assert.equal(entry.creatorOs, 3);
    assert.equal(entry.modifiedTime, 0);
    assert.equal(entry.modifiedDate, 0x21);
    assert.equal(entry.mode, entry.name === "manifest.json" ? 0o644 : 0o755);
  }
});

test("output inside staging is rejected", (context) => {
  const { root, staging } = fixture();
  context.after(() => rmSync(root, { recursive: true, force: true }));

  assert.throws(
    () => packMcpb(staging, join(staging, "bundle.mcpb")),
    /outside its staging directory/,
  );
});
