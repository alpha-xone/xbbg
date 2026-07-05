#!/usr/bin/env node

import type { Table } from 'apache-arrow';

import * as fs from 'node:fs';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import type { NativeArrowColumn, NativeArrowZeroCopyBatch } from '../src/napi';

const SCRIPT_DIR = __dirname;
const RESULTS_DIR = path.join(SCRIPT_DIR, 'results');
const DEFAULT_SHAPES: readonly Shape[] = [
  { columns: 5, rows: 100 },
  { columns: 10, rows: 10_000 },
  { columns: 10, rows: 100_000 },
];
const DEFAULT_WARMUP = envInteger('XBBG_HANDOFF_WARMUP', 2);
const DEFAULT_ITERATIONS = envInteger('XBBG_HANDOFF_ITERATIONS', 10);
const QUICK_WARMUP = 1;
const QUICK_ITERATIONS = 3;

type ScenarioName = 'tableFromNativeArrowBatch' | 'consumeRowsColumns' | 'jsonParseBaseline';

type ColumnKind = 'float64' | 'int64' | 'utf8' | 'timestamp_us';

interface Shape {
  readonly rows: number;
  readonly columns: number;
}

interface CliArgs {
  readonly help: boolean;
  readonly iterations?: number;
  readonly json: boolean;
  readonly quick: boolean;
  readonly resultsDir: string;
  readonly shapes?: readonly Shape[];
  readonly warmup?: number;
}

interface Fixture {
  readonly batch: NativeArrowZeroCopyBatch;
  readonly jsonPayload: string;
  readonly setupMs: number;
  readonly shape: Shape;
}

interface DurationSummary {
  readonly meanMs: number;
  readonly medianMs: number;
  readonly minMs: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly stdMs: number;
}

interface ScenarioResult extends DurationSummary {
  readonly addonProfile: string;
  readonly checksum: number;
  readonly columns: number;
  readonly iterations: number;
  readonly memoryMb: {
    readonly end: MemoryUsageMb;
    readonly start: MemoryUsageMb;
  };
  readonly offline: true;
  readonly rows: number;
  readonly scenario: ScenarioName;
  readonly shape: string;
  readonly setupMs: number;
  readonly warmupIterations: number;
}

interface MemoryUsageMb {
  readonly arrayBuffers: number;
  readonly external: number;
  readonly heapUsed: number;
  readonly rss: number;
}

interface BenchmarkDocument {
  readonly addonProfile: string;
  readonly benchmarkFile: string;
  outputPath?: string;
  readonly offline: true;
  readonly results: readonly ScenarioResult[];
  readonly timestamp: string;
  readonly updateModel: 'single-native-arrow-batch';
}

function usage(): string {
  return `Offline native Arrow handoff benchmark\n\nUsage:\n  npm --prefix js-xbbg run bench:handoff-offline -- [options]\n\nOptions:\n  --quick                 Run fewer iterations for smoke checks\n  --iterations <n>        Measured iterations per lane (env: XBBG_HANDOFF_ITERATIONS)\n  --warmup <n>            Warmup iterations per lane (env: XBBG_HANDOFF_WARMUP)\n  --shape <ROWSxCOLS>     Shape to benchmark; may be repeated\n  --results-dir <path>    Results directory (default: benchmarks/results)\n  --json                  Print only the JSON result document\n  --help                  Show this help\n`;
}

function parseArgs(argv: readonly string[]): CliArgs {
  let help = false;
  let iterations: number | undefined;
  let json = false;
  let quick = false;
  let resultsDir = RESULTS_DIR;
  let warmup: number | undefined;
  const shapes: Shape[] = [];

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === undefined) {
      throw new Error(`Missing argument at index ${index}`);
    }
    switch (arg) {
      case '--help':
      case '-h':
        help = true;
        break;
      case '--iterations':
        iterations = parsePositiveInteger(
          readArg(argv, (index += 1), '--iterations'),
          '--iterations',
        );
        break;
      case '--json':
        json = true;
        break;
      case '--quick':
        quick = true;
        break;
      case '--results-dir':
        resultsDir = path.resolve(readArg(argv, (index += 1), '--results-dir'));
        break;
      case '--shape':
        shapes.push(parseShape(readArg(argv, (index += 1), '--shape')));
        break;
      case '--warmup':
        warmup = parseNonNegativeInteger(readArg(argv, (index += 1), '--warmup'), '--warmup');
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return {
    help,
    iterations,
    json,
    quick,
    resultsDir,
    shapes: shapes.length > 0 ? shapes : undefined,
    warmup,
  };
}

function readArg(argv: readonly string[], index: number, name: string): string {
  const value = argv[index];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${name} requires a value`);
  }
  return value;
}

function envInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.trim() === '') {
    return fallback;
  }
  return parseNonNegativeInteger(value, name);
}

function parsePositiveInteger(value: string, name: string): number {
  const parsed = Math.trunc(Number(value));
  if (
    !Number.isFinite(parsed) ||
    parsed <= 0 ||
    String(parsed) !== value.replace(/^0+(?=\d)/u, '')
  ) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(value: string, name: string): number {
  const parsed = Math.trunc(Number(value));
  if (
    !Number.isFinite(parsed) ||
    parsed < 0 ||
    String(parsed) !== value.replace(/^0+(?=\d)/u, '')
  ) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function parseShape(value: string): Shape {
  const normalized = value.toLowerCase().replaceAll('_', '');
  const [rowsText, columnsText, extra] = normalized.split('x');
  if (rowsText === undefined || columnsText === undefined || extra !== undefined) {
    throw new Error(`Invalid shape ${value}; expected ROWSxCOLS`);
  }
  return {
    columns: parsePositiveInteger(columnsText, '--shape columns'),
    rows: parsePositiveInteger(rowsText, '--shape rows'),
  };
}

function columnKind(index: number): ColumnKind {
  switch (index % 4) {
    case 0:
      return 'float64';
    case 1:
      return 'int64';
    case 2:
      return 'utf8';
    default:
      return 'timestamp_us';
  }
}

function columnName(index: number): string {
  return `${columnKind(index)}_${String(index).padStart(2, '0')}`;
}

function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

function buildFixture(shape: Shape): Fixture {
  const started = performance.now();
  const columns = Array.from({ length: shape.columns }, (_, index) =>
    buildColumn(shape.rows, index),
  );
  const batch: NativeArrowZeroCopyBatch = {
    columns,
    kind: 'zeroCopy',
    metadata: {
      'xbbg.benchmark': 'handoff-offline',
      'xbbg.shape': `${shape.rows}x${shape.columns}`,
    },
    numRows: shape.rows,
  };
  const jsonPayload = JSON.stringify(buildRows(shape));
  return {
    batch,
    jsonPayload,
    setupMs: performance.now() - started,
    shape,
  };
}

function buildColumn(rows: number, columnIndex: number): NativeArrowColumn {
  const name = columnName(columnIndex);
  const kind = columnKind(columnIndex);
  switch (kind) {
    case 'float64': {
      const values = new Float64Array(rows);
      for (let row = 0; row < rows; row += 1) {
        values[row] = row * 0.25 + columnIndex / 10;
      }
      return scalarColumn(name, kind, rows, typedBuffer(values));
    }
    case 'int64': {
      const values = new BigInt64Array(rows);
      for (let row = 0; row < rows; row += 1) {
        values[row] = BigInt(row * 10_000 + columnIndex);
      }
      return scalarColumn(name, kind, rows, typedBuffer(values));
    }
    case 'timestamp_us': {
      const values = new BigInt64Array(rows);
      const baseMicros = BigInt(Date.UTC(2024, 0, 2, 9, 30, 0)) * 1000n;
      for (let row = 0; row < rows; row += 1) {
        values[row] = baseMicros + BigInt(row * 1000 + columnIndex);
      }
      return scalarColumn(name, kind, rows, typedBuffer(values));
    }
    case 'utf8':
      return stringColumn(name, rows, columnIndex);
  }
  return unreachableColumnKind(kind);
}

function scalarColumn(
  name: string,
  type: NativeArrowColumn['type'],
  rows: number,
  data: Buffer,
): NativeArrowColumn {
  return {
    data,
    length: rows,
    name,
    nullCount: 0,
    nullable: false,
    type,
  };
}

function stringColumn(name: string, rows: number, columnIndex: number): NativeArrowColumn {
  const offsets = new Int32Array(rows + 1);
  const chunks: Buffer[] = [];
  let byteLength = 0;
  for (let row = 0; row < rows; row += 1) {
    const chunk = Buffer.from(
      `SEC${String(row % 10_000).padStart(4, '0')}:${String(columnIndex).padStart(2, '0')}`,
    );
    chunks.push(chunk);
    byteLength += chunk.byteLength;
    offsets[row + 1] = byteLength;
  }
  return {
    data: Buffer.concat(chunks, byteLength),
    length: rows,
    name,
    nullCount: 0,
    nullable: false,
    offsets: typedBuffer(offsets),
    type: 'utf8',
  };
}

function buildRows(shape: Shape): Record<string, unknown>[] {
  const names = Array.from({ length: shape.columns }, (_, index) => columnName(index));
  return Array.from({ length: shape.rows }, (_, row) => {
    const record: Record<string, unknown> = {};
    for (let column = 0; column < shape.columns; column += 1) {
      const name = names[column];
      if (name === undefined) {
        throw new Error(`Missing generated column name at index ${column}`);
      }
      record[name] = jsonValue(row, column);
    }
    return record;
  });
}

function jsonValue(row: number, column: number): unknown {
  const kind = columnKind(column);
  switch (kind) {
    case 'float64':
      return row * 0.25 + column / 10;
    case 'int64':
      return row * 10_000 + column;
    case 'utf8':
      return `SEC${String(row % 10_000).padStart(4, '0')}:${String(column).padStart(2, '0')}`;
    case 'timestamp_us':
      return new Date(Date.UTC(2024, 0, 2, 9, 30, 0) + row).toISOString();
  }
  return unreachableColumnKind(kind);
}

function consumeRowsColumns(table: Table): number {
  const fields = table.schema.fields;
  const vectors = fields.map((field) => table.getChild(field.name));
  let checksum = 0;
  for (let row = 0; row < table.numRows; row += 1) {
    for (const vector of vectors) {
      checksum += valueChecksum(vector?.get(row));
    }
  }
  return checksum;
}

function valueChecksum(value: unknown): number {
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value % 997 : 0;
  }
  if (typeof value === 'bigint') {
    return Number(value % 997n);
  }
  if (typeof value === 'string') {
    return value.length;
  }
  if (value instanceof Date) {
    return value.getTime() % 997;
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)?.length ?? 1;
  }
  return 1;
}

function consumeJsonRows(rows: readonly Record<string, unknown>[]): number {
  let checksum = rows.length;
  for (const row of rows) {
    checksum += Object.keys(row).length;
  }
  return checksum;
}

function isJsonRecordRows(value: unknown): value is Record<string, unknown>[] {
  return (
    Array.isArray(value) &&
    value.every(
      (row): row is Record<string, unknown> =>
        typeof row === 'object' && row !== null && !Array.isArray(row),
    )
  );
}

function unreachableColumnKind(kind: never): never {
  throw new Error(`Unsupported benchmark column kind: ${String(kind)}`);
}

function unreachableScenario(scenario: never): never {
  throw new Error(`Unsupported benchmark scenario: ${String(scenario)}`);
}

function runScenario(
  fixture: Fixture,
  scenario: ScenarioName,
  iterations: number,
  warmupIterations: number,
  addonProfile: string,
): ScenarioResult {
  const durations: number[] = [];
  const startMemory = memoryMb();
  let checksum = 0;
  const totalIterations = warmupIterations + iterations;

  for (let iteration = 0; iteration < totalIterations; iteration += 1) {
    const measured = iteration >= warmupIterations;
    const before = performance.now();
    checksum += runScenarioOnce(fixture, scenario);
    const duration = performance.now() - before;
    if (measured) {
      durations.push(duration);
    }
  }

  const summary = summarizeDurations(durations);
  return {
    ...summary,
    addonProfile,
    checksum: round(checksum),
    columns: fixture.shape.columns,
    iterations,
    memoryMb: {
      end: memoryMb(),
      start: startMemory,
    },
    offline: true,
    rows: fixture.shape.rows,
    scenario,
    shape: `${fixture.shape.rows}x${fixture.shape.columns}`,
    setupMs: round(fixture.setupMs),
    warmupIterations,
  };
}

function runScenarioOnce(fixture: Fixture, scenario: ScenarioName): number {
  switch (scenario) {
    case 'tableFromNativeArrowBatch': {
      const table = tableFromNativeArrowBatch(fixture.batch);
      return table.numRows + table.schema.fields.length;
    }
    case 'consumeRowsColumns': {
      const table = tableFromNativeArrowBatch(fixture.batch);
      return consumeRowsColumns(table);
    }
    case 'jsonParseBaseline': {
      const parsed: unknown = JSON.parse(fixture.jsonPayload);
      if (!isJsonRecordRows(parsed)) {
        throw new Error('JSON baseline payload must decode to rows');
      }
      return consumeJsonRows(parsed);
    }
  }
  return unreachableScenario(scenario);
}

function summarizeDurations(durations: readonly number[]): DurationSummary {
  const sorted = [...durations].toSorted((left, right) => left - right);
  const mean =
    durations.length === 0
      ? 0
      : durations.reduce((sum, value) => sum + value, 0) / durations.length;
  return {
    meanMs: round(mean),
    medianMs: round(percentile(sorted, 50)),
    minMs: round(sorted[0] ?? 0),
    p95Ms: round(percentile(sorted, 95)),
    p99Ms: round(percentile(sorted, 99)),
    stdMs: round(standardDeviation(durations, mean)),
  };
}

function standardDeviation(values: readonly number[], mean: number): number {
  if (values.length <= 1) {
    return 0;
  }
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function percentile(sorted: readonly number[], p: number): number {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[index] ?? 0;
}

function memoryMb(): MemoryUsageMb {
  const mem = process.memoryUsage();
  return {
    arrayBuffers: bytesToMb(mem.arrayBuffers),
    external: bytesToMb(mem.external),
    heapUsed: bytesToMb(mem.heapUsed),
    rss: bytesToMb(mem.rss),
  };
}

function bytesToMb(bytes: number): number {
  return round(bytes / 1024 / 1024);
}

function round(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function detectAddonProfile(): string {
  const profilePath = path.resolve(SCRIPT_DIR, '..', '.native-build-profile');
  if (fs.existsSync(profilePath)) {
    const profile = fs.readFileSync(profilePath, 'utf8').trim();
    return profile === '' ? 'unknown' : profile;
  }
  return 'unknown';
}

function writeResults(document: BenchmarkDocument, resultsDir: string): string {
  fs.mkdirSync(resultsDir, { recursive: true });
  const stamp = new Date()
    .toISOString()
    .replace(/[-:]/gu, '')
    .replace(/\.\d{3}Z$/u, 'Z');
  const outputPath = path.join(resultsDir, `handoff_offline_${stamp}.json`);
  document.outputPath = outputPath;
  fs.writeFileSync(outputPath, JSON.stringify(document, null, 2));
  fs.writeFileSync(
    path.join(resultsDir, 'handoff_offline_latest.json'),
    JSON.stringify(document, null, 2),
  );
  return outputPath;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(usage());
    return;
  }

  const iterations = args.iterations ?? (args.quick ? QUICK_ITERATIONS : DEFAULT_ITERATIONS);
  const warmupIterations = args.warmup ?? (args.quick ? QUICK_WARMUP : DEFAULT_WARMUP);
  const shapes = args.shapes ?? DEFAULT_SHAPES;
  const addonProfile = detectAddonProfile();
  const scenarios: readonly ScenarioName[] = [
    'tableFromNativeArrowBatch',
    'consumeRowsColumns',
    'jsonParseBaseline',
  ];
  const results: ScenarioResult[] = [];

  if (!args.json) {
    console.log('Offline native Arrow handoff benchmark');
    console.log(`Iterations: ${iterations}`);
    console.log(`Warmup: ${warmupIterations}`);
    console.log(`Addon profile: ${addonProfile}`);
  }

  for (const shape of shapes) {
    if (!args.json) {
      console.log(
        `\nBuilding fixture ${shape.rows}x${shape.columns} (setup excluded from timings)...`,
      );
    }
    const fixture = buildFixture(shape);
    if (!args.json) {
      console.log(`  setup: ${round(fixture.setupMs)}ms`);
    }
    for (const scenario of scenarios) {
      const result = runScenario(fixture, scenario, iterations, warmupIterations, addonProfile);
      results.push(result);
      if (!args.json) {
        console.log(`  ${scenario}: median ${result.medianMs}ms, min ${result.minMs}ms`);
      }
    }
  }

  const document: BenchmarkDocument = {
    addonProfile,
    benchmarkFile: path.basename(__filename),
    offline: true,
    results,
    timestamp: new Date().toISOString(),
    updateModel: 'single-native-arrow-batch',
  };
  const outputPath = writeResults(document, args.resultsDir);
  if (!args.json) {
    console.log(`\nWrote JSON results: ${outputPath}`);
  }
  console.log(JSON.stringify(document, null, 2));
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
