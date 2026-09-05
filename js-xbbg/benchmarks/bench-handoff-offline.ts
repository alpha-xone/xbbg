#!/usr/bin/env node

import type { Table } from 'apache-arrow';

import * as fs from 'node:fs';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';

import { tableFromNativeArrowBatch } from '../src/arrow-zero-copy';
import type { NativeArrowColumn, NativeArrowZeroCopyBatch } from '../src/napi';
import { collectProvenance, memoryMb, round, summarizeDurations } from './benchmark-contracts';
import type { DurationSummary, MemoryUsageMb } from './benchmark-contracts';

const RESULTS_DIR = path.join(__dirname, 'results');
const DEFAULT_WARMUP = envInteger('XBBG_HANDOFF_WARMUP', 2);
const DEFAULT_ITERATIONS = envInteger('XBBG_HANDOFF_ITERATIONS', 30);
const QUICK_WARMUP = 1;
const QUICK_ITERATIONS = 3;
// Arrow validity bits; each row's bit is set once, so addition equals OR.
const ARROW_VALIDITY_BITS = [1, 2, 4, 8, 16, 32, 64, 128] as const;

type ScenarioName =
  | 'nativeBoundaryOnly'
  | 'nativeVectorCellConsumption'
  | 'nativeRowProxyCellConsumption'
  | 'jsonParseCellConsumption';
type ColumnKind = 'float64' | 'int64' | 'utf8' | 'timestamp_us';

interface ShapeCase {
  readonly name: string;
  readonly rows: number;
  readonly columns: number;
  readonly batchRows: number;
  readonly nullEvery: number;
  readonly stringBytes: number;
  readonly stringCardinality: number;
}

const DEFAULT_CASES: readonly ShapeCase[] = [
  {
    name: 'small-dense',
    rows: 100,
    columns: 5,
    batchRows: 100,
    nullEvery: 0,
    stringBytes: 12,
    stringCardinality: 100,
  },
  {
    name: 'chunked-nullable',
    rows: 4096,
    columns: 8,
    batchRows: 64,
    nullEvery: 10,
    stringBytes: 64,
    stringCardinality: 256,
  },
  {
    name: 'wide-string-heavy',
    rows: 1024,
    columns: 25,
    batchRows: 128,
    nullEvery: 2,
    stringBytes: 256,
    stringCardinality: 1024,
  },
  {
    name: 'large-mixed',
    rows: 100_000,
    columns: 10,
    batchRows: 1024,
    nullEvery: 0,
    stringBytes: 16,
    stringCardinality: 10_000,
  },
];

interface CliArgs {
  readonly help: boolean;
  readonly iterations?: number;
  readonly json: boolean;
  readonly quick: boolean;
  readonly resultsDir: string;
  readonly cases?: readonly ShapeCase[];
  readonly warmup?: number;
}

interface Fixture {
  readonly batches: readonly NativeArrowZeroCopyBatch[];
  readonly case: ShapeCase;
  readonly inputChecksum: number;
  readonly jsonPayloads: readonly string[];
  readonly setupMs: number;
}

interface JsonSerializableRow {
  toJSON(): unknown;
}

interface ScenarioResult extends DurationSummary {
  readonly batchRows: number;
  readonly checksum: number;
  readonly columns: number;
  readonly consumerScope: string;
  readonly inputChecksum: number;
  readonly memoryMb: {
    readonly after: MemoryUsageMb;
    readonly before: MemoryUsageMb;
  };
  readonly memorySampleCount: 1;
  readonly memoryScope: string;
  readonly nativeBatches: number;
  readonly nullEvery: number;
  readonly offline: true;
  readonly rows: number;
  readonly scenario: ScenarioName;
  readonly shapeCase: string;
  readonly setupMs: number;
  readonly stringBytes: number;
  readonly stringCardinality: number;
  readonly timingScope: string;
  readonly warmupIterations: number;
}

interface BenchmarkDocument {
  readonly benchmarkFile: string;
  readonly coverage: string;
  readonly offline: true;
  outputPath?: string;
  readonly provenance: Record<string, unknown>;
  readonly results: readonly ScenarioResult[];
  readonly schemaVersion: 2;
  readonly timestamp: string;
}

function usage(): string {
  return `Offline native Arrow handoff benchmark

Usage:
  npm --prefix js-xbbg run bench:handoff-offline -- [options]

Options:
  --quick                 Run three samples for the first two representative cases
  --iterations <n>        Measured samples per lane (default 30)
  --warmup <n>            Untimed warmup samples per lane (default 2)
  --shape <ROWSxCOLS>     Custom dense single-batch shape; may be repeated
  --results-dir <path>    Results directory (default: benchmarks/results)
  --json                  Print only the result document
  --help                  Show this help
`;
}

function parseArgs(argv: readonly string[]): CliArgs {
  let help = false;
  let iterations: number | undefined;
  let json = false;
  let quick = false;
  let resultsDir = RESULTS_DIR;
  let warmup: number | undefined;
  const cases: ShapeCase[] = [];
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
        cases.push(parseShape(readArg(argv, (index += 1), '--shape')));
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
    cases: cases.length === 0 ? undefined : cases,
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
  return value === undefined || value.trim() === ''
    ? fallback
    : parseNonNegativeInteger(value, name);
}

function parsePositiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function parseShape(value: string): ShapeCase {
  const [rowsText, columnsText, extra] = value.toLowerCase().replaceAll('_', '').split('x');
  if (rowsText === undefined || columnsText === undefined || extra !== undefined) {
    throw new Error(`Invalid shape ${value}; expected ROWSxCOLS`);
  }
  const rows = parsePositiveInteger(rowsText, '--shape rows');
  const columns = parsePositiveInteger(columnsText, '--shape columns');
  return {
    name: `custom-${rows}x${columns}`,
    rows,
    columns,
    batchRows: rows,
    nullEvery: 0,
    stringBytes: 16,
    stringCardinality: Math.min(rows, 10_000),
  };
}

function columnKind(index: number): ColumnKind {
  const kinds: readonly ColumnKind[] = ['float64', 'int64', 'utf8', 'timestamp_us'];
  const kind = kinds[index % kinds.length];
  if (kind === undefined) {
    throw new Error(`Missing generated column kind for ${index}`);
  }
  return kind;
}

function buildFixture(shapeCase: ShapeCase): Fixture {
  const started = performance.now();
  const rows = buildRows(shapeCase);
  const batches: NativeArrowZeroCopyBatch[] = [];
  const jsonPayloads: string[] = [];
  for (let startRow = 0; startRow < shapeCase.rows; startRow += shapeCase.batchRows) {
    const rowCount = Math.min(shapeCase.batchRows, shapeCase.rows - startRow);
    batches.push({
      columns: Array.from({ length: shapeCase.columns }, (_, columnIndex) =>
        buildColumn(shapeCase, startRow, rowCount, columnIndex),
      ),
      kind: 'zeroCopy',
      metadata: {
        'xbbg.benchmark': 'handoff-offline',
        'xbbg.case': shapeCase.name,
      },
      numRows: rowCount,
    });
    jsonPayloads.push(JSON.stringify(rows.slice(startRow, startRow + rowCount)));
  }
  return {
    batches,
    case: shapeCase,
    inputChecksum: consumeJsonCells(rows),
    jsonPayloads,
    setupMs: performance.now() - started,
  };
}

function buildColumn(
  shapeCase: ShapeCase,
  startRow: number,
  rowCount: number,
  columnIndex: number,
): NativeArrowColumn {
  const name = `${columnKind(columnIndex)}_${String(columnIndex).padStart(2, '0')}`;
  const kind = columnKind(columnIndex);
  const nullBitmap = Buffer.alloc(Math.ceil(rowCount / 8));
  let nullCount = 0;
  const valid = (localRow: number): boolean => {
    const isValid =
      shapeCase.nullEvery === 0 || (startRow + localRow + columnIndex) % shapeCase.nullEvery !== 0;
    if (isValid) {
      const byteIndex = Math.floor(localRow / ARROW_VALIDITY_BITS.length);
      nullBitmap[byteIndex] =
        (nullBitmap[byteIndex] ?? 0) +
        (ARROW_VALIDITY_BITS[localRow % ARROW_VALIDITY_BITS.length] ?? 0);
    } else {
      nullCount += 1;
    }
    return isValid;
  };

  if (kind === 'utf8') {
    const offsets = new Int32Array(rowCount + 1);
    const chunks: Buffer[] = [];
    let byteLength = 0;
    for (let localRow = 0; localRow < rowCount; localRow += 1) {
      if (valid(localRow)) {
        const value = stringValue(shapeCase, startRow + localRow, columnIndex);
        const chunk = Buffer.from(value);
        chunks.push(chunk);
        byteLength += chunk.byteLength;
      }
      offsets[localRow + 1] = byteLength;
    }
    return {
      data: Buffer.concat(chunks, byteLength),
      length: rowCount,
      name,
      nullBitmap: nullCount === 0 ? undefined : nullBitmap,
      nullCount,
      nullable: nullCount > 0,
      offsets: typedBuffer(offsets),
      type: kind,
    };
  }

  if (kind === 'float64') {
    const values = new Float64Array(rowCount);
    for (let localRow = 0; localRow < rowCount; localRow += 1) {
      if (valid(localRow)) {
        values[localRow] = (startRow + localRow) * 0.25 + columnIndex / 10;
      }
    }
    return scalarColumn(name, kind, rowCount, typedBuffer(values), nullBitmap, nullCount);
  }

  const values = new BigInt64Array(rowCount);
  const baseMicros = BigInt(Date.UTC(2024, 0, 2, 9, 30, 0)) * 1000n;
  for (let localRow = 0; localRow < rowCount; localRow += 1) {
    if (!valid(localRow)) {
      continue;
    }
    const row = startRow + localRow;
    values[localRow] =
      kind === 'int64' ? BigInt(row * 10_000 + columnIndex) : baseMicros + BigInt(row * 1000);
  }
  return scalarColumn(name, kind, rowCount, typedBuffer(values), nullBitmap, nullCount);
}

function scalarColumn(
  name: string,
  type: NativeArrowColumn['type'],
  rows: number,
  data: Buffer,
  nullBitmap: Buffer,
  nullCount: number,
): NativeArrowColumn {
  return {
    data,
    length: rows,
    name,
    nullBitmap: nullCount === 0 ? undefined : nullBitmap,
    nullCount,
    nullable: nullCount > 0,
    type,
  };
}

function typedBuffer(view: ArrayBufferView): Buffer {
  return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}

function stringValue(shapeCase: ShapeCase, row: number, column: number): string {
  const prefix = `S${row % shapeCase.stringCardinality}:${column}:`;
  return (prefix + 'x'.repeat(shapeCase.stringBytes)).slice(0, shapeCase.stringBytes);
}

function buildRows(shapeCase: ShapeCase): Record<string, unknown>[] {
  return Array.from({ length: shapeCase.rows }, (_, row) => {
    const record: Record<string, unknown> = {};
    for (let column = 0; column < shapeCase.columns; column += 1) {
      const name = `${columnKind(column)}_${String(column).padStart(2, '0')}`;
      record[name] =
        shapeCase.nullEvery > 0 && (row + column) % shapeCase.nullEvery === 0
          ? null
          : jsonValue(shapeCase, row, column);
    }
    return record;
  });
}

function jsonValue(shapeCase: ShapeCase, row: number, column: number): unknown {
  switch (columnKind(column)) {
    case 'float64':
      return row * 0.25 + column / 10;
    case 'int64':
      return row * 10_000 + column;
    case 'utf8':
      return stringValue(shapeCase, row, column);
    case 'timestamp_us':
      return new Date(Date.UTC(2024, 0, 2, 9, 30, 0) + row).toISOString();
  }
  throw new Error(`Unsupported column kind for column ${column}`);
}

function consumeVectorCells(table: Table): number {
  const vectors = table.schema.fields.map((field) => table.getChild(field.name));
  let checksum = 0;
  for (let row = 0; row < table.numRows; row += 1) {
    for (const vector of vectors) {
      checksum += valueChecksum(vector?.get(row));
    }
  }
  return checksum;
}

function isJsonSerializableRow(value: unknown): value is JsonSerializableRow {
  // Arrow row proxies expose methods through get, but their has trap only sees columns.
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'toJSON') === 'function'
  );
}

function consumeRowProxyCells(table: Table): number {
  let checksum = 0;
  for (const row of table) {
    const rowValue: unknown = row;
    if (!isJsonSerializableRow(rowValue)) {
      throw new TypeError('Arrow table yielded a row without toJSON');
    }
    const materialized: unknown = rowValue.toJSON();
    checksum += consumeObjectCells(materialized);
  }
  return checksum;
}

function consumeJsonCells(rows: readonly unknown[]): number {
  let checksum = 0;
  for (const row of rows) {
    checksum += consumeObjectCells(row);
  }
  return checksum;
}

function consumeObjectCells(row: unknown): number {
  if (typeof row !== 'object' || row === null || Array.isArray(row)) {
    throw new TypeError('benchmark row must be a JSON object');
  }
  let checksum = 0;
  for (const key of Object.keys(row)) {
    const value: unknown = Reflect.get(row, key);
    checksum += valueChecksum(value);
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
  return typeof value === 'object' ? (JSON.stringify(value)?.length ?? 1) : 1;
}

function scenarioScope(scenario: ScenarioName): string {
  switch (scenario) {
    case 'nativeBoundaryOnly':
      return 'native descriptor to Arrow Table only; no cell reads';
    case 'nativeVectorCellConsumption':
      return 'native descriptor to Arrow Table plus every cell via column vectors';
    case 'nativeRowProxyCellConsumption':
      return 'native descriptor to Arrow Table plus every cell via row proxy toJSON';
    case 'jsonParseCellConsumption':
      return 'JSON parse plus every materialized object cell';
  }
  return unsupportedScenario(scenario);
}

function unsupportedScenario(scenario: never): never {
  throw new Error(`Unsupported benchmark scenario: ${String(scenario)}`);
}

function runScenarioOnce(fixture: Fixture, scenario: ScenarioName): number {
  if (scenario === 'jsonParseCellConsumption') {
    let checksum = 0;
    for (const payload of fixture.jsonPayloads) {
      const parsed: unknown = JSON.parse(payload);
      if (!Array.isArray(parsed)) {
        throw new TypeError('benchmark JSON payload must contain an array of rows');
      }
      checksum += consumeJsonCells(parsed);
    }
    return checksum;
  }
  let checksum = 0;
  for (const batch of fixture.batches) {
    const table = tableFromNativeArrowBatch(batch);
    if (scenario === 'nativeBoundaryOnly') {
      checksum += table.numRows * 31 + table.schema.fields.length;
    } else if (scenario === 'nativeVectorCellConsumption') {
      checksum += consumeVectorCells(table);
    } else {
      checksum += consumeRowProxyCells(table);
    }
  }
  return checksum;
}

function runScenario(
  fixture: Fixture,
  scenario: ScenarioName,
  iterations: number,
  warmupIterations: number,
): ScenarioResult {
  for (let iteration = 0; iteration < warmupIterations; iteration += 1) {
    runScenarioOnce(fixture, scenario);
  }
  const durations: number[] = [];
  let checksum: number | undefined;
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const before = performance.now();
    const observed = runScenarioOnce(fixture, scenario);
    durations.push(performance.now() - before);
    if (checksum !== undefined && observed !== checksum) {
      throw new Error(`${scenario} produced an unstable checksum`);
    }
    checksum = observed;
  }

  const memoryBefore = memoryMb();
  const memoryChecksum = runScenarioOnce(fixture, scenario);
  const memoryAfter = memoryMb();
  if (checksum !== memoryChecksum) {
    throw new Error(`${scenario} memory observation changed the result`);
  }
  return {
    ...summarizeDurations(durations),
    batchRows: fixture.case.batchRows,
    checksum: checksum ?? 0,
    columns: fixture.case.columns,
    consumerScope: scenarioScope(scenario),
    inputChecksum: fixture.inputChecksum,
    memoryMb: { after: memoryAfter, before: memoryBefore },
    memorySampleCount: 1,
    memoryScope:
      'V8 process counters before/after one separate untimed run; includes heap/external/array buffers/RSS but is not a peak',
    nativeBatches: fixture.batches.length,
    nullEvery: fixture.case.nullEvery,
    offline: true,
    rows: fixture.case.rows,
    scenario,
    setupMs: round(fixture.setupMs),
    shapeCase: fixture.case.name,
    stringBytes: fixture.case.stringBytes,
    stringCardinality: fixture.case.stringCardinality,
    timingScope: 'uninstrumented warm conversion; fixture construction excluded',
    warmupIterations,
  };
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
  const cases = args.cases ?? (args.quick ? DEFAULT_CASES.slice(0, 2) : DEFAULT_CASES);
  const scenarios: readonly ScenarioName[] = [
    'nativeBoundaryOnly',
    'nativeVectorCellConsumption',
    'nativeRowProxyCellConsumption',
    'jsonParseCellConsumption',
  ];
  const results: ScenarioResult[] = [];
  for (const shapeCase of cases) {
    const fixture = buildFixture(shapeCase);
    for (const scenario of scenarios) {
      const result = runScenario(fixture, scenario, iterations, warmupIterations);
      results.push(result);
      if (!args.json) {
        console.log(
          `${shapeCase.name}/${scenario}: median=${result.medianMs}ms max=${result.maxMs}ms n=${result.sampleCount}`,
        );
      }
    }
  }
  const document: BenchmarkDocument = {
    benchmarkFile: path.basename(__filename),
    coverage:
      'synthetic Arrow descriptors and JS consumers only; no native SDK event or network coverage',
    offline: true,
    provenance: collectProvenance(cases, path.basename(__filename), false),
    results,
    schemaVersion: 2,
    timestamp: new Date().toISOString(),
  };
  const outputPath = writeResults(document, args.resultsDir);
  if (!args.json) {
    console.log(`Wrote JSON results: ${outputPath}`);
  }
  console.log(JSON.stringify(document, null, 2));
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
