import type { Data, DataType } from 'apache-arrow';

import {
  Binary,
  Bool,
  DateDay,
  DateMillisecond,
  Field,
  Float32,
  Float64,
  Int8,
  Int16,
  Int32,
  Int64,
  LargeBinary,
  LargeUtf8,
  Null,
  RecordBatch,
  Schema,
  Struct,
  Table,
  TimeMillisecond,
  TimeMicrosecond,
  TimeNanosecond,
  TimeSecond,
  TimestampMillisecond,
  TimestampMicrosecond,
  TimestampNanosecond,
  TimestampSecond,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Utf8,
  makeData,
} from 'apache-arrow';

import type { NativeArrowColumn, NativeArrowZeroCopyBatch } from './napi';

const NATIVE_ARROW_BUFFERS = Symbol('@xbbg/nativeArrowBuffers');
const EMPTY_BUFFER = Buffer.alloc(0);

interface CachedArrowLayout {
  readonly fields: Field[];
  readonly types: DataType[];
  readonly structType: Struct;
}

export const NATIVE_ARROW_LAYOUT_CACHE_LIMIT = 128;

const layoutCache = new Map<string, CachedArrowLayout>();

function layoutFingerprint(columns: readonly NativeArrowColumn[]): string {
  return columns
    .map((column) =>
      [column.name, column.type, column.nullable ? '1' : '0', column.timezone ?? ''].join('\u0000'),
    )
    .join('\u0001');
}

function cachedLayout(columns: readonly NativeArrowColumn[]): CachedArrowLayout {
  const fingerprint = layoutFingerprint(columns);
  const cached = layoutCache.get(fingerprint);
  if (cached !== undefined) {
    layoutCache.delete(fingerprint);
    layoutCache.set(fingerprint, cached);
    return cached;
  }
  const types = columns.map(arrowType);
  const fields = columns.map((column, index) => {
    const type = types[index];
    if (type === undefined) {
      throw new Error(`Missing Arrow type for column ${column.name}`);
    }
    return new Field(column.name, type, column.nullable);
  });
  const created = { fields, structType: new Struct(fields), types };
  if (layoutCache.size >= NATIVE_ARROW_LAYOUT_CACHE_LIMIT) {
    const oldest = layoutCache.keys().next().value;
    if (oldest !== undefined) {
      layoutCache.delete(oldest);
    }
  }
  layoutCache.set(fingerprint, created);
  return created;
}

export function nativeArrowLayoutCacheSize(): number {
  return layoutCache.size;
}

function unsupportedNativeArrowType(type: never): never {
  throw new Error(`Unsupported native Arrow column type: ${String(type)}`);
}

interface TypedArrayConstructor<T extends ArrayBufferView> {
  readonly BYTES_PER_ELEMENT: number;
  new (buffer: ArrayBufferLike, byteOffset: number, length: number): T;
}

export function tableFromNativeArrowBatch(batch: NativeArrowZeroCopyBatch): Table {
  const retainedBuffers: Buffer[] = [];
  const cached = cachedLayout(batch.columns);
  const children = batch.columns.map((column, index) => {
    const type = cached.types[index];
    if (type === undefined) {
      throw new Error(`Missing cached Arrow type for column ${column.name}`);
    }
    return dataFromColumn(column, retainedBuffers, type);
  });

  const schema = new Schema(cached.fields, new Map(Object.entries(batch.metadata)));
  const structData = makeData({
    children,
    length: batch.numRows,
    type: cached.structType,
  });
  const table = new Table(schema, new RecordBatch(schema, structData));

  // Keep the original Node Buffer views alive. Numeric Arrow vectors retain typed
  // Views over the same backing ArrayBuffers, but keeping these Buffer objects on
  // The Table makes the NAPI external-buffer lifetime explicit.
  Object.defineProperty(table, NATIVE_ARROW_BUFFERS, {
    enumerable: false,
    value: retainedBuffers,
  });

  return table;
}

function dataFromColumn(
  column: NativeArrowColumn,
  retainedBuffers: Buffer[],
  type: DataType,
): Data {
  const nullBitmap = optionalUint8View(column.nullBitmap, retainedBuffers);
  switch (column.type) {
    case 'bool': {
      return makeData({
        type,
        length: column.length,
        nullCount: column.nullCount,
        nullBitmap,
        data: requiredUint8View(column, retainedBuffers, Math.ceil(column.length / 8)),
      } as never) as Data;
    }
    case 'binary': {
      return makeData({
        type,
        length: column.length,
        nullCount: column.nullCount,
        nullBitmap,
        valueOffsets: requiredOffsets(column, retainedBuffers),
        data: requiredUint8View(column, retainedBuffers),
      } as never) as Data;
    }
    case 'date32': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int32Array);
    }
    case 'date64': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'float32': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Float32Array);
    }
    case 'float64': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Float64Array);
    }
    case 'int8': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int8Array);
    }
    case 'int16': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int16Array);
    }
    case 'int32': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int32Array);
    }
    case 'int64': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'large_binary': {
      return makeData({
        type,
        length: column.length,
        nullCount: column.nullCount,
        nullBitmap,
        valueOffsets: requiredLargeOffsets(column, retainedBuffers),
        data: requiredUint8View(column, retainedBuffers),
      } as never) as Data;
    }
    case 'large_utf8': {
      return makeData({
        type,
        length: column.length,
        nullCount: column.nullCount,
        nullBitmap,
        valueOffsets: requiredLargeOffsets(column, retainedBuffers),
        data: requiredUint8View(column, retainedBuffers),
      } as never) as Data;
    }
    case 'null': {
      return makeData({
        type,
        length: column.length,
      } as never) as Data;
    }
    case 'time32_ms': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int32Array);
    }
    case 'time32_s': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Int32Array);
    }
    case 'time64_us': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'time64_ns': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'timestamp_ms': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'timestamp_ns': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'timestamp_s': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'timestamp_us': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigInt64Array);
    }
    case 'uint8': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Uint8Array);
    }
    case 'uint16': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Uint16Array);
    }
    case 'uint32': {
      return scalarData(column, retainedBuffers, nullBitmap, type, Uint32Array);
    }
    case 'uint64': {
      return scalarData(column, retainedBuffers, nullBitmap, type, BigUint64Array);
    }
    case 'utf8': {
      return makeData({
        type,
        length: column.length,
        nullCount: column.nullCount,
        nullBitmap,
        valueOffsets: requiredOffsets(column, retainedBuffers),
        data: requiredUint8View(column, retainedBuffers),
      } as never) as Data;
    }
  }
  return unsupportedNativeArrowType(column.type);
}

function arrowType(column: NativeArrowColumn): DataType {
  switch (column.type) {
    case 'bool': {
      return new Bool();
    }
    case 'binary': {
      return new Binary();
    }
    case 'date32': {
      return new DateDay();
    }
    case 'date64': {
      return new DateMillisecond();
    }
    case 'float32': {
      return new Float32();
    }
    case 'float64': {
      return new Float64();
    }
    case 'int8': {
      return new Int8();
    }
    case 'int16': {
      return new Int16();
    }
    case 'int32': {
      return new Int32();
    }
    case 'int64': {
      return new Int64();
    }
    case 'large_binary': {
      return new LargeBinary();
    }
    case 'large_utf8': {
      return new LargeUtf8();
    }
    case 'null': {
      return new Null();
    }
    case 'time32_ms': {
      return new TimeMillisecond();
    }
    case 'time32_s': {
      return new TimeSecond();
    }
    case 'time64_us': {
      return new TimeMicrosecond();
    }
    case 'time64_ns': {
      return new TimeNanosecond();
    }
    case 'timestamp_ms': {
      return new TimestampMillisecond(column.timezone);
    }
    case 'timestamp_ns': {
      return new TimestampNanosecond(column.timezone);
    }
    case 'timestamp_s': {
      return new TimestampSecond(column.timezone);
    }
    case 'timestamp_us': {
      return new TimestampMicrosecond(column.timezone);
    }
    case 'uint8': {
      return new Uint8();
    }
    case 'uint16': {
      return new Uint16();
    }
    case 'uint32': {
      return new Uint32();
    }
    case 'uint64': {
      return new Uint64();
    }
    case 'utf8': {
      return new Utf8();
    }
  }
  return unsupportedNativeArrowType(column.type);
}

function scalarData(
  column: NativeArrowColumn,
  retainedBuffers: Buffer[],
  nullBitmap: Uint8Array | undefined,
  type: DataType,
  ctor: TypedArrayConstructor<ArrayBufferView>,
): Data {
  return makeData({
    data: requiredTypedView(column, ctor, retainedBuffers),
    length: column.length,
    nullBitmap,
    nullCount: column.nullCount,
    type,
  } as never) as Data;
}

function requiredTypedView<T extends ArrayBufferView>(
  column: NativeArrowColumn,
  ctor: TypedArrayConstructor<T>,
  retainedBuffers: Buffer[],
): T {
  const byteLength = column.length * ctor.BYTES_PER_ELEMENT;
  const buffer = requireBuffer(column, 'data', byteLength);
  assertBufferByteLength(column, 'data', buffer, column.length, ctor.BYTES_PER_ELEMENT);
  retainedBuffers.push(buffer);
  return new ctor(buffer.buffer, buffer.byteOffset, column.length);
}

function requiredOffsets(column: NativeArrowColumn, retainedBuffers: Buffer[]): Int32Array {
  const buffer = requireBuffer(column, 'offsets');
  assertBufferByteLength(
    column,
    'offsets',
    buffer,
    column.length + 1,
    Int32Array.BYTES_PER_ELEMENT,
  );
  retainedBuffers.push(buffer);
  return new Int32Array(buffer.buffer, buffer.byteOffset, column.length + 1);
}

function requiredLargeOffsets(column: NativeArrowColumn, retainedBuffers: Buffer[]): BigInt64Array {
  const buffer = requireBuffer(column, 'offsets');
  assertBufferByteLength(
    column,
    'offsets',
    buffer,
    column.length + 1,
    BigInt64Array.BYTES_PER_ELEMENT,
  );
  retainedBuffers.push(buffer);
  return new BigInt64Array(buffer.buffer, buffer.byteOffset, column.length + 1);
}

function requiredUint8View(
  column: NativeArrowColumn,
  retainedBuffers: Buffer[],
  byteLength?: number,
): Uint8Array {
  const buffer = requireBuffer(column, 'data', byteLength);
  if (byteLength !== undefined) {
    assertBufferByteLength(column, 'data', buffer, byteLength, 1);
  }
  retainedBuffers.push(buffer);
  return new Uint8Array(buffer.buffer, buffer.byteOffset, byteLength ?? buffer.byteLength);
}

function optionalUint8View(
  buffer: Buffer | undefined,
  retainedBuffers: Buffer[],
): Uint8Array | undefined {
  if (buffer === undefined) {
    return undefined;
  }
  retainedBuffers.push(buffer);
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

function assertBufferByteLength(
  column: NativeArrowColumn,
  property: 'data' | 'offsets',
  buffer: Buffer,
  elements: number,
  bytesPerElement: number,
): void {
  if (!Number.isSafeInteger(elements) || elements < 0) {
    throw new Error(
      `native Arrow column ${column.name} has invalid ${property} element count: ${elements}`,
    );
  }
  const requiredBytes = elements * bytesPerElement;
  if (!Number.isSafeInteger(requiredBytes) || buffer.byteLength < requiredBytes) {
    throw new Error(
      `native Arrow column ${column.name} ${property} buffer is too small: ` +
        `expected at least ${requiredBytes} bytes, got ${buffer.byteLength}`,
    );
  }
}

function requireBuffer(
  column: NativeArrowColumn,
  property: 'data' | 'offsets',
  expectedByteLength?: number,
): Buffer {
  const buffer = column[property];
  if (buffer === undefined) {
    if (property === 'data' && (expectedByteLength === undefined || expectedByteLength === 0)) {
      return EMPTY_BUFFER;
    }
    throw new Error(`native Arrow column ${column.name} is missing ${property} buffer`);
  }
  return buffer;
}
