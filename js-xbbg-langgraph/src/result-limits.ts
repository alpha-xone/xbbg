import type { BloombergToolName } from "./options";

interface LimitResult {
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly value: unknown;
}

export interface ToolEnvelope {
  readonly tool: BloombergToolName;
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly data: unknown;
}

export type ToolContentAndArtifact = [string, ToolEnvelope];

interface LimitState {
  truncated: boolean;
}

const MAX_RESULT_DEPTH = 32;
/** Maximum aggregate EIDs retained and accepted by entitlement checks. */
export const MAX_ENTITLEMENT_EIDS = 10_000;
export const MAX_BLOOMBERG_EID = 2_147_483_647;
const MAX_EID_SECURITIES = 1_000;
const MAX_EID_SECURITY_NAME_BYTES = 65_536;
const UTF8_ENCODER = new TextEncoder();

function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function truncateString(value: string, maxStringChars: number, state: LimitState): string {
  if (value.length <= maxStringChars) {
    return value;
  }
  state.truncated = true;
  return `${value.slice(0, maxStringChars)}…[truncated ${value.length - maxStringChars} chars]`;
}

function arrayMetadata(value: readonly unknown[]): Record<string, unknown> {
  const metadata: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (!/^(?:0|[1-9]\d*)$/u.test(key)) {
      metadata[key] = entry;
    }
  }
  return metadata;
}

interface EidDataTruncation {
  readonly totalSecurityCount: number;
  readonly retainedSecurityCount: number;
  readonly omittedSecurityCount: number;
  readonly invalidSecurityCount: number;
  readonly totalEidCount: number;
  readonly retainedEidCount: number;
  /** Counts align by index with Object.keys(eidData), avoiding duplicate security-name bytes. */
  readonly securityCounts: readonly { originalCount: number; retainedCount: number }[];
}

interface LimitedEidData {
  readonly data: unknown;
  readonly truncation?: EidDataTruncation;
}

function limitEidData(
  value: unknown,
  maxStringChars: number,
  state: LimitState,
  depth: number,
  seen: WeakSet<object>,
): LimitedEidData {
  if (typeof value !== "object" || value === null || !isPlainObject(value)) {
    state.truncated = true;
    return {
      data: {},
      truncation: {
        invalidSecurityCount: 1,
        omittedSecurityCount: 0,
        retainedEidCount: 0,
        retainedSecurityCount: 0,
        securityCounts: [],
        totalEidCount: 0,
        totalSecurityCount: 1,
      },
    };
  }
  const data: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
  const securityCounts: { originalCount: number; retainedCount: number }[] = [];
  const entries = Object.entries(value);
  let retainedEidCount = 0;
  let retainedSecurityCount = 0;
  let retainedSecurityNameBytes = 0;
  let totalEidCount = 0;
  let invalidSecurityCount = 0;
  for (const [security, eids] of entries) {
    if (!Array.isArray(eids)) {
      state.truncated = true;
      invalidSecurityCount += 1;
      continue;
    }
    let validEids = true;
    for (let index = 0; index < eids.length; index += 1) {
      const eid: unknown = eids[index];
      if (
        !Object.hasOwn(eids, index) ||
        typeof eid !== "number" ||
        !Number.isInteger(eid) ||
        eid <= 0 ||
        eid > MAX_BLOOMBERG_EID
      ) {
        validEids = false;
        break;
      }
    }
    if (!validEids) {
      state.truncated = true;
      invalidSecurityCount += 1;
      continue;
    }
    const originalCount = eids.length;
    totalEidCount += originalCount;
    const securityNameBytes = UTF8_ENCODER.encode(security).byteLength;
    const canRetainSecurity =
      retainedSecurityCount < MAX_EID_SECURITIES &&
      retainedSecurityNameBytes + securityNameBytes <= MAX_EID_SECURITY_NAME_BYTES;
    if (!canRetainSecurity) {
      state.truncated = true;
      continue;
    }
    retainedSecurityCount += 1;
    retainedSecurityNameBytes += securityNameBytes;
    const remainingEidCapacity = Math.max(0, MAX_ENTITLEMENT_EIDS - retainedEidCount);
    const retained = eids.slice(0, remainingEidCapacity);
    retainedEidCount += retained.length;
    data[security] = limitValue(retained, MAX_ENTITLEMENT_EIDS, maxStringChars, state, depth, seen);
    securityCounts.push({
      originalCount,
      retainedCount: retained.length,
    });
    if (retained.length !== originalCount) {
      state.truncated = true;
    }
  }
  const omittedSecurityCount = entries.length - retainedSecurityCount - invalidSecurityCount;
  const wasTruncated =
    retainedSecurityCount !== entries.length ||
    retainedEidCount !== totalEidCount ||
    invalidSecurityCount > 0;
  return {
    data,
    ...(wasTruncated
      ? {
          truncation: {
            invalidSecurityCount,
            omittedSecurityCount,
            retainedEidCount,
            retainedSecurityCount,
            securityCounts,
            totalEidCount,
            totalSecurityCount: entries.length,
          },
        }
      : {}),
  };
}

function limitValue(
  value: unknown,
  maxRows: number,
  maxStringChars: number,
  state: LimitState,
  depth = 0,
  seen: WeakSet<object> = new WeakSet<object>(),
): unknown {
  if (typeof value === "string") {
    return truncateString(value, maxStringChars, state);
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (depth > MAX_RESULT_DEPTH) {
    state.truncated = true;
    return "[Max result depth exceeded]";
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) {
      state.truncated = true;
      return "[Circular]";
    }
    seen.add(value);
    const capped = value.length > maxRows ? value.slice(0, maxRows) : value;
    if (capped.length !== value.length) {
      state.truncated = true;
    }
    const rows = capped.map((item) =>
      limitValue(item, maxRows, maxStringChars, state, depth + 1, seen),
    );
    const metadata = arrayMetadata(value);
    if (Object.keys(metadata).length === 0) {
      return rows;
    }
    const output: Record<string, unknown> = { rows };
    for (const [key, entry] of Object.entries(metadata)) {
      if (key === "eidData") {
        const limited = limitEidData(entry, maxStringChars, state, depth + 1, seen);
        output.eidData = limited.data;
        if (limited.truncation !== undefined) {
          output.eidDataTruncation = limited.truncation;
        }
        continue;
      }
      output[key] = limitValue(entry, maxRows, maxStringChars, state, depth + 1, seen);
    }
    return output;
  }
  if (typeof value === "object" && value !== null) {
    if (seen.has(value)) {
      state.truncated = true;
      return "[Circular]";
    }
    if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
      state.truncated = true;
      return `[binary data: ${value.byteLength} bytes]`;
    }
    if (!isPlainObject(value)) {
      const toJSON = (value as Record<string, unknown>).toJSON;
      if (typeof toJSON === "function") {
        seen.add(value);
        return limitValue(toJSON.call(value), maxRows, maxStringChars, state, depth + 1, seen);
      }
      return value;
    }
    seen.add(value);
    const output: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      output[key] = limitValue(entry, maxRows, maxStringChars, state, depth + 1, seen);
    }
    return output;
  }
  return value;
}

function rowCountOf(value: unknown): number | null {
  if (Array.isArray(value)) {
    return value.length;
  }
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const record = value as Record<string, unknown>;
  const rowCount = record.rowCount;
  if (typeof rowCount === "number" && Number.isInteger(rowCount) && rowCount >= 0) {
    return rowCount;
  }
  const updateCount = record.updateCount;
  if (typeof updateCount === "number" && Number.isInteger(updateCount) && updateCount >= 0) {
    return updateCount;
  }
  return null;
}

const ERROR_SHAPE_KEYS = new Set([
  "error",
  "errors",
  "fielderrors",
  "fieldexception",
  "fieldexceptions",
  "responseerror",
  "securityerror",
]);

function hasErrorShape(value: unknown): boolean {
  const pending: unknown[] = [value];
  const seen = new WeakSet<object>();

  while (pending.length > 0) {
    const entry = pending.pop();
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    if (seen.has(entry)) {
      continue;
    }
    seen.add(entry);

    if (Array.isArray(entry)) {
      for (const child of entry as readonly unknown[]) {
        pending.push(child);
      }
      continue;
    }

    for (const [key, child] of Object.entries(entry as Record<string, unknown>)) {
      if (ERROR_SHAPE_KEYS.has(key.toLowerCase()) && child !== undefined) {
        return true;
      }
      pending.push(child);
    }
  }

  return false;
}

export function limitResult(value: unknown, maxRows: number, maxStringChars: number): LimitResult {
  const state: LimitState = { truncated: false };
  const rowCount = rowCountOf(value);
  const limitedValue = limitValue(value, maxRows, maxStringChars, state);
  return {
    rowCount,
    truncated: state.truncated,
    value: limitedValue,
  };
}

function summarizeEnvelope(envelope: ToolEnvelope): string {
  const rowText =
    envelope.rowCount === null
      ? "row count unknown"
      : `${envelope.rowCount} row${envelope.rowCount === 1 ? "" : "s"}`;
  const notes: string[] = [];
  if (envelope.rowCount === 0 || envelope.data === null || envelope.data === undefined) {
    notes.push(
      "empty result; verify identifiers, fields, and date range before concluding no data exists",
    );
  }
  if (envelope.truncated) {
    notes.push("artifact truncated to configured limits");
  }
  if (hasErrorShape(envelope.data)) {
    notes.push("inspect result payload for Bloomberg error details");
  }
  const noteText = notes.length === 0 ? "" : `; ${notes.join("; ")}`;
  return `${envelope.tool}: ${rowText}; truncated=${String(envelope.truncated)}${noteText}`;
}

function resultJsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
}

function formatToolContent(envelope: ToolEnvelope): string {
  const payload = {
    tool: envelope.tool,
    rowCount: envelope.rowCount,
    truncated: envelope.truncated,
    data: envelope.data,
  };
  return `${summarizeEnvelope(envelope)}\n${JSON.stringify(payload, resultJsonReplacer)}`;
}

export function createToolResult(
  tool: BloombergToolName,
  value: unknown,
  maxRows: number,
  maxStringChars: number,
): ToolContentAndArtifact {
  const limited = limitResult(value, maxRows, maxStringChars);
  const envelope: ToolEnvelope = {
    tool,
    rowCount: limited.rowCount,
    truncated: limited.truncated,
    data: limited.value,
  };
  return [formatToolContent(envelope), envelope];
}

export function throwWithToolContext(tool: BloombergToolName, error: unknown): never {
  const prefix = `${tool} failed`;
  if (error instanceof Error) {
    if (error.message.startsWith(prefix)) {
      throw error;
    }
    // Never mutate the original: a memoized connect rejection delivers the
    // same Error instance to every concurrently pending tool call.
    const wrapped = new Error(`${prefix}: ${error.message}`, { cause: error });
    wrapped.name = error.name;
    throw wrapped;
  }
  throw new Error(`${prefix}: ${String(error)}`);
}
