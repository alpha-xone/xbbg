import type { BloombergToolName } from "./options";

export interface ResultLimitOptions {
  readonly maxResultBytes: number;
  readonly maxResultNodes: number;
  readonly maxRows: number;
  readonly maxStringChars: number;
}

export interface ToolResultLimitOptions extends ResultLimitOptions {
  readonly maxContentBytes: number;
  readonly maxContentRows: number;
}

interface ToolResultWorkBudgetOptions {
  readonly materializedNodes?: number;
}

type ToolResultBuildOptions = ToolResultLimitOptions & ToolResultWorkBudgetOptions;

export type ResultTruncationReason =
  | "accessor_omitted"
  | "binary_data"
  | "circular_reference"
  | "entitlement_limit"
  | "invalid_entitlement_data"
  | "max_result_bytes"
  | "max_result_depth"
  | "max_result_nodes"
  | "max_rows"
  | "max_string_chars"
  | "unsupported_value"
  | "upstream_truncation";

export interface ResultTruncationSummary {
  readonly reasons: readonly ResultTruncationReason[];
  readonly retainedNodes?: number;
  readonly inspectedNodes?: number;
  readonly omittedPropertiesAtLeast?: number;
  readonly omittedRows?: number;
}

export interface LimitResult {
  readonly byteLength: number;
  readonly inspectedNodes: number;
  readonly maximumArrayRows: number;
  readonly retainedRows: number;
  readonly errorDiagnostics: readonly Readonly<Record<string, unknown>>[];
  readonly hasErrors: boolean;
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly truncation?: ResultTruncationSummary;
  readonly value: unknown;
}

export interface ToolEnvelope {
  readonly tool: BloombergToolName;
  readonly rowCount: number | null;
  readonly truncated: boolean;
  readonly truncation?: ResultTruncationSummary;
  readonly hasErrors?: true;
  readonly data: unknown;
}

export type ToolContentAndArtifact = [string, ToolEnvelope];

interface LimitState {
  readonly ancestors: WeakSet<object>;
  readonly diagnostics: Readonly<Record<string, unknown>>[];
  readonly limits: ResultLimitOptions;
  readonly reasons: Set<ResultTruncationReason>;
  readonly rowsBeforeMetadata: boolean;
  maximumArrayRows: number;
  hasErrors: boolean;
  omittedPropertiesAtLeast: number;
  remainingRows: number;
  omittedRows: number;
  retainedNodes: number;
  retainedRows: number;
  visitedNodes: number;
}

interface BuiltValue {
  readonly byteLength: number;
  readonly value: unknown;
}

interface PreparedEidData {
  readonly data: Record<string, unknown>;
  readonly invalidSecurityCount: number;
  readonly scannedSecurityCount: number;
  readonly securityCounts: readonly { originalCount: number; retainedCount: number }[];
  readonly totalEidCount: number | null;
  readonly totalSecurityCount: number | null;
  readonly truncation?: EidDataTruncation;
}

interface EidDataTruncation {
  readonly totalSecurityCount: number | null;
  readonly retainedSecurityCount: number;
  readonly omittedSecurityCount: number | null;
  readonly invalidSecurityCount: number;
  readonly scannedSecurityCount: number;
  readonly totalEidCount: number | null;
  readonly retainedEidCount: number;
  /** Counts align by index with Object.keys(eidData), avoiding duplicate security-name bytes. */
  readonly securityCounts: readonly { originalCount: number | null; retainedCount: number }[];
}

interface ObjectAccumulator {
  byteLength: number;
  propertyCount: number;
  readonly value: Record<string, unknown>;
}

const OMIT = Symbol("omit_result_value");
const MAX_RESULT_DEPTH = 32;
const MIN_TOOL_RESULT_NODES = 10;
const MAX_ERROR_DIAGNOSTICS = 8;
const RESULT_ENVELOPE_RESERVE_BYTES = 768;
const CONTENT_ENVELOPE_RESERVE_BYTES = 768;
const MIN_TOOL_RESULT_BYTES = 256;
/** Maximum aggregate EIDs retained and accepted by entitlement checks. */
export const MAX_ENTITLEMENT_EIDS = 10_000;
export const MAX_BLOOMBERG_EID = 2_147_483_647;
const MAX_EID_SECURITIES = 1_000;
const MAX_EID_SECURITY_NAME_BYTES = 65_536;
const UTF8_ENCODER = new TextEncoder();

const TRUNCATION_REASON_ORDER: readonly ResultTruncationReason[] = [
  "max_rows",
  "max_string_chars",
  "max_result_bytes",
  "max_result_nodes",
  "max_result_depth",
  "circular_reference",
  "binary_data",
  "accessor_omitted",
  "unsupported_value",
  "invalid_entitlement_data",
  "entitlement_limit",
  "upstream_truncation",
];

const ERROR_SHAPE_KEYS: Readonly<Record<string, true>> = {
  error: true,
  errors: true,
  fielderrors: true,
  fieldexception: true,
  fieldexceptions: true,
  responseerror: true,
  responseerrors: true,
  securityerror: true,
  securityerrors: true,
  unsubscribeerror: true,
};

/**
 * Known diagnostic keys are probed before ordinary properties. This preserves
 * Bloomberg errors and entitlement metadata even when an earlier, very wide
 * object branch exhausts the aggregate budget. Remaining keys retain their
 * original enumeration order.
 */
const PRIORITY_KEYS = [
  "error",
  "errors",
  "responseError",
  "responseErrors",
  "securityError",
  "securityErrors",
  "fieldException",
  "fieldExceptions",
  "fieldErrors",
  "unsubscribeError",
  "truncated",
  "truncatedInput",
  "eidData",
  "eidDataTruncation",
  "diagnostics",
  "metadata",
] as const;

function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function addReason(state: LimitState, reason: ResultTruncationReason): void {
  state.reasons.add(reason);
}

function consumeVisit(state: LimitState): boolean {
  if (state.visitedNodes >= state.limits.maxResultNodes) {
    addReason(state, "max_result_nodes");
    return false;
  }
  state.visitedNodes += 1;
  return true;
}

function consumeVisitBefore(state: LimitState, limit: number): boolean {
  if (state.visitedNodes >= limit) {
    addReason(state, "max_result_nodes");
    return false;
  }
  return consumeVisit(state);
}

function projectionBudget(totalBytes: number, preferredReserveBytes: number): number {
  const reserve = Math.min(preferredReserveBytes, Math.floor(totalBytes / 2));
  return Math.max(4, totalBytes - reserve);
}
function jsonStringUnit(
  value: string,
  index: number,
): { readonly bytes: number; readonly width: number } {
  const code = value.charCodeAt(index);
  if (
    code === 0x22 ||
    code === 0x5c ||
    code === 0x08 ||
    code === 0x0c ||
    code === 0x0a ||
    code === 0x0d ||
    code === 0x09
  ) {
    return { bytes: 2, width: 1 };
  }
  if (code <= 0x1f) {
    return { bytes: 6, width: 1 };
  }
  if (code <= 0x7f) {
    return { bytes: 1, width: 1 };
  }
  if (code <= 0x7ff) {
    return { bytes: 2, width: 1 };
  }
  if (code >= 0xd800 && code <= 0xdbff) {
    const next = value.charCodeAt(index + 1);
    if (next >= 0xdc00 && next <= 0xdfff) {
      return { bytes: 4, width: 2 };
    }
    return { bytes: 6, width: 1 };
  }
  if (code >= 0xdc00 && code <= 0xdfff) {
    return { bytes: 6, width: 1 };
  }
  return { bytes: 3, width: 1 };
}

function jsonStringByteLength(value: string, stopAfter = Number.MAX_SAFE_INTEGER): number {
  let byteLength = 2;
  for (let index = 0; index < value.length;) {
    const unit = jsonStringUnit(value, index);
    byteLength += unit.bytes;
    if (byteLength > stopAfter) {
      return stopAfter + 1;
    }
    index += unit.width;
  }
  return byteLength;
}

function utf8ByteLengthAtMost(value: string, maximum: number): number | null {
  let byteLength = 0;
  for (let index = 0; index < value.length;) {
    const code = value.charCodeAt(index);
    let width = 1;
    let bytes: number;
    if (code <= 0x7f) {
      bytes = 1;
    } else if (code <= 0x7ff) {
      bytes = 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes = 4;
        width = 2;
      } else {
        bytes = 3;
      }
    } else {
      bytes = 3;
    }
    byteLength += bytes;
    if (byteLength > maximum) {
      return null;
    }
    index += width;
  }
  return byteLength;
}

function safePrefixEnd(value: string, requestedEnd: number): number {
  if (
    requestedEnd > 0 &&
    requestedEnd < value.length &&
    value.charCodeAt(requestedEnd - 1) >= 0xd800 &&
    value.charCodeAt(requestedEnd - 1) <= 0xdbff &&
    value.charCodeAt(requestedEnd) >= 0xdc00 &&
    value.charCodeAt(requestedEnd) <= 0xdfff
  ) {
    return requestedEnd - 1;
  }
  return requestedEnd;
}

function fitString(
  value: string,
  maximumJsonBytes: number,
  state: LimitState,
): BuiltValue | typeof OMIT {
  const charLimit = safePrefixEnd(value, Math.min(value.length, state.limits.maxStringChars));
  if (charLimit < value.length) {
    addReason(state, "max_string_chars");
  }
  const suffix = `…[truncated ${value.length - charLimit} chars]`;
  const characterLimited =
    charLimit === value.length ? value : `${value.slice(0, charLimit)}${suffix}`;
  const characterLimitedBytes = jsonStringByteLength(characterLimited, maximumJsonBytes);
  if (characterLimitedBytes <= maximumJsonBytes) {
    state.retainedNodes += 1;
    return { byteLength: characterLimitedBytes, value: characterLimited };
  }

  addReason(state, "max_result_bytes");
  const markerOnly = `…[truncated ${value.length} chars]`;
  const markerOnlyBytes = jsonStringByteLength(markerOnly, maximumJsonBytes);
  if (markerOnlyBytes > maximumJsonBytes) {
    if (maximumJsonBytes < 2) {
      return OMIT;
    }
    state.retainedNodes += 1;
    return { byteLength: 2, value: "" };
  }

  // Sixty-four bytes safely covers the fixed marker plus every possible JS
  // string-length digit. The scan therefore stops at the configured byte
  // budget instead of walking a multi-megabyte string just to cut it later.
  const prefixBudget = Math.max(0, maximumJsonBytes - 2 - 64);
  let prefixBytes = 0;
  let prefixEnd = 0;
  while (prefixEnd < charLimit) {
    const unit = jsonStringUnit(value, prefixEnd);
    if (prefixBytes + unit.bytes > prefixBudget) {
      break;
    }
    prefixBytes += unit.bytes;
    prefixEnd += unit.width;
  }
  const fittedSuffix = `…[truncated ${value.length - prefixEnd} chars]`;
  const fitted = `${value.slice(0, prefixEnd)}${fittedSuffix}`;
  const fittedBytes = jsonStringByteLength(fitted, maximumJsonBytes);
  if (fittedBytes > maximumJsonBytes) {
    state.retainedNodes += 1;
    return { byteLength: markerOnlyBytes, value: markerOnly };
  }
  state.retainedNodes += 1;
  return { byteLength: fittedBytes, value: fitted };
}

function defineJsonProperty(target: Record<string, unknown>, key: string, value: unknown): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

function ownEnumerableDescriptor(value: object, key: string): PropertyDescriptor | undefined {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor?.enumerable === true ? descriptor : undefined;
  } catch {
    return undefined;
  }
}

function rememberErrorDiagnostic(state: LimitState, key: string, value: unknown): void {
  state.hasErrors = true;
  if (state.diagnostics.length >= MAX_ERROR_DIAGNOSTICS) {
    return;
  }
  const diagnostic = Object.create(null) as Record<string, unknown>;
  defineJsonProperty(diagnostic, key, value);
  state.diagnostics.push(Object.freeze(diagnostic));
}

function hasReportedError(value: unknown): boolean {
  if (value === undefined || value === null || value === false || value === "") {
    return false;
  }
  return !Array.isArray(value) || value.length > 0;
}

function primitiveBuilt(
  value: null | boolean | number,
  maximumJsonBytes: number,
  state: LimitState,
): BuiltValue | typeof OMIT {
  const json = value === null ? "null" : String(value);
  const byteLength = json.length;
  if (byteLength > maximumJsonBytes) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.retainedNodes += 1;
  return { byteLength, value };
}

function buildUnsupported(
  label: string,
  maximumJsonBytes: number,
  state: LimitState,
  reason: ResultTruncationReason = "unsupported_value",
): BuiltValue | typeof OMIT {
  addReason(state, reason);
  return fitString(label, maximumJsonBytes, state);
}

function prepareEidData(value: unknown, state: LimitState): PreparedEidData {
  const data = Object.create(null) as Record<string, unknown>;
  const securityCounts: { originalCount: number; retainedCount: number }[] = [];
  let validContainer = false;
  if (typeof value === "object" && value !== null) {
    try {
      validContainer = isPlainObject(value);
    } catch {
      validContainer = false;
    }
  }
  if (!validContainer) {
    addReason(state, "invalid_entitlement_data");
    return {
      data,
      invalidSecurityCount: 1,
      scannedSecurityCount: 1,
      securityCounts,
      totalEidCount: 0,
      totalSecurityCount: 1,
      truncation: {
        invalidSecurityCount: 1,
        omittedSecurityCount: 0,
        retainedEidCount: 0,
        retainedSecurityCount: 0,
        scannedSecurityCount: 1,
        securityCounts,
        totalEidCount: 0,
        totalSecurityCount: 1,
      },
    };
  }
  const eidRecord = value as Record<string, unknown>;

  let complete = true;
  let invalidSecurityCount = 0;
  let retainedEidCount = 0;
  let retainedSecurityCount = 0;
  let retainedSecurityNameBytes = 0;
  let scannedSecurityCount = 0;
  let totalEidCount = 0;
  const remainingNodeBudget = state.limits.maxResultNodes - state.visitedNodes;
  const reservedSummaryNodes = Math.min(16, Math.max(0, remainingNodeBudget - 1));
  const eidVisitLimit =
    state.visitedNodes + Math.max(1, Math.floor((remainingNodeBudget - reservedSummaryNodes) / 2));

  securityLoop: for (const security in eidRecord) {
    if (!Object.hasOwn(eidRecord, security)) {
      continue;
    }
    if (!consumeVisitBefore(state, eidVisitLimit)) {
      complete = false;
      break;
    }
    scannedSecurityCount += 1;
    const descriptor = ownEnumerableDescriptor(eidRecord, security);
    if (descriptor === undefined) {
      continue;
    }
    if (!("value" in descriptor) || !Array.isArray(descriptor.value)) {
      invalidSecurityCount += 1;
      addReason(state, "invalid_entitlement_data");
      continue;
    }
    const eids = descriptor.value as readonly unknown[];
    const remainingNameBytes = MAX_EID_SECURITY_NAME_BYTES - retainedSecurityNameBytes;
    const securityNameBytes = utf8ByteLengthAtMost(security, remainingNameBytes);
    const canRetainSecurity =
      retainedSecurityCount < MAX_EID_SECURITIES && securityNameBytes !== null;
    const remainingEidCapacity = Math.max(0, MAX_ENTITLEMENT_EIDS - retainedEidCount);
    const retained: number[] = [];
    let incomplete = false;
    for (let index = 0; index < eids.length; index += 1) {
      if (!consumeVisitBefore(state, eidVisitLimit)) {
        complete = false;
        incomplete = true;
        break;
      }
      const eidDescriptor = Object.getOwnPropertyDescriptor(eids, String(index));
      const eid: unknown =
        eidDescriptor !== undefined && "value" in eidDescriptor ? eidDescriptor.value : undefined;
      if (
        eidDescriptor === undefined ||
        !("value" in eidDescriptor) ||
        typeof eid !== "number" ||
        !Number.isInteger(eid) ||
        eid <= 0 ||
        eid > MAX_BLOOMBERG_EID
      ) {
        invalidSecurityCount += 1;
        addReason(state, "invalid_entitlement_data");
        continue securityLoop;
      }
      if (canRetainSecurity && retained.length < remainingEidCapacity) {
        retained.push(eid);
      }
    }
    totalEidCount = Math.min(Number.MAX_SAFE_INTEGER, totalEidCount + eids.length);

    if (!canRetainSecurity) {
      addReason(state, "entitlement_limit");
      if (incomplete) {
        break;
      }
      continue;
    }

    retainedEidCount += retained.length;
    retainedSecurityCount += 1;
    retainedSecurityNameBytes += securityNameBytes;
    defineJsonProperty(data, security, retained);
    securityCounts.push({ originalCount: eids.length, retainedCount: retained.length });
    if (!incomplete && retained.length !== eids.length) {
      addReason(state, "entitlement_limit");
    }
    if (incomplete) {
      break;
    }
  }

  const omittedSecurityCount = complete
    ? scannedSecurityCount - retainedSecurityCount - invalidSecurityCount
    : null;
  const wasTruncated =
    !complete ||
    invalidSecurityCount > 0 ||
    omittedSecurityCount !== 0 ||
    retainedEidCount !== totalEidCount;
  if (!wasTruncated) {
    return {
      data,
      invalidSecurityCount,
      scannedSecurityCount,
      securityCounts,
      totalEidCount,
      totalSecurityCount: scannedSecurityCount,
    };
  }
  return {
    data,
    invalidSecurityCount,
    scannedSecurityCount,
    securityCounts,
    totalEidCount: complete ? totalEidCount : null,
    totalSecurityCount: complete ? scannedSecurityCount : null,
    truncation: {
      invalidSecurityCount,
      omittedSecurityCount,
      retainedEidCount,
      retainedSecurityCount,
      scannedSecurityCount,
      securityCounts,
      totalEidCount: complete ? totalEidCount : null,
      totalSecurityCount: complete ? scannedSecurityCount : null,
    },
  };
}

function eidSummaryRecord(source: object): Record<string, unknown> | undefined {
  const descriptor = ownEnumerableDescriptor(source, "eidDataTruncation");
  if (
    descriptor === undefined ||
    !("value" in descriptor) ||
    typeof descriptor.value !== "object" ||
    descriptor.value === null
  ) {
    return undefined;
  }
  return descriptor.value as Record<string, unknown>;
}

function eidSummaryCount(
  summary: Record<string, unknown> | undefined,
  key: string,
): number | null | undefined {
  if (summary === undefined) {
    return undefined;
  }
  const descriptor = ownEnumerableDescriptor(summary, key);
  if (descriptor === undefined || !("value" in descriptor)) {
    return undefined;
  }
  const value: unknown = descriptor.value;
  return value === null || (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    ? value
    : undefined;
}

function eidSummarySecurityCounts(
  summary: Record<string, unknown> | undefined,
): readonly { originalCount: number | null; retainedCount: number }[] {
  if (summary === undefined) {
    return [];
  }
  const descriptor = ownEnumerableDescriptor(summary, "securityCounts");
  if (descriptor === undefined || !("value" in descriptor) || !Array.isArray(descriptor.value)) {
    return [];
  }
  const counts: { originalCount: number | null; retainedCount: number }[] = [];
  for (let index = 0; index < descriptor.value.length; index += 1) {
    const entryDescriptor = Object.getOwnPropertyDescriptor(descriptor.value, String(index));
    if (
      entryDescriptor === undefined ||
      !("value" in entryDescriptor) ||
      typeof entryDescriptor.value !== "object" ||
      entryDescriptor.value === null
    ) {
      break;
    }
    const originalCount = eidSummaryCount(
      entryDescriptor.value as Record<string, unknown>,
      "originalCount",
    );
    const retainedCount = eidSummaryCount(
      entryDescriptor.value as Record<string, unknown>,
      "retainedCount",
    );
    if (
      originalCount === undefined ||
      (originalCount !== null && typeof originalCount !== "number") ||
      typeof retainedCount !== "number"
    ) {
      break;
    }
    counts.push({ originalCount, retainedCount });
  }
  return counts;
}

function emittedEidTruncation(
  source: object,
  emittedValue: unknown,
  prepared: PreparedEidData,
): EidDataTruncation | undefined {
  const emitted =
    typeof emittedValue === "object" && emittedValue !== null
      ? (emittedValue as Record<string, unknown>)
      : (Object.create(null) as Record<string, unknown>);
  const prior = eidSummaryRecord(source);
  const priorSecurityCounts = eidSummarySecurityCounts(prior);
  const securityCounts: { originalCount: number | null; retainedCount: number }[] = [];
  let retainedEidCount = 0;
  let retainedSecurityCount = 0;
  for (const security in emitted) {
    if (!Object.hasOwn(emitted, security)) {
      continue;
    }
    const descriptor = ownEnumerableDescriptor(emitted, security);
    if (descriptor === undefined || !("value" in descriptor) || !Array.isArray(descriptor.value)) {
      continue;
    }
    const retainedCount = descriptor.value.length;
    const preparedCount = prepared.securityCounts[retainedSecurityCount];
    const priorCount = priorSecurityCounts[retainedSecurityCount];
    securityCounts.push({
      originalCount:
        prior === undefined
          ? (preparedCount?.originalCount ?? retainedCount)
          : (priorCount?.originalCount ?? null),
      retainedCount,
    });
    retainedEidCount = Math.min(Number.MAX_SAFE_INTEGER, retainedEidCount + retainedCount);
    retainedSecurityCount += 1;
  }

  const priorTotalEidCount = eidSummaryCount(prior, "totalEidCount");
  const priorTotalSecurityCount = eidSummaryCount(prior, "totalSecurityCount");
  const totalEidCount =
    prior === undefined
      ? prepared.totalEidCount
      : priorTotalEidCount === undefined
        ? null
        : priorTotalEidCount;
  const totalSecurityCount =
    prior === undefined
      ? prepared.totalSecurityCount
      : priorTotalSecurityCount === undefined
        ? null
        : priorTotalSecurityCount;
  const priorInvalidSecurityCount = eidSummaryCount(prior, "invalidSecurityCount");
  const invalidSecurityCount =
    typeof priorInvalidSecurityCount === "number"
      ? priorInvalidSecurityCount
      : prepared.invalidSecurityCount;
  const priorScannedSecurityCount = eidSummaryCount(prior, "scannedSecurityCount");
  const scannedSecurityCount =
    typeof priorScannedSecurityCount === "number"
      ? priorScannedSecurityCount
      : prepared.scannedSecurityCount;
  const omittedSecurityCount =
    totalSecurityCount === null
      ? null
      : Math.max(0, totalSecurityCount - retainedSecurityCount - invalidSecurityCount);
  const truncated =
    prior !== undefined ||
    prepared.truncation !== undefined ||
    totalEidCount === null ||
    totalSecurityCount === null ||
    retainedEidCount !== totalEidCount ||
    retainedSecurityCount + invalidSecurityCount !== totalSecurityCount;
  if (!truncated) {
    return undefined;
  }
  return {
    retainedEidCount,
    totalEidCount,
    retainedSecurityCount,
    totalSecurityCount,
    securityCounts,
    invalidSecurityCount,
    omittedSecurityCount,
    scannedSecurityCount,
  };
}

function appendBuiltProperty(
  accumulator: ObjectAccumulator,
  key: string,
  built: BuiltValue,
  maximumJsonBytes: number,
  state: LimitState,
): boolean {
  if (Object.hasOwn(accumulator.value, key)) {
    return true;
  }
  const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
  const availableForKey =
    maximumJsonBytes - accumulator.byteLength - commaBytes - 1 - built.byteLength;
  if (availableForKey < 2) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const keyBytes = jsonStringByteLength(key, availableForKey);
  if (keyBytes > availableForKey) {
    addReason(state, "max_result_bytes");
    return false;
  }
  defineJsonProperty(accumulator.value, key, built.value);
  accumulator.byteLength += commaBytes + keyBytes + 1 + built.byteLength;
  accumulator.propertyCount += 1;
  return true;
}

function buildProperty(
  source: object,
  key: string,
  accumulator: ObjectAccumulator,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
): boolean {
  const descriptor = ownEnumerableDescriptor(source, key);
  if (descriptor === undefined) {
    return true;
  }
  if (key === "eidDataTruncation" && ownEnumerableDescriptor(source, "eidData") !== undefined) {
    return true;
  }
  const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
  const availableForKeyAndChild = maximumJsonBytes - accumulator.byteLength - commaBytes - 1;
  if (availableForKeyAndChild < 4) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const maximumKeyBytes = availableForKeyAndChild - 2;
  const keyBytes = jsonStringByteLength(key, maximumKeyBytes);
  if (keyBytes > maximumKeyBytes) {
    addReason(state, "max_result_bytes");
    return false;
  }
  const childBudget = availableForKeyAndChild - keyBytes;

  let rawValue: unknown;
  if ("value" in descriptor) {
    rawValue = descriptor.value;
  } else {
    addReason(state, "accessor_omitted");
    rawValue = "[Accessor omitted]";
  }
  const errorKey = ERROR_SHAPE_KEYS[key.toLowerCase()] === true && hasReportedError(rawValue);
  if (errorKey) {
    state.hasErrors = true;
  }
  const useSharedRows = key === "rows" || (key === "data" && Array.isArray(rawValue));

  if (key === "eidData") {
    const prepared = prepareEidData(rawValue, state);
    const built = buildValue(
      prepared.data,
      Math.max(2, Math.floor(childBudget / 3)),
      state,
      depth + 1,
      MAX_ENTITLEMENT_EIDS,
      false,
    );
    if (built === OMIT || !appendBuiltProperty(accumulator, key, built, maximumJsonBytes, state)) {
      return false;
    }
    const emittedTruncation = emittedEidTruncation(source, built.value, prepared);
    if (emittedTruncation !== undefined) {
      const summaryCommaBytes = accumulator.propertyCount === 0 ? 0 : 1;
      const summaryKeyBytes = jsonStringByteLength("eidDataTruncation");
      const summaryBudget =
        maximumJsonBytes - accumulator.byteLength - summaryCommaBytes - summaryKeyBytes - 1;
      const summary = buildValue(
        emittedTruncation,
        summaryBudget,
        state,
        depth + 1,
        MAX_EID_SECURITIES,
        false,
      );
      if (
        summary === OMIT ||
        !appendBuiltProperty(accumulator, "eidDataTruncation", summary, maximumJsonBytes, state)
      ) {
        state.omittedPropertiesAtLeast += 1;
      }
    }
    return true;
  }

  const built = buildValue(rawValue, childBudget, state, depth + 1, rowLimit, useSharedRows);
  if (built === OMIT || !appendBuiltProperty(accumulator, key, built, maximumJsonBytes, state)) {
    return false;
  }
  if (errorKey) {
    rememberErrorDiagnostic(state, key, built.value);
  }
  if ((key === "truncated" || key === "truncatedInput") && rawValue === true) {
    addReason(state, "upstream_truncation");
  }
  return true;
}

function hasArrayMetadata(value: readonly unknown[]): boolean {
  for (const key of PRIORITY_KEYS) {
    if (ownEnumerableDescriptor(value, key) !== undefined) {
      return true;
    }
  }
  return false;
}

function buildArrayRows(
  value: readonly unknown[],
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.retainedNodes += 1;
  const output: unknown[] = [];
  let byteLength = 2;
  let processedRows = 0;
  const retainedLength = Math.min(
    value.length,
    rowLimit,
    useSharedRows ? state.remainingRows : Number.MAX_SAFE_INTEGER,
  );
  for (let index = 0; index < retainedLength; index += 1) {
    const commaBytes = index === 0 ? 0 : 1;
    const childBudget = maximumJsonBytes - byteLength - commaBytes;
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    let entry: unknown = null;
    if (descriptor !== undefined && "value" in descriptor) {
      entry = descriptor.value;
    } else if (descriptor !== undefined) {
      addReason(state, "accessor_omitted");
      entry = "[Accessor omitted]";
    }
    const built = buildValue(entry, childBudget, state, depth + 1, rowLimit, false);
    if (built === OMIT) {
      break;
    }
    output.push(built.value);
    byteLength += commaBytes + built.byteLength;
    processedRows = index + 1;
  }
  if (useSharedRows) {
    state.maximumArrayRows = Math.max(state.maximumArrayRows, processedRows);
  }
  if (useSharedRows) {
    state.remainingRows -= processedRows;
    state.retainedRows += processedRows;
  }
  if (retainedLength < value.length) {
    addReason(state, "max_rows");
  }
  if (processedRows < value.length) {
    state.omittedRows += value.length - processedRows;
  }
  return { byteLength, value: output };
}

function buildArray(
  value: readonly unknown[],
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  state.ancestors.add(value);
  try {
    if (!hasArrayMetadata(value)) {
      return buildArrayRows(value, maximumJsonBytes, state, depth, rowLimit, useSharedRows);
    }
    if (maximumJsonBytes < 2) {
      addReason(state, "max_result_bytes");
      return OMIT;
    }
    state.retainedNodes += 1;
    const accumulator: ObjectAccumulator = {
      byteLength: 2,
      propertyCount: 0,
      value: Object.create(null) as Record<string, unknown>,
    };
    for (const key of PRIORITY_KEYS) {
      const isErrorMetadata = key === "diagnostics" || ERROR_SHAPE_KEYS[key.toLowerCase()] === true;
      if (state.rowsBeforeMetadata && !isErrorMetadata) {
        continue;
      }
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
      }
    }

    const commaBytes = accumulator.propertyCount === 0 ? 0 : 1;
    const rowsKeyBytes = jsonStringByteLength("rows");
    const rowsBudget = maximumJsonBytes - accumulator.byteLength - commaBytes - rowsKeyBytes - 1;
    const rows = consumeVisit(state)
      ? buildArrayRows(value, rowsBudget, state, depth + 1, rowLimit, useSharedRows)
      : OMIT;
    if (rows === OMIT) {
      state.omittedRows += value.length;
      state.omittedPropertiesAtLeast += 1;
    } else if (!appendBuiltProperty(accumulator, "rows", rows, maximumJsonBytes, state)) {
      state.omittedRows += Math.min(value.length, rowLimit);
      state.omittedPropertiesAtLeast += 1;
    }

    if (state.rowsBeforeMetadata) {
      for (const key of PRIORITY_KEYS) {
        if (key === "diagnostics" || ERROR_SHAPE_KEYS[key.toLowerCase()] === true) {
          continue;
        }
        if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
          state.omittedPropertiesAtLeast += 1;
        }
      }
    }

    return { byteLength: accumulator.byteLength, value: accumulator.value };
  } finally {
    state.ancestors.delete(value);
  }
}

function buildObject(
  value: Record<string, unknown>,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
): BuiltValue | typeof OMIT {
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  if (maximumJsonBytes < 2) {
    addReason(state, "max_result_bytes");
    return OMIT;
  }
  state.ancestors.add(value);
  state.retainedNodes += 1;
  try {
    const accumulator: ObjectAccumulator = {
      byteLength: 2,
      propertyCount: 0,
      value: Object.create(null) as Record<string, unknown>,
    };
    const processed = new Set<string>();
    for (const key of PRIORITY_KEYS) {
      processed.add(key);
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
      }
    }

    for (const key in value) {
      if (!Object.hasOwn(value, key) || processed.has(key)) {
        continue;
      }
      if (!buildProperty(value, key, accumulator, maximumJsonBytes, state, depth, rowLimit)) {
        state.omittedPropertiesAtLeast += 1;
        break;
      }
    }
    return { byteLength: accumulator.byteLength, value: accumulator.value };
  } finally {
    state.ancestors.delete(value);
  }
}

function errorRecord(error: Error): Record<string, unknown> {
  const record: Record<string, unknown> = {
    message: error.message,
    name: error.name,
  };
  if (error.cause !== undefined) {
    record.cause = error.cause;
  }
  return record;
}

const ACCESSOR_METHOD = Symbol("accessor_method");

function isCallable(value: unknown): value is (...args: readonly unknown[]) => unknown {
  return typeof value === "function";
}

function dataMethod(
  value: object,
  name: string,
): ((...args: readonly unknown[]) => unknown) | typeof ACCESSOR_METHOD | undefined {
  let current: object | null = value;
  try {
    for (let depth = 0; current !== null && depth <= MAX_RESULT_DEPTH; depth += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(current, name);
      if (descriptor !== undefined) {
        if (!("value" in descriptor)) {
          return ACCESSOR_METHOD;
        }
        const method: unknown = descriptor.value;
        return isCallable(method) ? method : undefined;
      }
      current = Object.getPrototypeOf(current) as object | null;
    }
  } catch {
    return ACCESSOR_METHOD;
  }
  return undefined;
}

function buildValue(
  value: unknown,
  maximumJsonBytes: number,
  state: LimitState,
  depth: number,
  rowLimit: number,
  useSharedRows: boolean,
): BuiltValue | typeof OMIT {
  if (!consumeVisit(state)) {
    return OMIT;
  }
  if (depth > MAX_RESULT_DEPTH) {
    return buildUnsupported(
      "[Max result depth exceeded]",
      maximumJsonBytes,
      state,
      "max_result_depth",
    );
  }
  if (typeof value === "string") {
    return fitString(value, maximumJsonBytes, state);
  }
  if (typeof value === "bigint") {
    return fitString(value.toString(), maximumJsonBytes, state);
  }
  if (value === null || typeof value === "boolean") {
    return primitiveBuilt(value, maximumJsonBytes, state);
  }
  if (typeof value === "number") {
    const jsonValue = Number.isFinite(value) ? value : null;
    if (jsonValue === null) {
      addReason(state, "unsupported_value");
    }
    return primitiveBuilt(jsonValue, maximumJsonBytes, state);
  }
  if (typeof value === "undefined" || typeof value === "function" || typeof value === "symbol") {
    addReason(state, "unsupported_value");
    return primitiveBuilt(null, maximumJsonBytes, state);
  }

  if (value instanceof Date) {
    const milliseconds = value.getTime();
    if (!Number.isFinite(milliseconds)) {
      addReason(state, "unsupported_value");
      return fitString("[Invalid Date]", maximumJsonBytes, state);
    }
    return fitString(value.toISOString(), maximumJsonBytes, state);
  }
  if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
    return buildUnsupported(
      `[binary data: ${String(value.byteLength)} bytes]`,
      maximumJsonBytes,
      state,
      "binary_data",
    );
  }
  if (Array.isArray(value)) {
    return buildArray(value, maximumJsonBytes, state, depth, rowLimit, useSharedRows);
  }
  if (value instanceof Error) {
    state.hasErrors = true;
    if (state.ancestors.has(value)) {
      return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
    }
    state.ancestors.add(value);
    try {
      const built = buildObject(errorRecord(value), maximumJsonBytes, state, depth, rowLimit);
      if (built !== OMIT) {
        rememberErrorDiagnostic(state, "error", built.value);
      }
      return built;
    } finally {
      state.ancestors.delete(value);
    }
  }

  let plainObject: Record<string, unknown> | undefined;
  try {
    plainObject = isPlainObject(value) ? value : undefined;
  } catch {
    return buildUnsupported("[Uninspectable object]", maximumJsonBytes, state);
  }
  if (plainObject !== undefined) {
    return buildObject(plainObject, maximumJsonBytes, state, depth, rowLimit);
  }

  const toJSON = dataMethod(value, "toJSON");
  if (toJSON === ACCESSOR_METHOD) {
    return buildUnsupported("[Accessor omitted]", maximumJsonBytes, state, "accessor_omitted");
  }
  if (toJSON === undefined) {
    return buildUnsupported("[Unsupported object]", maximumJsonBytes, state);
  }
  if (state.ancestors.has(value)) {
    return buildUnsupported("[Circular]", maximumJsonBytes, state, "circular_reference");
  }
  state.ancestors.add(value);
  try {
    let converted: unknown;
    try {
      converted = toJSON.call(value);
    } catch (error) {
      state.hasErrors = true;
      addReason(state, "unsupported_value");
      converted = {
        error: {
          message: error instanceof Error ? error.message : String(error),
          name: "toJSON",
        },
      };
    }
    return buildValue(converted, maximumJsonBytes, state, depth + 1, rowLimit, useSharedRows);
  } finally {
    state.ancestors.delete(value);
  }
}

function rowCountOf(value: unknown): number | null {
  if (Array.isArray(value)) {
    return value.length;
  }
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const record = value as Record<string, unknown>;
  for (const key of ["rowCount", "updateCount"] as const) {
    const descriptor = ownEnumerableDescriptor(record, key);
    if (
      descriptor !== undefined &&
      "value" in descriptor &&
      typeof descriptor.value === "number" &&
      Number.isSafeInteger(descriptor.value) &&
      descriptor.value >= 0
    ) {
      return descriptor.value;
    }
  }
  return null;
}

function truncationSummary(state: LimitState): ResultTruncationSummary | undefined {
  if (state.reasons.size === 0) {
    return undefined;
  }
  return {
    reasons: TRUNCATION_REASON_ORDER.filter((reason) => state.reasons.has(reason)),
    inspectedNodes: state.visitedNodes,
    retainedNodes: state.retainedNodes,
    ...(state.omittedPropertiesAtLeast === 0
      ? {}
      : { omittedPropertiesAtLeast: state.omittedPropertiesAtLeast }),
    ...(state.omittedRows === 0 ? {} : { omittedRows: state.omittedRows }),
  };
}

function validateResultLimits(limits: ResultLimitOptions): void {
  for (const name of ["maxResultBytes", "maxResultNodes", "maxRows", "maxStringChars"] as const) {
    const value = limits[name];
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new RangeError(`${name} must be a positive safe integer; got ${String(value)}`);
    }
  }
  if (limits.maxResultBytes < 4) {
    throw new RangeError(`maxResultBytes must be at least 4; got ${String(limits.maxResultBytes)}`);
  }
}

function limitResultWithRowPriority(
  value: unknown,
  limits: ResultLimitOptions,
  rowsBeforeMetadata: boolean,
): LimitResult {
  validateResultLimits(limits);
  const state: LimitState = {
    ancestors: new WeakSet<object>(),
    diagnostics: [],
    hasErrors: false,
    maximumArrayRows: 0,
    limits,
    omittedPropertiesAtLeast: 0,
    omittedRows: 0,
    remainingRows: limits.maxRows,
    reasons: new Set<ResultTruncationReason>(),
    retainedNodes: 0,
    retainedRows: 0,
    rowsBeforeMetadata,
    visitedNodes: 0,
  };
  const rowCount = rowCountOf(value);
  const built = buildValue(value, limits.maxResultBytes, state, 0, limits.maxRows, true);
  const result = built === OMIT ? { byteLength: 4, value: null } : built;
  if (built === OMIT) {
    addReason(state, "max_result_bytes");
  }
  const truncation = truncationSummary(state);
  return {
    byteLength: result.byteLength,
    errorDiagnostics: state.diagnostics,
    inspectedNodes: state.visitedNodes,
    maximumArrayRows: state.maximumArrayRows,
    retainedRows: state.retainedRows,
    hasErrors: state.hasErrors,
    rowCount,
    truncated: truncation !== undefined,
    ...(truncation === undefined ? {} : { truncation }),
    value: result.value,
  };
}

export function limitResult(value: unknown, limits: ResultLimitOptions): LimitResult {
  return limitResultWithRowPriority(value, limits, false);
}

function artifactEnvelope(
  tool: BloombergToolName,
  limited: LimitResult,
  truncation: ResultTruncationSummary | undefined,
): ToolEnvelope {
  return {
    tool,
    rowCount: limited.rowCount,
    truncated: limited.truncated,
    ...(truncation === undefined ? {} : { truncation }),
    ...(limited.hasErrors ? { hasErrors: true as const } : {}),
    data: limited.value,
  };
}

function boundedJsonByteLength(value: unknown): number {
  return UTF8_ENCODER.encode(JSON.stringify(value)).byteLength;
}

function mergeLimited(first: LimitResult, second: LimitResult): LimitResult {
  const reasons = new Set<ResultTruncationReason>(first.truncation?.reasons ?? []);
  for (const reason of second.truncation?.reasons ?? []) {
    reasons.add(reason);
  }
  const retainedNodes = second.truncation?.retainedNodes ?? first.truncation?.retainedNodes;
  const omittedPropertiesAtLeast =
    (first.truncation?.omittedPropertiesAtLeast ?? 0) +
    (second.truncation?.omittedPropertiesAtLeast ?? 0);
  const omittedRows = (first.truncation?.omittedRows ?? 0) + (second.truncation?.omittedRows ?? 0);
  const truncation: ResultTruncationSummary | undefined =
    reasons.size === 0
      ? undefined
      : {
          reasons: TRUNCATION_REASON_ORDER.filter((reason) => reasons.has(reason)),
          inspectedNodes: first.inspectedNodes + second.inspectedNodes,
          ...(retainedNodes === undefined ? {} : { retainedNodes }),
          ...(omittedPropertiesAtLeast === 0 ? {} : { omittedPropertiesAtLeast }),
          ...(omittedRows === 0 ? {} : { omittedRows }),
        };
  return {
    byteLength: second.byteLength,
    maximumArrayRows: second.maximumArrayRows,
    retainedRows: second.retainedRows,
    errorDiagnostics:
      second.errorDiagnostics.length === 0 ? first.errorDiagnostics : second.errorDiagnostics,
    hasErrors: first.hasErrors || second.hasErrors,
    inspectedNodes: first.inspectedNodes + second.inspectedNodes,
    rowCount: first.rowCount,
    truncated: truncation !== undefined,
    ...(truncation === undefined ? {} : { truncation }),
    value: second.value,
  };
}

function fitArtifact(
  tool: BloombergToolName,
  limited: LimitResult,
  maxResultBytes: number,
): ToolEnvelope {
  let envelope = artifactEnvelope(tool, limited, limited.truncation);
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  const fitLimited = limited.truncated ? limited : { ...limited, truncated: true };
  const reasons = limited.truncation?.reasons ?? ["max_result_bytes"];
  envelope = artifactEnvelope(tool, fitLimited, { reasons });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  const primaryReason =
    reasons.find((reason) => reason !== "max_result_bytes") ?? "max_result_bytes";
  const compactReasons: ResultTruncationReason[] =
    primaryReason === "max_result_bytes" ? [primaryReason] : [primaryReason, "max_result_bytes"];
  envelope = artifactEnvelope(tool, fitLimited, { reasons: compactReasons });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  envelope = artifactEnvelope(tool, fitLimited, {
    reasons: ["max_result_bytes"],
  });
  if (boundedJsonByteLength(envelope) <= maxResultBytes) {
    return envelope;
  }

  return {
    tool,
    rowCount: limited.rowCount,
    truncated: true,
    truncation: { reasons: ["max_result_bytes"] },
    ...(limited.hasErrors ? { hasErrors: true as const } : {}),
    data: null,
  };
}

function rowText(rowCount: number | null): string {
  return rowCount === null
    ? "row count unknown"
    : `${String(rowCount)} row${rowCount === 1 ? "" : "s"}`;
}

function summarizeEnvelope(
  envelope: ToolEnvelope,
  contentTruncation: ResultTruncationSummary | undefined,
): string {
  const notes: string[] = [];
  if (
    envelope.rowCount === 0 ||
    (!envelope.truncated && (envelope.data === null || envelope.data === undefined))
  ) {
    notes.push(
      "empty result; verify identifiers, fields, and date range before concluding no data exists",
    );
  }
  if (envelope.hasErrors === true) {
    notes.push("Bloomberg error diagnostics included in preview");
  }
  const artifactReasons = envelope.truncation?.reasons.join(",") ?? "none";
  const contentReasons = contentTruncation?.reasons.join(",") ?? "none";
  const noteText = notes.length === 0 ? "" : `; ${notes.join("; ")}`;
  return `${envelope.tool}: ${rowText(envelope.rowCount)}; artifactTruncated=${String(envelope.truncated)}; contentTruncated=${String(contentTruncation !== undefined)}; artifactReasons=${artifactReasons}; contentReasons=${contentReasons}${noteText}`;
}

function contentPayload(envelope: ToolEnvelope, limited: LimitResult): Record<string, unknown> {
  const projected = limited.value as Record<string, unknown> | null;
  return {
    tool: envelope.tool,
    rowCount: envelope.rowCount,
    truncated: envelope.truncated,
    contentTruncated: limited.truncated,
    ...(projected ?? { data: null }),
  };
}

function formatToolContent(
  envelope: ToolEnvelope,
  preview: LimitResult,
  maxContentBytes: number,
): string {
  const firstPreviewValue = preview.value;
  const summary = summarizeEnvelope(envelope, preview.truncation);
  const payload = contentPayload(envelope, preview);
  const content = `${summary}\n${JSON.stringify(payload)}`;
  if (UTF8_ENCODER.encode(content).byteLength <= maxContentBytes) {
    return content;
  }

  const compactSummary = `${envelope.tool}: ${rowText(envelope.rowCount)}; artifactTruncated=${String(envelope.truncated)}; contentTruncated=${String(preview.truncated)}; hasErrors=${String(envelope.hasErrors === true)}`;
  const compactContent = `${compactSummary}\n${JSON.stringify(firstPreviewValue)}`;
  if (UTF8_ENCODER.encode(compactContent).byteLength <= maxContentBytes) {
    return compactContent;
  }
  return compactSummary;
}

function exhaustedProjection(rowCount: number | null): LimitResult {
  return {
    byteLength: 4,
    maximumArrayRows: 0,
    retainedRows: 0,
    errorDiagnostics: [],
    hasErrors: false,
    inspectedNodes: 0,
    rowCount,
    truncated: true,
    truncation: {
      inspectedNodes: 0,
      reasons: ["max_result_nodes"],
      retainedNodes: 0,
    },
    value: null,
  };
}

function projectResult(
  value: unknown,
  limits: ResultLimitOptions,
  rowsBeforeMetadata: boolean,
): LimitResult {
  return limits.maxResultNodes === 0
    ? exhaustedProjection(rowCountOf(value))
    : limitResultWithRowPriority(value, limits, rowsBeforeMetadata);
}

function withAggregateInspection(limited: LimitResult, inspectedNodes: number): LimitResult {
  return {
    ...limited,
    inspectedNodes,
    ...(limited.truncation === undefined
      ? {}
      : {
          truncation: {
            ...limited.truncation,
            inspectedNodes,
          },
        }),
  };
}

function reusedProjection(value: unknown, canonical: LimitResult): LimitResult {
  return {
    byteLength: boundedJsonByteLength(value),
    errorDiagnostics: [],
    hasErrors: false,
    inspectedNodes: 0,
    maximumArrayRows: canonical.maximumArrayRows,
    retainedRows: canonical.retainedRows,
    rowCount: rowCountOf(value),
    truncated: false,
    value,
  };
}

function canReuseProjection(
  canonical: LimitResult,
  byteLength: number,
  maxBytes: number,
  maxRows: number,
): boolean {
  return byteLength <= maxBytes && canonical.maximumArrayRows <= maxRows;
}

export function createToolResult(
  tool: BloombergToolName,
  value: unknown,
  limits: ToolResultBuildOptions,
): ToolContentAndArtifact {
  const initialInspectedNodes = limits.materializedNodes ?? 0;
  if (limits.maxResultBytes < MIN_TOOL_RESULT_BYTES) {
    throw new RangeError(
      `maxResultBytes must be at least ${String(MIN_TOOL_RESULT_BYTES)}; got ${String(limits.maxResultBytes)}`,
    );
  }
  if (limits.maxContentBytes < MIN_TOOL_RESULT_BYTES) {
    throw new RangeError(
      `maxContentBytes must be at least ${String(MIN_TOOL_RESULT_BYTES)}; got ${String(limits.maxContentBytes)}`,
    );
  }
  if (limits.maxResultNodes < MIN_TOOL_RESULT_NODES) {
    throw new RangeError(
      `maxResultNodes must be at least ${String(MIN_TOOL_RESULT_NODES)}; got ${String(limits.maxResultNodes)}`,
    );
  }

  validateResultLimits(limits);
  if (
    !Number.isSafeInteger(initialInspectedNodes) ||
    initialInspectedNodes < 0 ||
    initialInspectedNodes > limits.maxResultNodes
  ) {
    throw new RangeError(
      `materializedNodes must be between 0 and maxResultNodes; got ${String(initialInspectedNodes)}`,
    );
  }
  const availableNodeBudget = limits.maxResultNodes - initialInspectedNodes;
  const hasEidMetadata =
    typeof value === "object" &&
    value !== null &&
    ownEnumerableDescriptor(value, "eidData") !== undefined;
  const canonicalNodeBudget =
    availableNodeBudget === 0
      ? 0
      : Math.max(
          1,
          hasEidMetadata
            ? availableNodeBudget - Math.ceil(availableNodeBudget / 5)
            : Math.floor(availableNodeBudget / 3),
        );
  const canonical = projectResult(
    value,
    {
      maxResultBytes: Math.max(limits.maxResultBytes, limits.maxContentBytes),
      maxResultNodes: canonicalNodeBudget,
      maxRows: Math.max(limits.maxRows, limits.maxContentRows),
      maxStringChars: limits.maxStringChars,
    },
    false,
  );

  const artifactDataBudget = projectionBudget(limits.maxResultBytes, RESULT_ENVELOPE_RESERVE_BYTES);
  const contentDataBudget = projectionBudget(
    limits.maxContentBytes,
    CONTENT_ENVELOPE_RESERVE_BYTES,
  );
  const contentSource = Object.create(null) as Record<string, unknown>;
  if (canonical.errorDiagnostics.length > 0) {
    contentSource.diagnostics = canonical.errorDiagnostics;
  }
  contentSource.data = canonical.value;
  const contentSourceBytes = boundedJsonByteLength(contentSource);
  const reuseArtifact = canReuseProjection(
    canonical,
    canonical.byteLength,
    artifactDataBudget,
    limits.maxRows,
  );
  const reuseContent = canReuseProjection(
    canonical,
    contentSourceBytes,
    contentDataBudget,
    limits.maxContentRows,
  );
  const remainingNodeBudget = Math.max(0, availableNodeBudget - canonical.inspectedNodes);
  const artifactNeedsNodes = !reuseArtifact;
  const contentNeedsNodes = !reuseContent;
  const artifactNodeBudget = artifactNeedsNodes
    ? contentNeedsNodes
      ? Math.floor(remainingNodeBudget / 2)
      : remainingNodeBudget
    : 0;
  const contentNodeBudget = contentNeedsNodes ? remainingNodeBudget - artifactNodeBudget : 0;
  const artifactProjection = reuseArtifact
    ? reusedProjection(canonical.value, canonical)
    : projectResult(
        canonical.value,
        {
          maxResultBytes: artifactDataBudget,
          maxResultNodes: artifactNodeBudget,
          maxRows: limits.maxRows,
          maxStringChars: Number.MAX_SAFE_INTEGER,
        },
        false,
      );
  const contentProjection = reuseContent
    ? reusedProjection(contentSource, canonical)
    : projectResult(
        contentSource,
        {
          maxResultBytes: contentDataBudget,
          maxResultNodes: contentNodeBudget,
          maxRows: limits.maxContentRows,
          maxStringChars: Number.MAX_SAFE_INTEGER,
        },
        true,
      );

  const inspectedNodes =
    initialInspectedNodes +
    canonical.inspectedNodes +
    artifactProjection.inspectedNodes +
    contentProjection.inspectedNodes;
  const artifactResult = withAggregateInspection(
    mergeLimited(canonical, artifactProjection),
    inspectedNodes,
  );
  const contentResult = withAggregateInspection(
    mergeLimited(canonical, contentProjection),
    inspectedNodes,
  );
  const envelope = fitArtifact(tool, artifactResult, limits.maxResultBytes);
  return [formatToolContent(envelope, contentResult, limits.maxContentBytes), envelope];
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
