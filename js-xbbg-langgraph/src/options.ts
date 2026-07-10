import type * as xbbg from "@xbbg/core";

import type { XbbgCoreLike, XbbgEngineLike } from "./core-loader";

export const BLOOMBERG_TOOL_NAMES = [
  "xbbg_bdp",
  "xbbg_bdh",
  "xbbg_bds",
  "xbbg_bdib",
  "xbbg_bdtick",
  "xbbg_check_entitlements",
  "xbbg_bql",
  "xbbg_bsrch",
  "xbbg_bqr",
  "xbbg_bflds",
  "xbbg_beqs",
  "xbbg_yas",
  "xbbg_preferreds",
  "xbbg_corporate_bonds",
  "xbbg_index_members",
  "xbbg_resolve_isins",
  "xbbg_issuer_isins",
  "xbbg_etf_holdings",
  "xbbg_stream_snapshot",
  "xbbg_mktbar_snapshot",
  "xbbg_depth_snapshot",
  "xbbg_ext_ticker",
  "xbbg_ext_futures",
  "xbbg_ext_cdx",
  "xbbg_ext_currency",
  "xbbg_ext_bql_builder",
  "xbbg_ext_chart_spec",
  "xbbg_ext_market_session",
  "xbbg_ext_yas_overrides",
  "xbbg_ext_constants",
  "xbbg_ext_columns",
  "xbbg_ext_calculate",
] as const;

export type BloombergToolName = (typeof BLOOMBERG_TOOL_NAMES)[number];

export interface BloombergToolsOptions {
  readonly engine?: XbbgEngineLike;
  readonly engineConfig?: xbbg.EngineConfig;
  readonly core?: XbbgCoreLike;
  readonly maxSecurities?: number;
  readonly maxFields?: number;
  readonly maxRows?: number;
  readonly maxStringChars?: number;
  readonly maxBqlQueryChars?: number;
  readonly maxSearchSpecChars?: number;
  readonly maxStreamUpdates?: number;
  readonly maxStreamWaitMs?: number;
  readonly validateFields?: boolean;
  readonly disabledTools?: readonly BloombergToolName[];
}

export interface NormalizedBloombergToolsOptions {
  readonly engine?: XbbgEngineLike;
  readonly engineConfig: xbbg.EngineConfig;
  readonly core?: XbbgCoreLike;
  readonly maxSecurities: number;
  readonly maxFields: number;
  readonly maxRows: number;
  readonly maxStringChars: number;
  readonly maxBqlQueryChars: number;
  readonly maxSearchSpecChars: number;
  readonly maxStreamUpdates: number;
  readonly maxStreamWaitMs: number;
  readonly validateFields: boolean | undefined;
  readonly disabledTools: ReadonlySet<BloombergToolName>;
}

const DEFAULT_MAX_SECURITIES = 25;
const DEFAULT_MAX_FIELDS = 25;
const DEFAULT_MAX_ROWS = 500;
const DEFAULT_MAX_STRING_CHARS = 2000;
const DEFAULT_MAX_BQL_QUERY_CHARS = 4000;
const DEFAULT_MAX_SEARCH_SPEC_CHARS = 1000;
const DEFAULT_MAX_STREAM_UPDATES = 10;
const DEFAULT_MAX_STREAM_WAIT_MS = 15_000;

/**
 * Default hard per-request timeout applied to lazily connected engines.
 * @xbbg/core disables request timeouts by default (`requestTimeoutMs: 0`),
 * which would let a wedged Terminal session hang tool calls forever. An
 * explicit `engineConfig.requestTimeoutMs` (including 0) always wins.
 */
export const DEFAULT_ENGINE_REQUEST_TIMEOUT_MS = 60_000;

function engineConfigWithDefaults(config: xbbg.EngineConfig | undefined): xbbg.EngineConfig {
  if (config?.requestTimeoutMs !== undefined) {
    return config;
  }
  return { ...config, requestTimeoutMs: DEFAULT_ENGINE_REQUEST_TIMEOUT_MS };
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer; got ${String(value)}`);
  }
  return value;
}

function disabledToolSet(
  tools: readonly BloombergToolName[] | undefined,
): ReadonlySet<BloombergToolName> {
  return new Set(tools ?? []);
}

export function normalizeBloombergToolsOptions(
  options: BloombergToolsOptions = {},
): NormalizedBloombergToolsOptions {
  return {
    core: options.core,
    disabledTools: disabledToolSet(options.disabledTools),
    engine: options.engine,
    engineConfig: engineConfigWithDefaults(options.engineConfig),
    maxBqlQueryChars: positiveInteger(
      options.maxBqlQueryChars,
      DEFAULT_MAX_BQL_QUERY_CHARS,
      "maxBqlQueryChars",
    ),
    maxFields: positiveInteger(options.maxFields, DEFAULT_MAX_FIELDS, "maxFields"),
    maxRows: positiveInteger(options.maxRows, DEFAULT_MAX_ROWS, "maxRows"),
    maxSearchSpecChars: positiveInteger(
      options.maxSearchSpecChars,
      DEFAULT_MAX_SEARCH_SPEC_CHARS,
      "maxSearchSpecChars",
    ),
    maxStreamUpdates: positiveInteger(
      options.maxStreamUpdates,
      DEFAULT_MAX_STREAM_UPDATES,
      "maxStreamUpdates",
    ),
    maxStreamWaitMs: positiveInteger(
      options.maxStreamWaitMs,
      DEFAULT_MAX_STREAM_WAIT_MS,
      "maxStreamWaitMs",
    ),
    maxSecurities: positiveInteger(options.maxSecurities, DEFAULT_MAX_SECURITIES, "maxSecurities"),
    maxStringChars: positiveInteger(
      options.maxStringChars,
      DEFAULT_MAX_STRING_CHARS,
      "maxStringChars",
    ),
    validateFields: options.validateFields,
  };
}

export function isToolDisabled(
  options: NormalizedBloombergToolsOptions,
  name: BloombergToolName,
): boolean {
  return options.disabledTools.has(name);
}
