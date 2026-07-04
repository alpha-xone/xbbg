import * as z from "zod/v3";

import type { NormalizedBloombergToolsOptions } from "./options";
type ZodOutput<T> = z.ZodType<T, z.ZodTypeDef, unknown>;

export interface StringPair {
  readonly key: string;
  readonly value: string;
}

export interface FuturesCandidate {
  readonly ticker: string;
  readonly year: number;
  readonly month: number;
}

export type PrimitiveMap = Readonly<Record<string, string | number | boolean>>;

export type TickerOperation =
  | "parse_ticker"
  | "normalize_tickers"
  | "filter_equity_tickers"
  | "is_specific_contract"
  | "validate_generic_ticker";

export type SingleTickerOperation = Exclude<
  TickerOperation,
  "normalize_tickers" | "filter_equity_tickers"
>;

export interface SingleTickerInput {
  readonly operation: SingleTickerOperation;
  readonly ticker: string;
}

export interface TickerListInput {
  readonly operation: "normalize_tickers" | "filter_equity_tickers";
  readonly tickers: readonly string[];
}

export type TickerInput = SingleTickerInput | TickerListInput;

export type FuturesOperation =
  | "build_futures_ticker"
  | "generate_candidates"
  | "contract_index"
  | "filter_candidates_by_cycle"
  | "filter_valid_contracts"
  | "get_futures_months";

export interface FuturesBuildTickerInput {
  readonly operation: "build_futures_ticker";
  readonly prefix: string;
  readonly monthCode: string;
  readonly year: string;
  readonly asset: string;
}

export interface FuturesGenerateCandidatesInput {
  readonly operation: "generate_candidates";
  readonly genTicker: string;
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly freq?: string;
  readonly count?: number;
}

export interface FuturesContractIndexInput {
  readonly operation: "contract_index";
  readonly genTicker: string;
}

export interface FuturesFilterCandidatesByCycleInput {
  readonly operation: "filter_candidates_by_cycle";
  readonly candidates: readonly FuturesCandidate[];
  readonly cycle: string;
}

export interface FuturesFilterValidContractsInput {
  readonly operation: "filter_valid_contracts";
  readonly contracts: readonly StringPair[];
  readonly year: number;
  readonly month: number;
  readonly day: number;
}

export interface FuturesMonthsInput {
  readonly operation: "get_futures_months";
}

export type FuturesInput =
  | FuturesBuildTickerInput
  | FuturesGenerateCandidatesInput
  | FuturesContractIndexInput
  | FuturesFilterCandidatesByCycleInput
  | FuturesFilterValidContractsInput
  | FuturesMonthsInput;

export type CdxOperation =
  | "parse_cdx_ticker"
  | "previous_cdx_series"
  | "cdx_gen_to_specific"
  | "cdx_info"
  | "cdx_pricing"
  | "cdx_risk";

export interface CdxTickerInput {
  readonly operation: "parse_cdx_ticker" | "previous_cdx_series" | "cdx_info";
  readonly ticker: string;
}

export interface CdxMarketDataInput {
  readonly operation: "cdx_pricing" | "cdx_risk";
  readonly ticker: string;
  readonly recoveryRate?: number;
}

export interface CdxGenToSpecificInput {
  readonly operation: "cdx_gen_to_specific";
  readonly genTicker: string;
  readonly series: number;
}

export type CdxInput = CdxTickerInput | CdxMarketDataInput | CdxGenToSpecificInput;

export type CurrencyOperation = "build_fx_pair" | "same_currency" | "currencies_needing_conversion";

export interface FxPairInput {
  readonly operation: "build_fx_pair";
  readonly fromCcy: string;
  readonly toCcy: string;
}

export interface SameCurrencyInput {
  readonly operation: "same_currency";
  readonly ccy1: string;
  readonly ccy2: string;
}

export interface CurrencyConversionInput {
  readonly operation: "currencies_needing_conversion";
  readonly currencies: readonly string[];
  readonly target: string;
}

export type CurrencyInput = FxPairInput | SameCurrencyInput | CurrencyConversionInput;

export type BqlBuilderOperation =
  | "build_preferreds_query"
  | "build_corporate_bonds_query"
  | "build_etf_holdings_query";

export interface PreferredsQueryInput {
  readonly operation: "build_preferreds_query";
  readonly equityTicker: string;
  readonly extraFields?: readonly string[];
}

export interface CorporateBondsQueryInput {
  readonly operation: "build_corporate_bonds_query";
  readonly ticker: string;
  readonly ccy?: string;
  readonly extraFields?: readonly string[];
  readonly activeOnly?: boolean;
}

export interface EtfHoldingsQueryInput {
  readonly operation: "build_etf_holdings_query";
  readonly etfTicker: string;
  readonly extraFields?: readonly string[];
}

export type BqlBuilderInput =
  | PreferredsQueryInput
  | CorporateBondsQueryInput
  | EtfHoldingsQueryInput;

export type MarketSessionOperation =
  | "derive_sessions"
  | "get_market_rule"
  | "infer_timezone"
  | "session_times_to_utc"
  | "default_turnover_dates"
  | "default_bqr_datetimes"
  | "get_exchange_override"
  | "list_exchange_overrides";

export interface DeriveSessionsInput {
  readonly operation: "derive_sessions";
  readonly dayStart: string;
  readonly dayEnd: string;
  readonly mic?: string;
  readonly exchCode?: string;
}

export interface MarketRuleInput {
  readonly operation: "get_market_rule";
  readonly mic?: string;
  readonly exchCode?: string;
}

export interface InferTimezoneInput {
  readonly operation: "infer_timezone";
  readonly countryIso: string;
}

export interface SessionTimesToUtcInput {
  readonly operation: "session_times_to_utc";
  readonly startTime: string;
  readonly endTime: string;
  readonly exchangeTz: string;
  readonly date: string;
}

export interface TurnoverDatesInput {
  readonly operation: "default_turnover_dates";
  readonly startDate?: string;
  readonly endDate?: string;
}

export interface BqrDatetimesInput {
  readonly operation: "default_bqr_datetimes";
  readonly startDatetime?: string;
  readonly endDatetime?: string;
}

export interface ExchangeOverrideLookupInput {
  readonly operation: "get_exchange_override";
  readonly ticker: string;
}

export interface ListExchangeOverridesInput {
  readonly operation: "list_exchange_overrides";
}

export type MarketSessionInput =
  | DeriveSessionsInput
  | MarketRuleInput
  | InferTimezoneInput
  | SessionTimesToUtcInput
  | TurnoverDatesInput
  | BqrDatetimesInput
  | ExchangeOverrideLookupInput
  | ListExchangeOverridesInput;

export interface YasOverridesInput {
  readonly settleDt?: string;
  readonly yieldType?: number;
  readonly spread?: number;
  readonly yieldVal?: number;
  readonly price?: number;
  readonly benchmark?: string;
}

export type ConstantsOperation =
  | "parse_date"
  | "fmt_date"
  | "get_month_code"
  | "get_month_name"
  | "get_futures_months"
  | "get_dvd_type"
  | "get_dvd_types"
  | "get_dvd_cols"
  | "get_etf_cols";

export interface ParseDateInput {
  readonly operation: "parse_date";
  readonly dateStr: string;
}

export interface FmtDateInput {
  readonly operation: "fmt_date";
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly fmt?: string;
}

export interface MonthCodeInput {
  readonly operation: "get_month_code";
  readonly monthName: string;
}

export interface MonthNameInput {
  readonly operation: "get_month_name";
  readonly code: string;
}

export interface DvdTypeInput {
  readonly operation: "get_dvd_type";
  readonly dvdType: string;
}

export interface ConstantsLookupInput {
  readonly operation: "get_futures_months" | "get_dvd_types" | "get_dvd_cols" | "get_etf_cols";
}

export type ConstantsInput =
  | ParseDateInput
  | FmtDateInput
  | MonthCodeInput
  | MonthNameInput
  | DvdTypeInput
  | ConstantsLookupInput;

export type ColumnsOperation =
  | "rename_dividend_columns"
  | "rename_etf_columns"
  | "build_earning_header_rename";

export interface RenameColumnsInput {
  readonly operation: "rename_dividend_columns" | "rename_etf_columns";
  readonly columns: readonly string[];
}

export interface EarningHeaderRenameInput {
  readonly operation: "build_earning_header_rename";
  readonly headerRow: readonly StringPair[];
  readonly dataColumns: readonly string[];
}

export type ColumnsInput = RenameColumnsInput | EarningHeaderRenameInput;

export interface CalculateInput {
  readonly operation: "calculate_level_percentages";
  readonly values: readonly (number | null)[];
  readonly levels: readonly (number | null)[];
}

export type BloombergChartSource = "bdh" | "bdib" | "holdings" | "depth" | "rows";

export type ChartKind = "line" | "area" | "bar" | "scatter" | "candlestick" | "depth";

export type ChartRenderer = "vega-lite";

export type ChartScalar = string | number | boolean | null;

export type ChartRow = Readonly<Record<string, ChartScalar>>;

export interface ChartSpecInput {
  readonly source: BloombergChartSource;
  readonly rows: readonly ChartRow[];
  readonly renderer?: ChartRenderer;
  readonly chart?: ChartKind;
  readonly title?: string;
  readonly xField?: string;
  readonly yFields?: readonly string[];
  readonly seriesField?: string;
  readonly labelField?: string;
  readonly valueField?: string;
  readonly openField?: string;
  readonly highField?: string;
  readonly lowField?: string;
  readonly closeField?: string;
  readonly sideField?: string;
  readonly priceField?: string;
  readonly sizeField?: string;
  readonly maxPoints?: number;
}

const stringPairSchema = z.object({
  key: z.string().trim().min(1).describe("String pair key."),
  value: z.string().trim().min(1).describe("String pair value."),
});

const futuresCandidateSchema = z.object({
  month: z.number().int().min(1).max(12).describe("Contract month number, 1-12."),
  ticker: z.string().trim().min(1).describe("Specific Bloomberg futures ticker."),
  year: z.number().int().min(1900).describe("Contract year."),
});

function nonEmptyString(
  options: NormalizedBloombergToolsOptions,
  description: string,
): ZodOutput<string> {
  return z
    .string()
    .trim()
    .pipe(z.string().min(1).max(options.maxStringChars).describe(description));
}

function stringArray(
  options: NormalizedBloombergToolsOptions,
  description: string,
  maxItems = options.maxFields,
): ZodOutput<string[]> {
  return z.array(nonEmptyString(options, description)).min(1).max(maxItems).describe(description);
}

function optionalString(
  options: NormalizedBloombergToolsOptions,
  description: string,
): ZodOutput<string | undefined> {
  return nonEmptyString(options, description).optional();
}
export function tickerSchema(options: NormalizedBloombergToolsOptions): ZodOutput<TickerInput> {
  const ticker = nonEmptyString(
    options,
    "One generic futures-style Bloomberg ticker: <ROOT><N> ending in Index, Curncy, Comdty, or Corp, or <ROOT><N> <EXCHANGE> Equity. parse_ticker rejects other market sectors (Pfd, Govt, Muni, Mtge, M-Mkt) and non-futures securities.",
  );
  const tickers = stringArray(
    options,
    "Bloomberg tickers to normalize or filter.",
    options.maxSecurities,
  );
  return z.discriminatedUnion("operation", [
    z.object({ operation: z.literal("parse_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("is_specific_contract"), ticker }).strict(),
    z.object({ operation: z.literal("validate_generic_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("normalize_tickers"), tickers }).strict(),
    z.object({ operation: z.literal("filter_equity_tickers"), tickers }).strict(),
  ]);
}

export function futuresSchema(options: NormalizedBloombergToolsOptions): ZodOutput<FuturesInput> {
  const genTicker = nonEmptyString(
    options,
    "Generic Bloomberg futures ticker, for example ES1 Index.",
  );
  const year = z.number().int().describe("Contract year, for example 2024.");
  const month = z.number().int().min(1).max(12).describe("Month number, 1-12.");
  const day = z.number().int().min(1).max(31).describe("Day number, 1-31.");
  return z.discriminatedUnion("operation", [
    z
      .object({
        asset: nonEmptyString(
          options,
          "Bloomberg asset class suffix, for example Index or Comdty.",
        ),
        monthCode: nonEmptyString(options, "Bloomberg futures month code, for example H."),
        operation: z.literal("build_futures_ticker"),
        prefix: nonEmptyString(options, "Futures ticker root prefix, for example ES."),
        year: z
          .union([z.string().trim().min(1), z.number().int().transform(String)])
          .describe("Contract year, full or abbreviated, as a string or integer."),
      })
      .strict(),
    z
      .object({
        count: z
          .number()
          .int()
          .positive()
          .optional()
          .describe("Maximum number of futures candidates to generate."),
        day,
        freq: optionalString(options, "Futures frequency/cycle hint."),
        genTicker,
        month,
        operation: z.literal("generate_candidates"),
        year,
      })
      .strict(),
    z.object({ genTicker, operation: z.literal("contract_index") }).strict(),
    z
      .object({
        candidates: z
          .array(futuresCandidateSchema)
          .min(1)
          .max(options.maxFields)
          .describe("Candidate futures contracts."),
        cycle: nonEmptyString(options, "Futures cycle code to filter candidates by."),
        operation: z.literal("filter_candidates_by_cycle"),
      })
      .strict(),
    z
      .object({
        contracts: z
          .array(stringPairSchema)
          .min(1)
          .max(options.maxFields)
          .describe("Contract pairs for validity filtering."),
        day,
        month,
        operation: z.literal("filter_valid_contracts"),
        year,
      })
      .strict(),
    z.object({ operation: z.literal("get_futures_months") }).strict(),
  ]);
}

export function cdxSchema(options: NormalizedBloombergToolsOptions): ZodOutput<CdxInput> {
  const ticker = nonEmptyString(options, "CDX ticker, generic or specific.");
  const recoveryRate = z
    .number()
    .min(0)
    .max(1)
    .optional()
    .describe("Decimal recovery rate override, e.g. 0.4 for 40%; sent as the CDS_RR override.");
  return z.discriminatedUnion("operation", [
    z.object({ operation: z.literal("parse_cdx_ticker"), ticker }).strict(),
    z.object({ operation: z.literal("previous_cdx_series"), ticker }).strict(),
    z.object({ operation: z.literal("cdx_info"), ticker }).strict(),
    z.object({ operation: z.literal("cdx_pricing"), recoveryRate, ticker }).strict(),
    z.object({ operation: z.literal("cdx_risk"), recoveryRate, ticker }).strict(),
    z
      .object({
        genTicker: nonEmptyString(
          options,
          "Generic CDX ticker, for example CDX IG CDSI GEN 5Y Corp.",
        ),
        operation: z.literal("cdx_gen_to_specific"),
        series: z.number().int().positive().describe("Specific CDX series number."),
      })
      .strict(),
  ]);
}

export function currencySchema(options: NormalizedBloombergToolsOptions): ZodOutput<CurrencyInput> {
  return z.discriminatedUnion("operation", [
    z
      .object({
        fromCcy: nonEmptyString(options, "Source ISO currency code."),
        operation: z.literal("build_fx_pair"),
        toCcy: nonEmptyString(options, "Destination ISO currency code."),
      })
      .strict(),
    z
      .object({
        ccy1: nonEmptyString(options, "First ISO currency code."),
        ccy2: nonEmptyString(options, "Second ISO currency code."),
        operation: z.literal("same_currency"),
      })
      .strict(),
    z
      .object({
        currencies: stringArray(options, "ISO currency codes to check."),
        operation: z.literal("currencies_needing_conversion"),
        target: nonEmptyString(options, "Target ISO currency code."),
      })
      .strict(),
  ]);
}

export function bqlBuilderSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<BqlBuilderInput> {
  const extraFields = stringArray(options, "Extra BQL fields to include.").optional();
  return z.discriminatedUnion("operation", [
    z
      .object({
        equityTicker: nonEmptyString(options, "Equity ticker for preferreds query."),
        extraFields,
        operation: z.literal("build_preferreds_query"),
      })
      .strict(),
    z
      .object({
        activeOnly: z
          .boolean()
          .optional()
          .describe("Restrict corporate bond query to active bonds."),
        ccy: optionalString(options, "Currency filter for corporate bond query."),
        extraFields,
        operation: z.literal("build_corporate_bonds_query"),
        ticker: nonEmptyString(options, "Ticker for corporate bond query."),
      })
      .strict(),
    z
      .object({
        etfTicker: nonEmptyString(options, "ETF ticker for holdings query."),
        extraFields,
        operation: z.literal("build_etf_holdings_query"),
      })
      .strict(),
  ]);
}

export function marketSessionSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<MarketSessionInput> {
  const mic = optionalString(options, "Market Identifier Code, for example XNYS.");
  const exchCode = optionalString(options, "Bloomberg exchange code.");
  return z.discriminatedUnion("operation", [
    z
      .object({
        dayEnd: nonEmptyString(options, "Exchange day end time, for example 16:00."),
        dayStart: nonEmptyString(options, "Exchange day start time, for example 09:30."),
        exchCode,
        mic,
        operation: z.literal("derive_sessions"),
      })
      .strict(),
    z.object({ exchCode, mic, operation: z.literal("get_market_rule") }).strict(),
    z
      .object({
        countryIso: nonEmptyString(options, "ISO country code for timezone inference."),
        operation: z.literal("infer_timezone"),
      })
      .strict(),
    z
      .object({
        date: nonEmptyString(options, "Date for UTC session conversion, YYYY-MM-DD or YYYYMMDD."),
        endTime: nonEmptyString(options, "Session end time, for example 16:00."),
        exchangeTz: nonEmptyString(
          options,
          "IANA exchange timezone, for example America/New_York.",
        ),
        operation: z.literal("session_times_to_utc"),
        startTime: nonEmptyString(options, "Session start time, for example 09:30."),
      })
      .strict(),
    z
      .object({
        endDate: optionalString(options, "Optional end date."),
        operation: z.literal("default_turnover_dates"),
        startDate: optionalString(options, "Optional start date."),
      })
      .strict(),
    z
      .object({
        endDatetime: optionalString(options, "Optional end datetime."),
        operation: z.literal("default_bqr_datetimes"),
        startDatetime: optionalString(options, "Optional start datetime."),
      })
      .strict(),
    z
      .object({
        operation: z.literal("get_exchange_override"),
        ticker: nonEmptyString(options, "Ticker for exchange override lookup."),
      })
      .strict(),
    z.object({ operation: z.literal("list_exchange_overrides") }).strict(),
  ]);
}

export function yasOverridesSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<YasOverridesInput> {
  return z
    .object({
      benchmark: optionalString(options, "Optional YAS benchmark."),
      price: z.number().optional().describe("YAS price override."),
      settleDt: optionalString(options, "YAS settlement date."),
      spread: z.number().optional().describe("YAS spread override."),
      yieldType: z.number().int().optional().describe("YAS yield type override."),
      yieldVal: z.number().optional().describe("YAS yield value override."),
    })
    .strict();
}

export function constantsSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ConstantsInput> {
  return z.discriminatedUnion("operation", [
    z
      .object({
        dateStr: nonEmptyString(options, "Date string to parse."),
        operation: z.literal("parse_date"),
      })
      .strict(),
    z
      .object({
        day: z.number().int().min(1).max(31).describe("Day number, 1-31."),
        fmt: optionalString(options, "Date output format."),
        month: z.number().int().min(1).max(12).describe("Month number, 1-12."),
        operation: z.literal("fmt_date"),
        year: z.number().int().min(1).describe("Year number."),
      })
      .strict(),
    z
      .object({
        monthName: nonEmptyString(options, "Month name, for example March."),
        operation: z.literal("get_month_code"),
      })
      .strict(),
    z
      .object({
        code: nonEmptyString(options, "Month code, for example H."),
        operation: z.literal("get_month_name"),
      })
      .strict(),
    z
      .object({
        dvdType: nonEmptyString(options, "Dividend type code or label."),
        operation: z.literal("get_dvd_type"),
      })
      .strict(),
    z.object({ operation: z.literal("get_futures_months") }).strict(),
    z.object({ operation: z.literal("get_dvd_types") }).strict(),
    z.object({ operation: z.literal("get_dvd_cols") }).strict(),
    z.object({ operation: z.literal("get_etf_cols") }).strict(),
  ]);
}

export function columnsSchema(options: NormalizedBloombergToolsOptions): ZodOutput<ColumnsInput> {
  const columns = stringArray(options, "Column names to rename.");
  return z.discriminatedUnion("operation", [
    z.object({ columns, operation: z.literal("rename_dividend_columns") }).strict(),
    z.object({ columns, operation: z.literal("rename_etf_columns") }).strict(),
    z
      .object({
        dataColumns: stringArray(options, "Earnings data column names."),
        headerRow: z
          .array(stringPairSchema)
          .min(1)
          .max(options.maxFields)
          .describe("Earnings header row key/value pairs."),
        operation: z.literal("build_earning_header_rename"),
      })
      .strict(),
  ]);
}

export function calculateSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<CalculateInput> {
  return z
    .object({
      levels: z
        .array(z.number().nullable())
        .min(1)
        .max(options.maxFields)
        .describe("Hierarchy level values; supported levels are 1, 2, or null."),
      operation: z
        .literal("calculate_level_percentages")
        .describe("Numeric helper operation to run."),
      values: z
        .array(z.number().nullable())
        .min(1)
        .max(options.maxFields)
        .describe("Observed values."),
    })
    .strict()
    .superRefine((input, ctx) => {
      input.levels.forEach((level, index) => {
        if (level !== null && level !== 1 && level !== 2) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: "levels must contain only 1, 2, or null",
            path: ["levels", index],
          });
        }
      });
      if (input.values.length !== input.levels.length) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "values and levels must have the same length",
        });
      }
    });
}

export function chartSpecSchema(
  options: NormalizedBloombergToolsOptions,
): ZodOutput<ChartSpecInput> {
  const chartScalar = z.union([
    z.string().trim().max(options.maxStringChars),
    z.number(),
    z.boolean(),
    z.null(),
  ]);
  const fieldName = nonEmptyString(options, "Input row field name.");
  return z
    .object({
      chart: z
        .enum(["line", "area", "bar", "scatter", "candlestick", "depth"])
        .optional()
        .describe("Chart shape to generate. Defaults from source."),
      closeField: fieldName.optional().describe("Candlestick close-value field."),
      highField: fieldName.optional().describe("Candlestick high-value field."),
      labelField: fieldName.optional().describe("Categorical label field for bar charts."),
      lowField: fieldName.optional().describe("Candlestick low-value field."),
      maxPoints: z
        .number()
        .int()
        .positive()
        .max(options.maxRows)
        .optional()
        .describe("Maximum rows to include in the frontend spec; defaults to all provided rows."),
      openField: fieldName.optional().describe("Candlestick open-value field."),
      priceField: fieldName.optional().describe("Market-depth price field."),
      renderer: z
        .literal("vega-lite")
        .optional()
        .describe("Visualization spec renderer. Currently only vega-lite is generated."),
      rows: z
        .array(z.record(chartScalar))
        .min(1)
        .max(options.maxRows)
        .describe("Chart data rows copied from a bounded Bloomberg tool result."),
      seriesField: fieldName.optional().describe("Optional series/color field."),
      sideField: fieldName.optional().describe("Market-depth bid/ask side field."),
      sizeField: fieldName.optional().describe("Market-depth size field."),
      source: z
        .enum(["bdh", "bdib", "holdings", "depth", "rows"])
        .describe("Bloomberg result shape that produced rows."),
      title: nonEmptyString(options, "Chart title.").optional(),
      valueField: fieldName.optional().describe("Primary numeric value field."),
      xField: fieldName.optional().describe("X-axis field."),
      yFields: stringArray(options, "Numeric value fields to plot.").optional(),
    })
    .strict();
}
