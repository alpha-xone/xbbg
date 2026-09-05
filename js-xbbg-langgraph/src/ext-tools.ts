import { createBloombergStructuredTool, type ToolInvocationConfig } from "./langchain-tool";
import { createChartSpec } from "./chart-spec";

import { CDX_INFO_FIELDS, CDX_PRICING_FIELDS, CDX_RISK_FIELDS } from "./cdx-fields";
import { createCoreResolver, type CoreResolver } from "./core-loader";
import {
  EXT_BQL_BUILDER_DESCRIPTION,
  EXT_CALCULATE_DESCRIPTION,
  EXT_CDX_DESCRIPTION,
  EXT_CHART_SPEC_DESCRIPTION,
  EXT_COLUMNS_DESCRIPTION,
  EXT_CONSTANTS_DESCRIPTION,
  EXT_CURRENCY_DESCRIPTION,
  EXT_FUTURES_DESCRIPTION,
  EXT_MARKET_SESSION_DESCRIPTION,
  EXT_TICKER_DESCRIPTION,
  EXT_YAS_OVERRIDES_DESCRIPTION,
} from "./descriptions";
import type { BloombergToolName, BloombergToolsOptions } from "./options";
import { isToolDisabled } from "./options";
import type { BloombergTool } from "./tools";
import {
  createToolResult,
  throwWithToolContext,
  type ToolContentAndArtifact,
} from "./result-limits";
import {
  bqlBuilderSchema,
  calculateSchema,
  cdxSchema,
  chartSpecSchema,
  columnsSchema,
  constantsSchema,
  currencySchema,
  futuresSchema,
  marketSessionSchema,
  tickerSchema,
  yasOverridesSchema,
  type BqlBuilderInput,
  type CalculateInput,
  type CdxInput,
  type ChartSpecInput,
  type ColumnsInput,
  type ConstantsInput,
  type CurrencyInput,
  type FuturesInput,
  type MarketSessionInput,
  type PrimitiveMap,
  type TickerInput,
  type YasOverridesInput,
} from "./ext-schemas";
function resultString(
  resolver: CoreResolver,
  name: BloombergToolName,
  value: unknown,
): ToolContentAndArtifact {
  return createToolResult(name, value, resolver.options);
}

function recoveryOverrides(recoveryRate: number | undefined): PrimitiveMap | undefined {
  return recoveryRate === undefined ? undefined : { CDS_RR: recoveryRate };
}

interface ExtToolDefinition {
  readonly create: (resolver: CoreResolver) => BloombergTool;
  readonly name: BloombergToolName;
}

const EXT_TOOL_DEFINITIONS: readonly ExtToolDefinition[] = Object.freeze([
  { create: extTickerWithResolver, name: "xbbg_ext_ticker" },
  { create: extFuturesWithResolver, name: "xbbg_ext_futures" },
  { create: extCdxWithResolver, name: "xbbg_ext_cdx" },
  { create: extCurrencyWithResolver, name: "xbbg_ext_currency" },
  { create: extBqlBuilderWithResolver, name: "xbbg_ext_bql_builder" },
  { create: extChartSpecWithResolver, name: "xbbg_ext_chart_spec" },
  { create: extMarketSessionWithResolver, name: "xbbg_ext_market_session" },
  { create: extYasOverridesWithResolver, name: "xbbg_ext_yas_overrides" },
  { create: extConstantsWithResolver, name: "xbbg_ext_constants" },
  { create: extColumnsWithResolver, name: "xbbg_ext_columns" },
  { create: extCalculateWithResolver, name: "xbbg_ext_calculate" },
]);

export const BLOOMBERG_EXT_TOOL_NAMES = Object.freeze(
  EXT_TOOL_DEFINITIONS.map((definition) => definition.name),
);

function extChartSpecWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_chart_spec" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ChartSpecInput): Promise<ToolContentAndArtifact> => {
      try {
        return await Promise.resolve(resultString(resolver, name, createChartSpec(input)));
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_CHART_SPEC_DESCRIPTION,
      name,
      schema: chartSpecSchema(resolver.options),
    },
  );
}

function extTickerWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_ticker" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: TickerInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "parse_ticker":
            return resultString(resolver, name, core.ext.parseTicker(input.ticker));
          case "normalize_tickers":
            return resultString(resolver, name, core.ext.normalizeTickers(input.tickers));
          case "filter_equity_tickers":
            return resultString(resolver, name, core.ext.filterEquityTickers(input.tickers));
          case "is_specific_contract":
            return resultString(resolver, name, core.ext.isSpecificContract(input.ticker));
          case "validate_generic_ticker":
            core.ext.validateGenericTicker(input.ticker);
            return resultString(resolver, name, { ticker: input.ticker, valid: true });
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_TICKER_DESCRIPTION,
      name,
      schema: tickerSchema(resolver.options),
    },
  );
}

function extFuturesWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_futures" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: FuturesInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "build_futures_ticker":
            return resultString(
              resolver,
              name,
              core.ext.buildFuturesTicker(input.prefix, input.monthCode, input.year, input.asset),
            );
          case "generate_candidates":
            return resultString(
              resolver,
              name,
              core.ext.generateFuturesCandidates(
                input.genTicker,
                input.year,
                input.month,
                input.day,
                input.freq,
                input.count,
              ),
            );
          case "contract_index":
            return resultString(resolver, name, core.ext.contractIndex(input.genTicker));
          case "filter_candidates_by_cycle":
            return resultString(
              resolver,
              name,
              core.ext.filterCandidatesByCycle(input.candidates, input.cycle),
            );
          case "filter_valid_contracts":
            return resultString(
              resolver,
              name,
              core.ext.filterValidContracts(input.contracts, input.year, input.month, input.day),
            );
          case "get_futures_months":
            return resultString(resolver, name, core.ext.getFuturesMonths());
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_FUTURES_DESCRIPTION,
      name,
      schema: futuresSchema(resolver.options),
    },
  );
}

function extCdxWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_cdx" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CdxInput, config?: ToolInvocationConfig): Promise<ToolContentAndArtifact> => {
      try {
        config?.signal?.throwIfAborted();
        if (
          input.operation === "cdx_info" ||
          input.operation === "cdx_pricing" ||
          input.operation === "cdx_risk"
        ) {
          const engine = await resolver.getEngine();
          const fields =
            input.operation === "cdx_info"
              ? CDX_INFO_FIELDS
              : input.operation === "cdx_pricing"
                ? CDX_PRICING_FIELDS
                : CDX_RISK_FIELDS;
          const result = await engine.bdp([input.ticker], fields, {
            backend: "json",
            overrides: recoveryOverrides(
              input.operation === "cdx_pricing" || input.operation === "cdx_risk"
                ? input.recoveryRate
                : undefined,
            ),
          });
          return resultString(resolver, name, result);
        }
        const core = await resolver.getCore();
        switch (input.operation) {
          case "parse_cdx_ticker":
            return resultString(resolver, name, core.ext.parseCdxTicker(input.ticker));
          case "previous_cdx_series":
            return resultString(resolver, name, core.ext.previousCdxSeries(input.ticker));
          case "cdx_gen_to_specific":
            return resultString(
              resolver,
              name,
              core.ext.cdxGenToSpecific(input.genTicker, input.series),
            );
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_CDX_DESCRIPTION,
      name,
      schema: cdxSchema(resolver.options),
    },
  );
}

function extCurrencyWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_currency" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CurrencyInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "build_fx_pair":
            return resultString(resolver, name, core.ext.buildFxPair(input.fromCcy, input.toCcy));
          case "same_currency":
            return resultString(resolver, name, core.ext.sameCurrency(input.ccy1, input.ccy2));
          case "currencies_needing_conversion":
            return resultString(
              resolver,
              name,
              core.ext.currenciesNeedingConversion(input.currencies, input.target),
            );
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_CURRENCY_DESCRIPTION,
      name,
      schema: currencySchema(resolver.options),
    },
  );
}

function extBqlBuilderWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_bql_builder" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: BqlBuilderInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "build_preferreds_query":
            return resultString(
              resolver,
              name,
              core.ext.buildPreferredsQuery(input.equityTicker, input.extraFields),
            );
          case "build_corporate_bonds_query":
            return resultString(
              resolver,
              name,
              core.ext.buildCorporateBondsQuery(
                input.ticker,
                input.ccy,
                input.extraFields,
                input.activeOnly,
              ),
            );
          case "build_etf_holdings_query":
            return resultString(
              resolver,
              name,
              core.ext.buildEtfHoldingsQuery(input.etfTicker, input.extraFields),
            );
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_BQL_BUILDER_DESCRIPTION,
      name,
      schema: bqlBuilderSchema(resolver.options),
    },
  );
}

function extMarketSessionWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_market_session" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: MarketSessionInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "derive_sessions":
            return resultString(
              resolver,
              name,
              core.ext.deriveSessions(input.dayStart, input.dayEnd, input.mic, input.exchCode),
            );
          case "get_market_rule":
            return resultString(resolver, name, core.ext.getMarketRule(input.mic, input.exchCode));
          case "infer_timezone":
            return resultString(resolver, name, core.ext.inferTimezone(input.countryIso));
          case "session_times_to_utc":
            return resultString(
              resolver,
              name,
              core.ext.sessionTimesToUtc(
                input.startTime,
                input.endTime,
                input.exchangeTz,
                input.date,
              ),
            );
          case "default_turnover_dates":
            return resultString(
              resolver,
              name,
              core.ext.defaultTurnoverDates(input.startDate, input.endDate),
            );
          case "default_bqr_datetimes":
            return resultString(
              resolver,
              name,
              core.ext.defaultBqrDatetimes(input.startDatetime, input.endDatetime),
            );
          case "get_exchange_override":
            return resultString(resolver, name, core.ext.getExchangeOverride(input.ticker));
          case "list_exchange_overrides":
            return resultString(resolver, name, core.ext.listExchangeOverrides());
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_MARKET_SESSION_DESCRIPTION,
      name,
      schema: marketSessionSchema(resolver.options),
    },
  );
}

function extYasOverridesWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_yas_overrides" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: YasOverridesInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        return resultString(
          resolver,
          name,
          core.ext.buildYasOverrides(
            input.settleDt,
            input.yieldType,
            input.spread,
            input.yieldVal,
            input.price,
            input.benchmark,
          ),
        );
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_YAS_OVERRIDES_DESCRIPTION,
      name,
      schema: yasOverridesSchema(resolver.options),
    },
  );
}

function extConstantsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_constants" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ConstantsInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "parse_date":
            return resultString(resolver, name, core.ext.parseDate(input.dateStr));
          case "fmt_date":
            return resultString(
              resolver,
              name,
              core.ext.fmtDate(input.year, input.month, input.day, input.fmt),
            );
          case "get_month_code":
            return resultString(resolver, name, core.ext.getMonthCode(input.monthName));
          case "get_month_name":
            return resultString(resolver, name, core.ext.getMonthName(input.code));
          case "get_futures_months":
            return resultString(resolver, name, core.ext.getFuturesMonths());
          case "get_dvd_type":
            return resultString(resolver, name, core.ext.getDvdType(input.dvdType));
          case "get_dvd_types":
            return resultString(resolver, name, core.ext.getDvdTypes());
          case "get_dvd_cols":
            return resultString(resolver, name, core.ext.getDvdCols());
          case "get_etf_cols":
            return resultString(resolver, name, core.ext.getEtfCols());
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_CONSTANTS_DESCRIPTION,
      name,
      schema: constantsSchema(resolver.options),
    },
  );
}

function extColumnsWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_columns" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: ColumnsInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        switch (input.operation) {
          case "rename_dividend_columns":
            return resultString(resolver, name, core.ext.renameDividendColumns(input.columns));
          case "rename_etf_columns":
            return resultString(resolver, name, core.ext.renameEtfColumns(input.columns));
          case "build_earning_header_rename":
            return resultString(
              resolver,
              name,
              core.ext.buildEarningHeaderRename(input.headerRow, input.dataColumns),
            );
        }
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_COLUMNS_DESCRIPTION,
      name,
      schema: columnsSchema(resolver.options),
    },
  );
}

function extCalculateWithResolver(resolver: CoreResolver): BloombergTool {
  const name = "xbbg_ext_calculate" satisfies BloombergToolName;
  return createBloombergStructuredTool(
    async (input: CalculateInput): Promise<ToolContentAndArtifact> => {
      try {
        const core = await resolver.getCore();
        return resultString(
          resolver,
          name,
          core.ext.calculateLevelPercentages(input.values, input.levels),
        );
      } catch (error) {
        throwWithToolContext(name, error);
      }
    },
    {
      responseFormat: "content_and_artifact",
      description: EXT_CALCULATE_DESCRIPTION,
      name,
      schema: calculateSchema(resolver.options),
    },
  );
}

export function createExtTickerTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extTickerWithResolver(createCoreResolver(options));
}

export function createExtFuturesTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extFuturesWithResolver(createCoreResolver(options));
}

export function createExtCdxTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCdxWithResolver(createCoreResolver(options));
}

export function createExtCurrencyTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCurrencyWithResolver(createCoreResolver(options));
}

export function createExtBqlBuilderTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extBqlBuilderWithResolver(createCoreResolver(options));
}

export function createExtMarketSessionTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extMarketSessionWithResolver(createCoreResolver(options));
}

export function createExtYasOverridesTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extYasOverridesWithResolver(createCoreResolver(options));
}

export function createExtConstantsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extConstantsWithResolver(createCoreResolver(options));
}

export function createExtColumnsTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extColumnsWithResolver(createCoreResolver(options));
}

export function createExtCalculateTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extCalculateWithResolver(createCoreResolver(options));
}

export function createExtChartSpecTool(options: BloombergToolsOptions = {}): BloombergTool {
  return extChartSpecWithResolver(createCoreResolver(options));
}

export function createBloombergExtToolsForResolver(resolver: CoreResolver): BloombergTool[] {
  return EXT_TOOL_DEFINITIONS.filter(
    (definition) => !isToolDisabled(resolver.options, definition.name),
  ).map((definition) => definition.create(resolver));
}

export function createBloombergExtTools(options: BloombergToolsOptions = {}): BloombergTool[] {
  return createBloombergExtToolsForResolver(createCoreResolver(options));
}
