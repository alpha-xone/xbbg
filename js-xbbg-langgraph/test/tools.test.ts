import type { StructuredToolInterface } from "@langchain/core/tools";
import { AIMessage, ToolMessage } from "@langchain/core/messages";
import { ToolNode } from "@langchain/langgraph/prebuilt";
import { ChatOpenAI } from "@langchain/openai";
import { createAgent } from "langchain";

import {
  BLOOMBERG_TOOL_INSTRUCTIONS,
  BLOOMBERG_TOOL_NAMES,
  createAllBloombergTools,
  createBloombergTools,
  getBloombergToolInstructions,
  toolParameterJsonSchema,
} from "../src";
import { limitResult } from "../src/result-limits";
import type { XbbgCoreLike, XbbgEngineLike } from "../src/core-loader";

type CoreSubscription = Awaited<ReturnType<XbbgEngineLike["stream"]>>;

function fakeSubscription(updates: readonly unknown[], error?: Error) {
  let index = 0;
  const subscription = {
    next: vi.fn(async () => {
      if (error !== undefined) {
        throw error;
      }
      if (index >= updates.length) {
        return { done: true, value: undefined } as IteratorResult<unknown>;
      }
      const value = updates[index];
      index += 1;
      return { done: false, value } as IteratorResult<unknown>;
    }),
    unsubscribe: vi.fn(async () => []),
  };
  return {
    next: subscription.next,
    subscription: subscription as unknown as CoreSubscription,
    unsubscribe: subscription.unsubscribe,
  };
}

function fakeEngine(): XbbgEngineLike {
  return {
    bdp: vi.fn(async () => [
      { ticker: "AAPL US Equity", field: "PX_LAST", value: "1234567890123456789012345" },
    ]),
    bdh: vi.fn(async () => [{ ticker: "AAPL US Equity", date: "20240102", value: 1 }]),
    bdib: vi.fn(async () => [{ time: "2024-01-02T09:30:00-05:00", close: 1 }]),
    bdtick: vi.fn(async () => [{ time: "2024-01-02T09:30:00-05:00", type: "TRADE", value: 1 }]),
    bds: vi.fn(async () => [{ member: "AAPL US Equity" }]),
    checkEntitlements: vi.fn(async () => ({
      entitled: true,
      failedEids: [],
    })),
    bflds: vi.fn(async () => [{ id: "PX_LAST" }]),
    bql: vi.fn(async () => [{ value: 1 }]),
    bsrch: vi.fn(async () => [{ security: "AAPL US Equity" }]),
    bqr: vi.fn(async () => [{ broker_buy: "DLR", event_type: "BID", price: 99 }]),
    beqs: vi.fn(async () => [{ security: "AAPL US Equity" }]),
    yas: vi.fn(async () => [{ field: "YAS_BOND_YLD", value: 5 }]),
    preferreds: vi.fn(async () => [{ ticker: "AAPL 4.5 Pfd" }]),
    corporateBonds: vi.fn(async () => [{ ticker: "AAPL 3.25 02/23/26 Corp" }]),
    indexMembers: vi.fn(async () => [{ member: "AAPL US Equity" }]),
    resolveIsins: vi.fn(async () => [{ isin: "US0378331005", security: "AAPL US Equity" }]),
    issuerIsins: vi.fn(async () => [{ issuer: "Apple Inc", isin: "US037833FB15" }]),
    etfHoldings: vi.fn(async () => [{ ticker: "AAPL US Equity", weight: 0.07 }]),
    stream: vi.fn(async () => fakeSubscription([]).subscription),
    mktbar: vi.fn(async () => fakeSubscription([]).subscription),
    depth: vi.fn(async () => fakeSubscription([]).subscription),
  };
}

function fakeCore(engine: XbbgEngineLike): XbbgCoreLike {
  return {
    connect: vi.fn(async () => engine),
    ext: {
      buildCorporateBondsQuery: vi.fn(() => "corp-bonds-query"),
      buildEarningHeaderRename: vi.fn(() => [{ key: "OLD", value: "NEW" }]),
      buildEtfHoldingsQuery: vi.fn(() => "etf-query"),
      buildFuturesTicker: vi.fn(() => "ESH4 Index"),
      buildFxPair: vi.fn(() => ({
        factor: 1,
        fromCcy: "USD",
        fxPair: "USDEUR Curncy",
        toCcy: "EUR",
      })),
      buildPreferredsQuery: vi.fn(() => "preferreds-query"),
      buildYasOverrides: vi.fn(() => [{ key: "YAS_BOND_PX", value: "99" }]),
      calculateLevelPercentages: vi.fn(() => [0.1, null]),
      cdx: {
        acdx_info: vi.fn(async () => []),
        acdx_pricing: vi.fn(async () => []),
        acdx_risk: vi.fn(async () => []),
      },
      cdxGenToSpecific: vi.fn(() => "CDX IG S40 5Y Corp"),
      clearExchangeOverride: vi.fn(),
      contractIndex: vi.fn(() => 1),
      currenciesNeedingConversion: vi.fn(() => ["EUR"]),
      defaultBqrDatetimes: vi.fn(() => ({
        end: "2024-01-02T10:00:00",
        start: "2024-01-02T09:30:00",
      })),
      defaultTurnoverDates: vi.fn(() => ({ end: "20240131", start: "20240101" })),
      deriveSessions: vi.fn(() => ({ day: { end: "16:00", start: "09:30" } })),
      filterCandidatesByCycle: vi.fn(() => [{ month: 3, ticker: "ESH4 Index", year: 2024 }]),
      filterEquityTickers: vi.fn(() => ["AAPL US Equity"]),
      filterValidContracts: vi.fn(() => ["ESH4 Index"]),
      fmtDate: vi.fn(() => "20240102"),
      generateFuturesCandidates: vi.fn(() => [{ month: 3, ticker: "ESH4 Index", year: 2024 }]),
      getDvdCols: vi.fn(() => [{ key: "dvd", value: "Dividend" }]),
      getDvdType: vi.fn(() => "Cash"),
      getDvdTypes: vi.fn(() => [{ key: "C", value: "Cash" }]),
      getEtfCols: vi.fn(() => [{ key: "ticker", value: "Ticker" }]),
      getExchangeOverride: vi.fn(() => null),
      getFuturesMonths: vi.fn(() => [{ key: "H", value: "March" }]),
      getMarketRule: vi.fn(() => ({ isContinuous: true, postMinutes: 0, preMinutes: 0 })),
      getMonthCode: vi.fn(() => "H"),
      getMonthName: vi.fn(() => "March"),
      inferTimezone: vi.fn(() => "America/New_York"),
      isLongFormat: vi.fn(() => false),
      isSpecificContract: vi.fn(() => true),
      listExchangeOverrides: vi.fn(() => []),
      normalizeTickers: vi.fn(() => ["AAPL US Equity"]),
      parseCdxTicker: vi.fn(() => ({
        asset: "Corp",
        index: "CDX IG",
        isGeneric: true,
        series: "S40",
        tenor: "5Y",
      })),
      parseDate: vi.fn(() => [2024, 1, 2]),
      parseTicker: vi.fn(() => ({ asset: "Equity", index: 0, prefix: "AAPL" })),
      pivotToWide: vi.fn((buffer: Buffer) => buffer),
      previousCdxSeries: vi.fn(() => "CDX IG S39 5Y Corp"),
      renameDividendColumns: vi.fn(() => [{ key: "DVD", value: "Dividend" }]),
      renameEtfColumns: vi.fn(() => [{ key: "ETF", value: "ETF" }]),
      sameCurrency: vi.fn(() => false),
      sessionTimesToUtc: vi.fn(() => ({ end: "21:00", start: "14:30" })),
      setExchangeOverride: vi.fn(),
      validateGenericTicker: vi.fn(),
    },
  };
}

function byName(tools: readonly StructuredToolInterface[], name: string): StructuredToolInterface {
  const found = tools.find((entry) => entry.name === name);
  if (found === undefined) {
    throw new Error(`Missing tool ${name}`);
  }
  return found;
}

async function invokeArtifact(tool: StructuredToolInterface, input: unknown) {
  const result = await tool.invoke(input, {
    toolCall: { args: {}, id: `call_${tool.name}`, name: tool.name },
  });
  if (!ToolMessage.isInstance(result)) {
    throw new TypeError(`Expected ToolMessage from ${tool.name}`);
  }
  if (typeof result.content !== "string") {
    throw new TypeError(`Expected string content from ${tool.name}`);
  }
  return [result.content, result.artifact] as const;
}

async function invokeJson(tool: StructuredToolInterface, input: unknown) {
  const [, artifact] = await invokeArtifact(tool, input);
  return artifact as Record<string, any>;
}

describe("Bloomberg request tools", () => {
  it("creates tool objects without connecting to Bloomberg", () => {
    const engine = fakeEngine();
    const core = fakeCore(engine);

    const tools = createAllBloombergTools({ core });

    expect(tools.map((entry) => entry.name)).toContain("xbbg_bdp");
    expect(core.connect).not.toHaveBeenCalled();
  });

  it("constructs LangChain agent and bindTools workflows without provider calls", () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });
    const model = new ChatOpenAI({ apiKey: "test-api-key", model: "gpt-4.1" });
    const boundModel = model.bindTools(tools);
    const toolNode = new ToolNode(tools);
    const agent = createAgent({
      model,
      systemPrompt: BLOOMBERG_TOOL_INSTRUCTIONS,
      tools,
    });

    expect(typeof boundModel.invoke).toBe("function");
    expect(toolNode).toBeInstanceOf(ToolNode);
    expect(typeof agent.invoke).toBe("function");
    expect(engine.bdp).not.toHaveBeenCalled();
  });

  it("honors disabledTools for request and recipe tools", () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({
      core: fakeCore(engine),
      disabledTools: ["xbbg_beqs", "xbbg_etf_holdings", "xbbg_stream_snapshot"],
    });

    const names = tools.map((entry) => entry.name);
    expect(names).not.toContain("xbbg_beqs");
    expect(names).not.toContain("xbbg_etf_holdings");
    expect(names).not.toContain("xbbg_stream_snapshot");
    expect(names).toContain("xbbg_yas");
  });

  it("memoizes lazy engine creation across parallel tool calls", async () => {
    const engine = fakeEngine();
    let release!: () => void;
    const started = new Promise<void>((resolve) => {
      release = resolve;
    });
    const connect = vi.fn(async () => {
      await started;
      return engine;
    });
    const core = { ...fakeCore(engine), connect } as unknown as XbbgCoreLike;
    const tools = createBloombergTools({ core });

    const bdpPromise = byName(tools, "xbbg_bdp").invoke({
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
    });
    const bdhPromise = byName(tools, "xbbg_bdh").invoke({
      end: "2024-01-02",
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
      start: "2024-01-01",
    });

    release();
    await Promise.all([bdpPromise, bdhPromise]);
    expect(connect).toHaveBeenCalledTimes(1);
  });

  it("retries lazy engine creation after a rejected connect", async () => {
    const engine = fakeEngine();
    let attempts = 0;
    const connect = vi.fn(async () => {
      attempts += 1;
      if (attempts === 1) {
        throw new Error("temporary connect failure");
      }
      return engine;
    });
    const core = { ...fakeCore(engine), connect } as unknown as XbbgCoreLike;
    const tools = createBloombergTools({ core });
    const bdp = byName(tools, "xbbg_bdp");

    await expect(
      bdp.invoke({ fields: ["PX_LAST"], securities: ["AAPL US Equity"] }),
    ).rejects.toThrow(/temporary connect failure/u);

    const result = await invokeJson(bdp, { fields: ["PX_LAST"], securities: ["AAPL US Equity"] });

    expect(connect).toHaveBeenCalledTimes(2);
    expect(engine.bdp).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({ rowCount: 1, tool: "xbbg_bdp" });
  });

  it("calls every request method with json backend and normalized inputs", async () => {
    const engine = fakeEngine();
    const core = fakeCore(engine);
    const tools = createBloombergTools({
      core,
      maxRows: 1,
      maxStringChars: 20,
      validateFields: true,
    });

    const bdp = await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: [" PX_LAST "],
      overrides: {
        "AAPL US Equity": { EQY_FUND_CRNCY: " EUR " },
        EQY_FUND_CRNCY: " USD ",
      },
      securities: [" AAPL US Equity "],
    });
    expect(engine.bdp).toHaveBeenCalledWith(
      ["AAPL US Equity"],
      ["PX_LAST"],
      expect.objectContaining({
        backend: "json",
        overrides: {
          "AAPL US Equity": { EQY_FUND_CRNCY: "EUR" },
          EQY_FUND_CRNCY: "USD",
        },
        validateFields: true,
      }),
    );
    expect(bdp).toMatchObject({ rowCount: 1, tool: "xbbg_bdp", truncated: true });
    expect(bdp.data[0].value).toContain("[truncated");

    await invokeJson(byName(tools, "xbbg_bdh"), {
      end: "2024-01-31",
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
      start: "20240101",
    });
    expect(engine.bdh).toHaveBeenCalledWith(
      ["AAPL US Equity"],
      ["PX_LAST"],
      expect.objectContaining({ backend: "json", end: "20240131", start: "20240101" }),
    );

    await invokeJson(byName(tools, "xbbg_bds"), {
      field: " INDX_MEMBERS ",
      securities: ["SPX Index"],
    });
    expect(engine.bds).toHaveBeenCalledWith(
      ["SPX Index"],
      ["INDX_MEMBERS"],
      expect.objectContaining({ backend: "json" }),
    );

    await invokeJson(byName(tools, "xbbg_bdib"), {
      end: "2024-01-02T16:00:00-05:00",
      interval: 5,
      start: "2024-01-02 09:30:00",
      ticker: "AAPL US Equity",
    });
    expect(engine.bdib).toHaveBeenCalledWith(
      "AAPL US Equity",
      expect.objectContaining({ backend: "json", interval: 5, start: "2024-01-02T09:30:00" }),
    );

    await invokeJson(byName(tools, "xbbg_bdtick"), {
      end: "2024-01-02T10:00:00-05:00",
      eventTypes: [" BID ", "ASK"],
      includeBrokerCodes: true,
      start: "2024-01-02T09:30:00-05:00",
      ticker: "AAPL US Equity",
    });
    expect(engine.bdtick).toHaveBeenCalledWith(
      "AAPL US Equity",
      expect.objectContaining({
        backend: "json",
        eventTypes: ["BID", "ASK"],
        includeBrokerCodes: true,
      }),
    );

    await invokeJson(byName(tools, "xbbg_bql"), { query: " get(px_last) for([AAPL US Equity]) " });
    expect(engine.bql).toHaveBeenCalledWith(
      "get(px_last) for([AAPL US Equity])",
      expect.objectContaining({ backend: "json" }),
    );

    await invokeJson(byName(tools, "xbbg_bsrch"), { searchSpec: " COMDTY:NG " });
    expect(engine.bsrch).toHaveBeenCalledWith(
      "COMDTY:NG",
      expect.objectContaining({ backend: "json" }),
    );

    await invokeJson(byName(tools, "xbbg_bqr"), {
      end: "2024-06-03T15:00:00",
      eventTypes: ["BID", "ASK"],
      start: "2024-06-03T14:30:00",
      ticker: "IBM US Equity@MSG1",
    });
    expect(engine.bqr).toHaveBeenCalledWith(
      "IBM US Equity@MSG1",
      expect.objectContaining({
        backend: "json",
        endDatetime: "2024-06-03T15:00:00",
        eventTypes: ["BID", "ASK"],
        startDatetime: "2024-06-03T14:30:00",
      }),
    );

    await invokeJson(byName(tools, "xbbg_bflds"), { fields: [" PX_LAST "] });
    expect(engine.bflds).toHaveBeenCalledWith(
      expect.objectContaining({ backend: "json", fields: ["PX_LAST"] }),
    );

    await invokeJson(byName(tools, "xbbg_beqs"), {
      asof: "2024-01-02",
      group: " General ",
      overrides: { LANG: " EN " },
      screen: " Capital Goods ",
      screenType: " PRIVATE ",
    });
    expect(engine.beqs).toHaveBeenCalledWith(
      "Capital Goods",
      expect.objectContaining({
        asof: "20240102",
        backend: "json",
        group: "General",
        overrides: { LANG: "EN" },
        screenType: "PRIVATE",
      }),
    );

    await invokeJson(byName(tools, "xbbg_yas"), {
      fields: [" YAS_BOND_YLD "],
      price: 99,
      settleDt: "2024-01-02",
      tickers: [" IBM Corp "],
    });
    expect(engine.yas).toHaveBeenCalledWith(
      ["IBM Corp"],
      ["YAS_BOND_YLD"],
      expect.objectContaining({ backend: "json", price: 99, settleDt: "20240102" }),
    );

    await invokeJson(byName(tools, "xbbg_preferreds"), {
      equityTicker: " AAPL US Equity ",
      fields: [" id "],
    });
    expect(engine.preferreds).toHaveBeenCalledWith(
      "AAPL US Equity",
      expect.objectContaining({ backend: "json", fields: ["id"] }),
    );

    await invokeJson(byName(tools, "xbbg_preferreds"), {
      equityTicker: " JPM US Equity ",
      fields: [],
    });
    expect(engine.preferreds).toHaveBeenLastCalledWith(
      "JPM US Equity",
      expect.objectContaining({ backend: "json" }),
    );
    expect(vi.mocked(engine.preferreds).mock.calls.at(-1)?.[1]?.fields).toBeUndefined();

    await invokeJson(byName(tools, "xbbg_corporate_bonds"), {
      activeOnly: false,
      ccy: " USD ",
      fields: [" id "],
      ticker: " AAPL US Equity ",
    });
    expect(engine.corporateBonds).toHaveBeenCalledWith(
      "AAPL US Equity",
      expect.objectContaining({
        activeOnly: false,
        backend: "json",
        ccy: "USD",
        fields: ["id"],
      }),
    );

    await invokeJson(byName(tools, "xbbg_index_members"), {
      asof: "2024-01-31",
      field: "INDX_MEMBERS",
      index: " SPX Index ",
    });
    expect(engine.indexMembers).toHaveBeenCalledWith(
      "SPX Index",
      expect.objectContaining({ asof: "20240131", backend: "json", field: "INDX_MEMBERS" }),
    );

    await invokeJson(byName(tools, "xbbg_resolve_isins"), { isins: [" US0378331005 "] });
    expect(engine.resolveIsins).toHaveBeenCalledWith(
      ["US0378331005"],
      expect.objectContaining({ backend: "json" }),
    );

    await invokeJson(byName(tools, "xbbg_issuer_isins"), { bondIsins: [" US037833FB15 "] });
    expect(engine.issuerIsins).toHaveBeenCalledWith(
      ["US037833FB15"],
      expect.objectContaining({ backend: "json" }),
    );

    await invokeJson(byName(tools, "xbbg_etf_holdings"), {
      etfTicker: " SPY US Equity ",
      fields: [" id "],
    });
    expect(engine.etfHoldings).toHaveBeenCalledWith(
      "SPY US Equity",
      expect.objectContaining({ backend: "json", fields: ["id"] }),
    );
  });

  it("forwards returnEids through all five capable typed request tools", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });
    await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      returnEids: true,
      securities: ["AAPL US Equity"],
    });
    await invokeJson(byName(tools, "xbbg_bds"), {
      field: "INDX_MEMBERS",
      returnEids: true,
      securities: ["SPX Index"],
    });
    await invokeJson(byName(tools, "xbbg_bdh"), {
      end: "20240102",
      fields: ["PX_LAST"],
      returnEids: true,
      securities: ["AAPL US Equity"],
      start: "20240101",
    });
    await invokeJson(byName(tools, "xbbg_bdib"), {
      end: "2024-01-02T10:00:00Z",
      interval: 5,
      returnEids: true,
      start: "2024-01-02T09:30:00Z",
      ticker: "AAPL US Equity",
    });
    await invokeJson(byName(tools, "xbbg_bdtick"), {
      end: "2024-01-02T10:00:00Z",
      returnEids: true,
      start: "2024-01-02T09:30:00Z",
      ticker: "AAPL US Equity",
    });

    for (const method of [engine.bdp, engine.bds, engine.bdh]) {
      expect(method).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.objectContaining({ returnEids: true }),
      );
    }
    for (const method of [engine.bdib, engine.bdtick]) {
      expect(method).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ returnEids: true }),
      );
    }
  });

  it("retains enumerable result metadata while limiting rows, including zero-row results", async () => {
    const engine = fakeEngine();
    const withMetadata = Object.assign([{ value: 1 }, { value: 2 }], {
      eidData: { "AAPL US Equity": [101, 202] },
      fieldExceptions: [{ fieldId: "BAD_FIELD" }],
      metadata: { source: "ReferenceDataRequest" },
      securityErrors: [{ security: "BAD US Equity" }],
    });
    vi.mocked(engine.bdp).mockResolvedValueOnce(withMetadata);
    const tools = createBloombergTools({ core: fakeCore(engine), maxRows: 1 });

    const [content, artifact] = await invokeArtifact(byName(tools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      returnEids: true,
      securities: ["AAPL US Equity"],
    });
    expect(artifact).toMatchObject({
      data: {
        eidData: { "AAPL US Equity": [101, 202] },
        fieldExceptions: [{ fieldId: "BAD_FIELD" }],
        metadata: { source: "ReferenceDataRequest" },
        rows: [{ value: 1 }],
        securityErrors: [{ security: "BAD US Equity" }],
      },
      rowCount: 2,
      truncated: true,
    });
    expect(JSON.parse(content.split("\n")[1] ?? "{}").data).toMatchObject({
      eidData: { "AAPL US Equity": [101, 202] },
      rows: [{ value: 1 }],
    });

    const emptyWithMetadata = Object.assign([], {
      eidData: { "AAPL US Equity": [303] },
      metadata: { source: "HistoricalDataRequest" },
    });
    vi.mocked(engine.bdh).mockResolvedValueOnce(emptyWithMetadata);
    const empty = await invokeJson(byName(tools, "xbbg_bdh"), {
      end: "20240102",
      fields: ["PX_LAST"],
      returnEids: true,
      securities: ["AAPL US Equity"],
      start: "20240101",
    });
    expect(empty).toMatchObject({
      data: {
        eidData: { "AAPL US Equity": [303] },
        metadata: { source: "HistoricalDataRequest" },
        rows: [],
      },
      rowCount: 0,
    });

    const remainingCases = [
      {
        input: { field: "INDX_MEMBERS", returnEids: true, securities: ["SPX Index"] },
        method: engine.bds,
        name: "xbbg_bds",
      },
      {
        input: {
          end: "2024-01-02T10:00:00Z",
          interval: 5,
          returnEids: true,
          start: "2024-01-02T09:30:00Z",
          ticker: "AAPL US Equity",
        },
        method: engine.bdib,
        name: "xbbg_bdib",
      },
      {
        input: {
          end: "2024-01-02T10:00:00Z",
          returnEids: true,
          start: "2024-01-02T09:30:00Z",
          ticker: "AAPL US Equity",
        },
        method: engine.bdtick,
        name: "xbbg_bdtick",
      },
    ] as const;
    for (const testCase of remainingCases) {
      vi.mocked(testCase.method).mockResolvedValueOnce(
        Object.assign([{ value: 1 }, { value: 2 }], {
          eidData: { "AAPL US Equity": [404, 505] },
        }),
      );
      const artifact = await invokeJson(byName(tools, testCase.name), testCase.input);
      expect(artifact.data).toMatchObject({
        eidData: { "AAPL US Equity": [404, 505] },
        rows: [{ value: 1 }],
      });
    }
  });

  it("validates and invokes the read-only entitlement check with the default service", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });
    const tool = byName(tools, "xbbg_check_entitlements");
    const schema = toolParameterJsonSchema(tool) as Record<string, any>;
    expect(schema.required).toContain("eids");
    expect(schema.properties.eids.minItems).toBe(1);
    expect(schema.properties.eids.maxItems).toBe(10_000);

    const result = await invokeJson(tool, { eids: [101, 202] });
    expect(engine.checkEntitlements).toHaveBeenCalledWith("//blp/refdata", [101, 202]);
    expect(result).toMatchObject({
      data: { entitled: true, failedEids: [] },
      tool: "xbbg_check_entitlements",
    });
    await expect(tool.invoke({ eids: [] })).rejects.toThrow(/at least one/u);
    await expect(tool.invoke({ eids: [1.5] })).rejects.toThrow(/integers/u);
    await expect(tool.invoke({ eids: [0] })).rejects.toThrow(/positive integers/u);
    await expect(tool.invoke({ eids: [2_147_483_648] })).rejects.toThrow(/32-bit/u);
  });

  it("passes security identifiers unchanged and documents Bloomberg identifier syntax", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });

    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("/isin/<ISIN>");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("/cusip/<CUSIP>");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("/isin/<ISIN>@<QUOTE_SOURCE> <MARKET_SECTOR>");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain(
      "format template, not authorization to construct a ticker",
    );
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain(
      "Pass each security in the form the user supplied it",
    );
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain(
      "resolve it with xbbg_resolve_isins first and use the returned Bloomberg security",
    );
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("never a guessed preferred ('Pfd') ticker");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain(
      "Equity, Index, Curncy, Comdty, Corp, Govt, Muni, Mtge, M-Mkt, and Pfd",
    );
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("parse_ticker splits generic futures-style");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("xbbg_bdtick");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("xbbg_bqr");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("get(<FIELD_1>, <FIELD_2>) for(<UNIVERSE>)");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("holdings('<ETF_TICKER> <MARKET_SECTOR>')");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("members('<INDEX_TICKER> <MARKET_SECTOR>')");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("filter_valid_contracts");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("fixed-income YAS recipe fields");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("default_bqr_datetimes");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("## Core request tools");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("## Extension helper tools");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("bounded model-readable JSON");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("rowCount");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("truncated");

    const requiredOnly = getBloombergToolInstructions({
      includeExtensionGuidance: false,
      includeLimitReminder: false,
    });
    expect(requiredOnly).toContain("## Core request tools");
    expect(requiredOnly).not.toContain("## Extension helper tools");
    expect(requiredOnly).not.toContain("## Request limits and inputs");

    await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: ["<FIELD>"],
      securities: [" SYNTHETIC_ID ", " /isin/<ISIN> "],
    });
    expect(engine.bdp).toHaveBeenCalledWith(
      ["SYNTHETIC_ID", "/isin/<ISIN>"],
      ["<FIELD>"],
      expect.objectContaining({ backend: "json" }),
    );
  });

  it("returns bounded model content and structured artifacts", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine), maxStringChars: 20 });
    const bdp = byName(tools, "xbbg_bdp");

    expect((bdp as unknown as { readonly responseFormat?: string }).responseFormat).toBe(
      "content_and_artifact",
    );

    const [content, artifact] = await invokeArtifact(bdp, {
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
    });
    const [summaryLine, payloadLine] = content.split("\n");
    const payload = JSON.parse(payloadLine ?? "") as Record<string, any>;

    expect(summaryLine).toContain("xbbg_bdp: 1 row");
    expect(summaryLine).toContain("truncated=true");
    expect(payload).toMatchObject({
      rowCount: 1,
      tool: "xbbg_bdp",
      truncated: true,
    });
    expect(payload.data[0].value).toBe("12345678901234567890…[truncated 5 chars]");
    expect(content).not.toContain("1234567890123456789012345");
    expect(artifact).toMatchObject({ rowCount: 1, tool: "xbbg_bdp", truncated: true });

    const node = new ToolNode([bdp]);
    const result = (await node.invoke({
      messages: [
        new AIMessage({
          content: "",
          tool_calls: [
            {
              args: { fields: ["PX_LAST"], securities: ["AAPL US Equity"] },
              id: "call_bdp",
              name: "xbbg_bdp",
              type: "tool_call",
            },
          ],
        }),
      ],
    })) as { messages: unknown[] };

    const [message] = result.messages;
    expect(message).toBeInstanceOf(ToolMessage);
    const toolMessage = message as ToolMessage;
    expect(toolMessage.content).toContain("xbbg_bdp: 1 row");
    expect(toolMessage.content).toContain("12345678901234567890…[truncated 5 chars]");
    expect(toolMessage.content).not.toContain("1234567890123456789012345");
    expect(toolMessage.artifact).toMatchObject({
      rowCount: 1,
      tool: "xbbg_bdp",
    });
  });

  it("surfaces row-level Bloomberg errors in model content", async () => {
    const engine = fakeEngine();
    vi.mocked(engine.bdp).mockResolvedValueOnce([
      {
        security: "<ERROR_TICKER> <MARKET_SECTOR>",
        securityError: { message: "synthetic row failure" },
      },
    ]);
    const tools = createBloombergTools({ core: fakeCore(engine) });

    const [content, artifact] = await invokeArtifact(byName(tools, "xbbg_bdp"), {
      fields: ["<FIELD>"],
      securities: ["<ERROR_TICKER> <MARKET_SECTOR>"],
    });

    expect(content.split("\n")[0]).toContain("inspect result payload for Bloomberg error details");
    expect(content).toContain("securityError");
    expect(content).toContain("synthetic row failure");
    expect(artifact).toMatchObject({ rowCount: 1, tool: "xbbg_bdp", truncated: false });
  });

  it("collects bounded snapshot tools and always unsubscribes", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({
      core: fakeCore(engine),
      maxStreamUpdates: 3,
      maxStreamWaitMs: 1_000,
    });

    const streamSub = fakeSubscription([
      { toObject: () => ({ LAST_PRICE: 123n, topic: "AAPL US Equity" }) },
      { toObject: () => ({ LAST_PRICE: 124, topic: "AAPL US Equity" }) },
    ]);
    vi.mocked(engine.stream).mockResolvedValueOnce(streamSub.subscription);

    const stream = await invokeJson(byName(tools, "xbbg_stream_snapshot"), {
      conflate: true,
      drain: true,
      fields: [" LAST_PRICE "],
      maxUpdates: 2,
      tickers: [" AAPL US Equity "],
      timeoutMs: 1_000,
    });

    expect(engine.stream).toHaveBeenCalledWith(
      ["AAPL US Equity"],
      ["LAST_PRICE"],
      expect.objectContaining({ conflate: true }),
    );
    expect(stream.data).toMatchObject({ reason: "max_updates", updateCount: 2 });
    expect(stream.data.updates[0].LAST_PRICE).toBe("123");
    expect(streamSub.unsubscribe).toHaveBeenCalledWith(true);

    const mktbarSub = fakeSubscription([
      { toArray: () => [{ close: 1n, time: new Date(Date.UTC(2024, 0, 2)) }] },
    ]);
    vi.mocked(engine.mktbar).mockResolvedValueOnce(mktbarSub.subscription);

    const mktbar = await invokeJson(byName(tools, "xbbg_mktbar_snapshot"), {
      fields: [" LAST_PRICE "],
      maxUpdates: 1,
      ticker: " AAPL US Equity ",
      timeoutMs: 1_000,
    });

    expect(engine.mktbar).toHaveBeenCalledWith(
      "AAPL US Equity",
      expect.objectContaining({ fields: ["LAST_PRICE"] }),
    );
    expect(mktbar.data.updates[0]).toEqual([{ close: "1", time: "2024-01-02T00:00:00.000Z" }]);
    expect(mktbarSub.unsubscribe).toHaveBeenCalledWith(false);

    const depthSub = fakeSubscription([{ toObject: () => ({ BID: 1, topic: "AAPL US Equity" }) }]);
    vi.mocked(engine.depth).mockResolvedValueOnce(depthSub.subscription);

    const depth = await invokeJson(byName(tools, "xbbg_depth_snapshot"), {
      maxUpdates: 2,
      ticker: " AAPL US Equity ",
      timeoutMs: 1_000,
    });

    expect(engine.depth).toHaveBeenCalledWith("AAPL US Equity", expect.objectContaining({}));
    expect(depth.data).toMatchObject({ reason: "done", updateCount: 1 });
    expect(depthSub.unsubscribe).toHaveBeenCalledWith(false);

    const failingSub = fakeSubscription([], new Error("boom"));
    vi.mocked(engine.depth).mockResolvedValueOnce(failingSub.subscription);

    await expect(
      byName(tools, "xbbg_depth_snapshot").invoke({
        maxUpdates: 1,
        ticker: "AAPL US Equity",
        timeoutMs: 1_000,
      }),
    ).rejects.toThrow(/xbbg_depth_snapshot failed: boom/u);
    expect(failingSub.unsubscribe).toHaveBeenCalledWith(false);
  });

  it("rejects unsafe or ambiguous request inputs", async () => {
    const tools = createBloombergTools({
      core: fakeCore(fakeEngine()),
      maxSecurities: 1,
      maxFields: 1,
      maxStreamUpdates: 1,
    });

    await expect(
      byName(tools, "xbbg_bdp").invoke({ fields: ["PX_LAST"], securities: ["A", "B"] }),
    ).rejects.toThrow(/at most 1/u);
    await expect(
      byName(tools, "xbbg_bdp").invoke({ fields: [""], securities: ["AAPL US Equity"] }),
    ).rejects.toThrow(/non-empty/u);
    await expect(
      byName(tools, "xbbg_bdp").invoke({
        fields: ["PX_LAST"],
        overrides: { "AAPL US Equity": { bad: { nested: true } } },
        securities: ["AAPL US Equity"],
      }),
    ).rejects.toThrow();
    await expect(
      byName(tools, "xbbg_bdh").invoke({
        end: "2024-01-01",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: "01/01/2024",
      }),
    ).rejects.toThrow();
    await expect(
      byName(tools, "xbbg_bdh").invoke({
        end: "2024-01-01",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: "2024-01-02",
      }),
    ).rejects.toThrow();
    await expect(
      byName(tools, "xbbg_bdib").invoke({
        end: "2024-01-02T10:00:00",
        interval: 0,
        start: "2024-01-02T09:30:00",
        ticker: "AAPL US Equity",
      }),
    ).rejects.toThrow(/greater than zero/u);
    await expect(
      byName(tools, "xbbg_bdib").invoke({
        end: "2024-01-02T10:00:00",
        interval: 1,
        start: "2024-01-02",
        ticker: "AAPL US Equity",
      }),
    ).rejects.toThrow(/explicit time component/u);
    await expect(
      byName(tools, "xbbg_bdtick").invoke({
        end: "20240102",
        start: "2024-01-02T09:30:00",
        ticker: "AAPL US Equity",
      }),
    ).rejects.toThrow(/explicit time component/u);
    await expect(
      byName(tools, "xbbg_bqr").invoke({
        end: "2024-06-03T15:00:00",
        start: "2024-06-03",
        ticker: "FAKE US Equity@MSG1",
      }),
    ).rejects.toThrow(/explicit time component/u);
    await expect(
      byName(tools, "xbbg_bflds").invoke({ fields: ["PX_LAST"], searchSpec: "last price" }),
    ).rejects.toThrow(/exactly one/u);
    await expect(
      byName(tools, "xbbg_stream_snapshot").invoke({
        fields: ["PX"],
        maxUpdates: 2,
        tickers: ["A"],
      }),
    ).rejects.toThrow(/at most 1/u);
    await expect(
      byName(tools, "xbbg_stream_snapshot").invoke({
        fields: ["PX"],
        tickers: ["A"],
      }),
    ).rejects.toThrow();
  });

  it("does not mutate original results while truncating output", async () => {
    const original = [{ text: "1234567890" }, { text: "second" }];
    const engine = fakeEngine();
    vi.mocked(engine.bdp).mockResolvedValue(original);
    const tools = createBloombergTools({ core: fakeCore(engine), maxRows: 1, maxStringChars: 4 });

    const output = await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: ["PX"],
      securities: ["A"],
    });

    expect(output).toMatchObject({ rowCount: 2, truncated: true });
    expect(output.data).toHaveLength(1);
    expect(output.data[0].text).toBe("1234…[truncated 6 chars]");
    expect(original).toEqual([{ text: "1234567890" }, { text: "second" }]);
  });

  it("enforces aggregate EID bounds and reports exact retention counts", () => {
    const first = Array.from({ length: 5_000 }, (_, index) => index + 1);
    const second = Array.from({ length: 5_001 }, (_, index) => index + 5_001);
    const exactRows = Object.assign([], {
      eidData: { "AAPL US Equity": first, "MSFT US Equity": second.slice(0, 5_000) },
    });
    expect(limitResult(exactRows, 1, 100)).toMatchObject({
      truncated: false,
      value: { eidData: { "AAPL US Equity": first, "MSFT US Equity": second.slice(0, 5_000) } },
    });

    const overRows = Object.assign([], {
      eidData: { "AAPL US Equity": first, "MSFT US Equity": second },
    });
    const result = limitResult(overRows, 1, 100);
    expect(result).toMatchObject({
      truncated: true,
      value: {
        eidDataTruncation: {
          invalidSecurityCount: 0,
          omittedSecurityCount: 0,
          retainedEidCount: 10_000,
          retainedSecurityCount: 2,
          securityCounts: [
            { originalCount: 5_000, retainedCount: 5_000 },
            { originalCount: 5_001, retainedCount: 5_000 },
          ],
          totalEidCount: 10_001,
          totalSecurityCount: 2,
        },
        rows: [],
      },
    });
    expect((result.value as Record<string, any>).eidData["MSFT US Equity"]).toHaveLength(5_000);
  });

  it("enforces security-count and cumulative UTF-8 security-name budgets", () => {
    const manySecurities = Object.fromEntries(
      Array.from({ length: 1_001 }, (_, index) => [`SEC${String(index)}`, [index + 1]]),
    );
    const exactCount = limitResult(
      Object.assign([], {
        eidData: Object.fromEntries(Object.entries(manySecurities).slice(0, 1_000)),
      }),
      1,
      100,
    );
    expect(exactCount.truncated).toBe(false);

    const countLimited = limitResult(Object.assign([], { eidData: manySecurities }), 1, 100);
    expect(countLimited).toMatchObject({
      truncated: true,
      value: {
        eidDataTruncation: {
          invalidSecurityCount: 0,
          omittedSecurityCount: 1,
          retainedEidCount: 1_000,
          retainedSecurityCount: 1_000,
          totalEidCount: 1_001,
          totalSecurityCount: 1_001,
        },
      },
    });
    expect(Object.keys((countLimited.value as Record<string, any>).eidData)).toHaveLength(1_000);
    expect(
      (countLimited.value as Record<string, any>).eidDataTruncation.securityCounts,
    ).toHaveLength(1_000);

    const exactName = "é".repeat(32_768);
    const exactBytes = limitResult(Object.assign([], { eidData: { [exactName]: [1] } }), 1, 100);
    expect(exactBytes.truncated).toBe(false);
    const byteLimited = limitResult(
      Object.assign([], { eidData: { [exactName]: [1], overflow: [2] } }),
      1,
      100,
    );
    expect(byteLimited).toMatchObject({
      truncated: true,
      value: {
        eidDataTruncation: {
          invalidSecurityCount: 0,
          omittedSecurityCount: 1,
          retainedEidCount: 1,
          retainedSecurityCount: 1,
          securityCounts: [{ originalCount: 1, retainedCount: 1 }],
          totalEidCount: 2,
          totalSecurityCount: 2,
        },
      },
    });
    expect(Object.keys((byteLimited.value as Record<string, any>).eidData)).toEqual([exactName]);
  });

  it("preserves an own __proto__ security key with aligned accounting", () => {
    const eidData = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(eidData, "__proto__", {
      enumerable: true,
      value: [101],
    });
    const result = limitResult(Object.assign([], { eidData }), 1, 100);
    const limited = result.value as Record<string, any>;
    expect(result.truncated).toBe(false);
    expect(Object.hasOwn(limited.eidData, "__proto__")).toBe(true);
    expect(Object.getOwnPropertyDescriptor(limited.eidData, "__proto__")?.value).toEqual([101]);
    const parsed = JSON.parse(JSON.stringify(limited)) as Record<string, any>;
    expect(Object.getOwnPropertyDescriptor(parsed.eidData, "__proto__")?.value).toEqual([101]);
  });

  it("omits malformed EID metadata without granting independent nested budgets", () => {
    const whollySparse: unknown[] = [];
    whollySparse.length = 2;
    const partiallySparse: unknown[] = [1];
    partiallySparse.length = 2;
    const malformed = Object.assign([], {
      eidData: {
        A: { nested: Array.from({ length: 10_001 }, (_, index) => index) },
        B: "not-an-eid-list",
        C: [Array.from({ length: 10_001 }, (_, index) => index + 1)],
        D: [1, "not-an-eid"],
        E: [0],
        F: whollySparse,
        G: partiallySparse,
      },
    });
    expect(limitResult(malformed, 1, 100)).toMatchObject({
      truncated: true,
      value: {
        eidData: {},
        eidDataTruncation: {
          invalidSecurityCount: 7,
          omittedSecurityCount: 0,
          retainedEidCount: 0,
          retainedSecurityCount: 0,
          securityCounts: [],
          totalEidCount: 0,
          totalSecurityCount: 7,
        },
      },
    });
  });

  it("limits cyclic and excessively deep artifacts without recursion failure", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;

    const cyclicResult = limitResult(cyclic, 10, 100);
    expect(cyclicResult).toMatchObject({
      truncated: true,
      value: { self: "[Circular]" },
    });

    let deep: Record<string, unknown> = { leaf: "ok" };
    for (let index = 0; index < 40; index += 1) {
      deep = { child: deep };
    }

    const deepResult = limitResult(deep, 10, 100);
    expect(deepResult.truncated).toBe(true);
    expect(JSON.stringify(deepResult.value)).toContain("Max result depth");
  });
});

describe("Bloomberg tool hardening", () => {
  it("exposes every registered tool name exactly once", () => {
    const tools = createAllBloombergTools({ core: fakeCore(fakeEngine()) });

    expect(tools.map((entry) => entry.name).sort()).toEqual([...BLOOMBERG_TOOL_NAMES].sort());
  });

  it("treats integer YYYYMMDD dates as calendar dates, not epoch milliseconds", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });
    const bdh = byName(tools, "xbbg_bdh");

    await invokeJson(bdh, {
      end: 20240131,
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
      start: 20240101,
    });
    expect(engine.bdh).toHaveBeenCalledWith(
      ["AAPL US Equity"],
      ["PX_LAST"],
      expect.objectContaining({ end: "20240131", start: "20240101" }),
    );

    await invokeJson(bdh, {
      end: Date.UTC(2024, 0, 31),
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
      start: Date.UTC(2024, 0, 1),
    });
    expect(engine.bdh).toHaveBeenLastCalledWith(
      ["AAPL US Equity"],
      ["PX_LAST"],
      expect.objectContaining({ end: "20240131", start: "20240101" }),
    );

    await expect(
      bdh.invoke({
        end: "2024-01-31",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: 99999999,
      }),
    ).rejects.toThrow(/Ambiguous numeric date/u);
    await expect(
      bdh.invoke({
        end: "2024-01-31",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: 20241385,
      }),
    ).rejects.toThrow(/Invalid date/u);
    await expect(
      byName(tools, "xbbg_bdib").invoke({
        end: "2024-01-02T16:00:00",
        interval: 5,
        start: 20240102,
        ticker: "AAPL US Equity",
      }),
    ).rejects.toThrow(/time component/u);
  });

  it("rejects already-aborted tool calls before issuing Bloomberg requests", async () => {
    const engine = fakeEngine();
    const core = fakeCore(engine);
    const tools = createBloombergTools({ core });
    const controller = new AbortController();
    controller.abort();

    await expect(
      byName(tools, "xbbg_bdp").invoke(
        { fields: ["PX_LAST"], securities: ["AAPL US Equity"] },
        { signal: controller.signal },
      ),
    ).rejects.toThrow();

    expect(core.connect).not.toHaveBeenCalled();
    expect(engine.bdp).not.toHaveBeenCalled();
  });

  it("stops snapshot collection and unsubscribes promptly on abort", async () => {
    const engine = fakeEngine();
    let resolveNext!: (value: IteratorResult<unknown>) => void;
    const pending = new Promise<IteratorResult<unknown>>((resolve) => {
      resolveNext = resolve;
    });
    const subscription = {
      next: vi.fn(() => pending),
      unsubscribe: vi.fn(async () => []),
    };
    vi.mocked(engine.stream).mockResolvedValueOnce(subscription as unknown as CoreSubscription);
    const tools = createBloombergTools({ core: fakeCore(engine) });
    const controller = new AbortController();

    const invocation = byName(tools, "xbbg_stream_snapshot").invoke(
      {
        drain: true,
        fields: ["LAST_PRICE"],
        maxUpdates: 5,
        tickers: ["AAPL US Equity"],
        timeoutMs: 10_000,
      },
      { signal: controller.signal },
    );
    await vi.waitFor(() => {
      expect(subscription.next).toHaveBeenCalled();
    });
    controller.abort();

    await expect(invocation).rejects.toThrow();
    // Abort overrides drain so the subscription closes immediately.
    await vi.waitFor(() => {
      expect(subscription.unsubscribe).toHaveBeenCalledWith(false);
    });
    resolveNext({ done: true, value: undefined });
  });

  it("applies a default request timeout to lazily connected engines", async () => {
    const engine = fakeEngine();
    const core = fakeCore(engine);
    const tools = createBloombergTools({ core });

    await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
    });
    const config = vi.mocked(core.connect).mock.calls[0]?.[0];
    expect(config?.requestTimeoutMs).toBeGreaterThan(0);

    const explicitCore = fakeCore(fakeEngine());
    const explicitTools = createBloombergTools({
      core: explicitCore,
      engineConfig: { requestTimeoutMs: 0 },
    });
    await invokeJson(byName(explicitTools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      securities: ["AAPL US Equity"],
    });
    expect(explicitCore.connect).toHaveBeenCalledWith(
      expect.objectContaining({ requestTimeoutMs: 0 }),
    );
  });

  it("does not cross-contaminate tool error prefixes on a shared connect failure", async () => {
    const failure = new Error("connect refused");
    const connect = vi.fn(async () => {
      throw failure;
    });
    const core = { ...fakeCore(fakeEngine()), connect } as unknown as XbbgCoreLike;
    const tools = createBloombergTools({ core });

    const [bdpResult, bdhResult] = await Promise.allSettled([
      byName(tools, "xbbg_bdp").invoke({ fields: ["PX_LAST"], securities: ["AAPL US Equity"] }),
      byName(tools, "xbbg_bdh").invoke({
        end: "2024-01-02",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: "2024-01-01",
      }),
    ]);

    expect(bdpResult.status).toBe("rejected");
    expect(bdhResult.status).toBe("rejected");
    const bdpMessage = ((bdpResult as PromiseRejectedResult).reason as Error).message;
    const bdhMessage = ((bdhResult as PromiseRejectedResult).reason as Error).message;
    expect(bdpMessage).toBe("xbbg_bdp failed: connect refused");
    expect(bdhMessage).toBe("xbbg_bdh failed: connect refused");
    expect(failure.message).toBe("connect refused");
  });

  it("returns collected snapshot data when only unsubscribe fails", async () => {
    const engine = fakeEngine();
    const subscription = {
      next: vi.fn(async () => ({
        done: false,
        value: { toObject: () => ({ LAST_PRICE: 1 }) },
      })),
      unsubscribe: vi.fn(async () => {
        throw new Error("close failed");
      }),
    };
    vi.mocked(engine.stream).mockResolvedValueOnce(subscription as unknown as CoreSubscription);
    const tools = createBloombergTools({ core: fakeCore(engine) });

    const result = await invokeJson(byName(tools, "xbbg_stream_snapshot"), {
      fields: ["LAST_PRICE"],
      maxUpdates: 1,
      tickers: ["AAPL US Equity"],
      timeoutMs: 1_000,
    });

    expect(result.data).toMatchObject({
      reason: "max_updates",
      unsubscribeError: "close failed",
      updateCount: 1,
    });
  });

  it("flags empty results with verification guidance in model content", async () => {
    const engine = fakeEngine();
    vi.mocked(engine.bdp).mockResolvedValueOnce([]);
    const tools = createBloombergTools({ core: fakeCore(engine) });

    const [content] = await invokeArtifact(byName(tools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      securities: ["ZZZZ US Equity"],
    });
    const [summary] = content.split("\n");

    expect(summary).toContain("0 rows");
    expect(summary).toContain("empty result");
    expect(summary).toContain("verify identifiers, fields, and date range");
  });

  it("bounds binary payloads instead of serializing raw bytes", () => {
    const limited = limitResult({ blob: new Uint8Array(2048) }, 10, 100);

    expect(limited.truncated).toBe(true);
    expect((limited.value as Record<string, unknown>).blob).toBe("[binary data: 2048 bytes]");
  });

  it("does not forward format to tools whose engine output rejects it", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });

    // The engine rejects format for BulkData/FieldInfo output; the schema no
    // longer advertises it and a model-sent value is stripped, not forwarded.
    await invokeJson(byName(tools, "xbbg_bds"), {
      field: "TOP_20_HOLDERS_PUBLIC_FILINGS",
      format: "long",
      securities: ["/isin/US0000000000"],
    });
    const bdsOptions = vi.mocked(engine.bds).mock.calls[0]?.[2] as Record<string, unknown>;
    expect("format" in bdsOptions).toBe(false);

    await invokeJson(byName(tools, "xbbg_bflds"), {
      fields: ["PX_LAST"],
      format: "long",
    });
    const bfldsOptions = vi.mocked(engine.bflds).mock.calls[0]?.[0] as Record<string, unknown>;
    expect("format" in bfldsOptions).toBe(false);

    // Reference output still supports format; bdp keeps forwarding it.
    await invokeJson(byName(tools, "xbbg_bdp"), {
      fields: ["PX_LAST"],
      format: "long_typed",
      securities: ["AAPL US Equity"],
    });
    expect(engine.bdp).toHaveBeenCalledWith(
      ["AAPL US Equity"],
      ["PX_LAST"],
      expect.objectContaining({ format: "long_typed" }),
    );
  });

  it("keeps Date instances off the wire contract and out of JSON schemas", async () => {
    const engine = fakeEngine();
    const tools = createBloombergTools({ core: fakeCore(engine) });
    const bdh = byName(tools, "xbbg_bdh");

    await expect(
      bdh.invoke({
        end: "2024-01-31",
        fields: ["PX_LAST"],
        securities: ["AAPL US Equity"],
        start: new Date(Date.UTC(2024, 0, 1)),
      }),
    ).rejects.toThrow();

    const parameters = toolParameterJsonSchema(bdh);
    const serialized = JSON.stringify(parameters);
    expect(serialized).toContain('"start"');
    expect(serialized).not.toContain("date-time");
  });

  it("instructs one call per dataset without parameter probing", () => {
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("Issue one tool call per dataset");
    expect(BLOOMBERG_TOOL_INSTRUCTIONS).toContain("never probe parameter variants in parallel");
  });
});
