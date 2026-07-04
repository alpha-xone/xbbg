# @xbbg/core

Independent Node.js bindings for Bloomberg-connected environments, backed by the Rust `xbbg` engine and a native N-API addon.

## Status

Supported Node.js 24+ bindings for Bloomberg-connected server environments. `@xbbg/core` ships TypeScript definitions, prebuilt native addons for supported platforms, and the same Rust request engine used by the Python package; API changes follow normal semver release notes.

## Install

Install the wrapper package with your package manager:

```bash
npm install @xbbg/core
# or
bun add @xbbg/core
```

`@xbbg/core` targets Node.js 24+ server runtimes. It is not a browser package and still requires a local Bloomberg runtime at execution time.

`@xbbg/core` uses optional dependencies to load a packaged native `napi_xbbg.node` addon for supported targets:

- `@xbbg/core-darwin-arm64` — macOS arm64
- `@xbbg/core-linux-x64` — Linux x64
- `@xbbg/core-win32-x64` — Windows x64

If no packaged addon is available for your platform, build from source locally instead.

## Runtime prerequisites

Using the API requires Bloomberg Terminal, B-PIPE, or ZFP access and Bloomberg SDK runtime libraries available on the target system. Configure Bloomberg connectivity and credentials according to your Bloomberg deployment before making requests.

On Windows, `@xbbg/core` prepends the first detected Bloomberg runtime DLL directory before loading the native addon. Explicit `BLPAPI_LIB_DIR` / `BLPAPI_ROOT` values win; otherwise it probes local SDK roots plus standard Terminal DAPI installs including `C:\blp\DAPI` and `C:\Program Files (x86)\Bloomberg\Blp\DAPI`.

`@xbbg/core` is not affiliated with, endorsed by, sponsored by, or approved by Bloomberg Finance L.P. or its affiliates. It does not provide Bloomberg access, credentials, entitlements, data rights, or SDK licenses; users must supply and use those under their own Bloomberg agreements and policies.

## Release integrity

Release packages are published from GitHub Actions using npm trusted publishing with provenance, backed by GitHub OIDC at publish time. No npm token or local `.npmrc` credential is required in this repository.

## Local Development

```bash
# Preferred: build and copy js-xbbg/napi_xbbg.node into the package directory
npm --prefix js-xbbg run build

# Lower-level Rust build (useful while hacking on napi-xbbg itself)
cargo build -p napi-xbbg

# Stage the current platform package template with the built addon
npm --prefix js-xbbg run stage:native-package

# Run package smoke tests from js-xbbg/
npm run test:smoke
```

The JS package automatically loads a local `js-xbbg/napi_xbbg.node` addon first, then falls back to packaged optional native dependencies for supported platforms.

`bdp()` / `bds()` / `bdh()` forward `validateFields` for per-request field validation. `bdib()` and `bdtick()` forward `requestTz` / `outputTz`; `bdtick()` also exposes common include-code request flags such as `includeConditionCodes` and `includeExchangeCodes` as typed options while still accepting raw Bloomberg request kwargs.

### Date and datetime input (#317)

Every API surface that takes a date or datetime accepts a wide input set:

- `Date` (JavaScript)
- ISO 8601 `string` (`"2023-01-17"`, `"2023-01-17T10:30:00"`, `"2023-01-17T10:30:00-05:00"`)
- Bloomberg-native `string` (`"20230117"`)
- Epoch milliseconds `number`
- Duck-typed Luxon `DateTime` — anything implementing `toJSDate(): Date`

Ambiguous formats like `"01/17/2023"` are rejected with a clear `TypeError`. Naive ISO datetime strings without a tz suffix are passed through to the Rust engine so `requestTz` semantics still apply; tz-aware strings are preserved end-to-end. The helpers `formatDate` and `formatDateTime` plus the `DateLike` / `DateTimeLike` types are re-exported from `@xbbg/core` if you want to apply them yourself.

```ts
import { bdh, bdtick } from '@xbbg/core';

await bdh('AAPL US Equity', 'PX_LAST', { start: new Date('2024-01-01'), end: '20240630' });
await bdtick('AAPL US Equity', new Date('2024-12-01T09:30Z'), Date.UTC(2024, 11, 1, 16, 0));
```

Subscriptions use a NAPI Arrow zero-copy transfer path for supported primitive/string/time/timestamp columns, constructing Apache Arrow JS tables directly from native Arrow buffers instead of serializing every update through Arrow IPC. Unsupported or sliced Arrow subscription schemas now fail fast with a column-level diagnostic so schema gaps are visible instead of silently switching transport paths.
Pass `{ allFields: true }` to `stream()` / `subscribe()` / service stream helpers to expose every top-level scalar field Bloomberg sends, matching Python's `all_fields=True`. The default remains filtered mode: requested fields plus `MKTDATA_EVENT_TYPE` and `MKTDATA_EVENT_SUBTYPE`.

### Subscription replay benchmark

`npm run bench:subscription-replay` is a TypeScript benchmark script for one-update-at-a-time subscription processing. It does not change the production streaming API and does not batch updates by default. Use `--path legacy` for the original encode+decode measurement, `--path arrow-decode-only` to exclude benchmark-only IPC encoding from timed results, and `--path subscription-wrapper` to time the current JS `Subscription.next()` wrapper with fake native zero-copy descriptors. `--consume rows|vector|schema|none` controls how much decoded output is touched; `rows` remains the default for continuity with prior results. Use `--warmup-iterations N` for untimed replay warmup. Live capture exercises the default native subscription path and prints `sub.stats` telemetry; unsupported-schema diagnostics are surfaced when the native stream returns a schema the zero-copy bridge cannot describe.

```bash
# Synthetic one-update replay, no Bloomberg connection needed; row materialization is the default
npm run bench:subscription-replay -- --rows 100000 --iterations 3

# Time JS Arrow decode only, with IPC buffers precomputed outside the timed loop
npm run bench:subscription-replay -- --path arrow-decode-only --rows 100000 --iterations 3

# Time the current JS Subscription wrapper around fake native zero-copy descriptors
npm run build:ts
npm run bench:subscription-replay -- --path subscription-wrapper --rows 100000 --iterations 3

# Capture real XBTUSD ticks to JSONL, printing existing sub.stats telemetry
npm run bench:subscription-replay -- --capture-live "XBTUSD Curncy" --capture-ms 10000 --out tmp/xbtusd-ticks.jsonl

# Replay captured ticks one update at a time with schema-only consumption after one warmup iteration
npm run bench:subscription-replay -- --fixture tmp/xbtusd-ticks.jsonl --iterations 10 --warmup-iterations 1 --consume schema
```

## Usage

```typescript
import * as xbbg from '@xbbg/core';
import { bdp, ovr } from '@xbbg/core';

xbbg.configure({
  host: 'localhost',
  port: 8194,
});

// Direct B-PIPE / leased-line hosts with ordered failover
const bpipeEngine = await xbbg.connect({
  servers: [
    { host: 'bpipe-primary.example.com', port: 8194 },
    { host: 'bpipe-secondary.example.com', port: 8196 },
  ],
  auth: { method: 'userapp', appName: 'my-bpipe-app' },
  tls: {
    clientCredentials: '/secure/client.p12',
    clientCredentialsPassword: process.env.BPIPE_TLS_PASSWORD,
    trustMaterial: '/secure/trust.p7',
  },
});

// Authorized ZFP over leased lines: use Bloomberg-provisioned endpoints via zfpRemote
const zfpEngine = await xbbg.connect({
  zfpRemote: '8194',
  tls: {
    clientCredentials: '/secure/client.p12',
    clientCredentialsPassword: process.env.BPIPE_TLS_PASSWORD,
    trustMaterial: '/secure/trust.p7',
  },
});

// Python-style blp namespace
const hist = await xbbg.blp.abdh(['AAPL US Equity'], ['PX_LAST'], '2024-01-01', '2024-12-31');
const ref = await xbbg.blp.abdp(['AAPL US Equity'], ['PX_LAST', 'SECURITY_NAME']);
const bulk = await xbbg.blp.abds(['ES1 Index'], ['FUT_CHAIN_LAST_TRADE_DATES']);

// Composable override helper
await bdp(['AAPL US Equity'], ['CRNCY_ADJ_PX_LAST'], {
  overrides: ovr({ EQY_FUND_CRNCY: 'EUR' }),
});
await bdp(['AAPL US Equity', 'MSFT US Equity'], ['CRNCY_ADJ_PX_LAST'], {
  overrides: ovr({
    EQY_FUND_CRNCY: 'USD',
    'AAPL US Equity': ovr({ EQY_FUND_CRNCY: 'EUR' }),
    'MSFT US Equity': ovr({ EQY_FUND_CRNCY: 'JPY' }),
  }),
});

const bars = await xbbg.blp.abdib('AAPL US Equity', '2024-12-01', 5);
const ticks = await xbbg.blp.abdtick(
  'AAPL US Equity',
  '2024-12-01T09:30:00',
  '2024-12-01T10:00:00',
);

// Live streaming
const sub = await xbbg.blp.asubscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK']);
for await (const tick of sub) {
  console.log(tick);
}

// Conflated market data: quote updates are conflated by Bloomberg; trades still stream as received.
const conflated = await xbbg.blp.asubscribe(
  ['ES1 Index'],
  ['BID', 'ASK', 'LAST_PRICE', 'MKTDATA_EVENT_TYPE', 'MKTDATA_EVENT_SUBTYPE'],
  { conflate: true },
);
for await (const tick of conflated) {
  console.log(tick);
}

// CDX analytics
const cdxInfo = await xbbg.ext.cdx.acdx_info('CDX IG CDSI GEN 5Y Corp');
const cdxPricing = await xbbg.ext.cdx.acdx_pricing('CDX IG CDSI GEN 5Y Corp');
const cdxRisk = await xbbg.ext.cdx.acdx_risk('CDX IG CDSI GEN 5Y Corp');
```

## Recipes

High-level workflows that wrap common Bloomberg request patterns. Each recipe returns an Arrow `Table` by default (or a JSON/Polars result when `backend` is set) and errors are mapped to the standard `BlpError` hierarchy.

```javascript
import * as xbbg from '@xbbg/core';

const engine = await xbbg.connect({ host: 'localhost', port: 8194 });

// Fixed income
const yas = await engine.yas(['US912810TM69 Govt'], ['YAS_BOND_YLD'], {
  settleDt: '20240115',
  yieldType: 1, // 1=YTM, 2=YTC, 3=YTW, 4=YTB, 5=YTP, 6=YTN, 7=OAS, 8=YTS, 9=YTAL
  price: 99.5,
});
const bqr = await engine.bqr('US912810TM69 Govt', {
  startDatetime: '2024-06-03T14:30:00',
  endDatetime: '2024-06-03T15:00:00',
  eventTypes: ['BID', 'ASK'],
});
const preferreds = await engine.preferreds('BAC US Equity');
const corpBonds = await engine.corporateBonds('AAPL', { ccy: 'USD' });

// Futures and CDX resolution
const front = await engine.futTicker('ES1 Index', '20240301');
const active = await engine.activeFutures('CL1 Comdty', '20240301', { freq: 'M' });
const curve = await engine.futuresCurve('ES1 Index', { maxContracts: 6 });
const cdx = await engine.cdxTicker('CDX IG CDSI GEN 5Y Corp', '20240301');
const activeCdx = await engine.activeCdx('CDX IG CDSI GEN 5Y Corp', '20240301', {
  lookbackDays: 10,
});

// Historical helpers
const dvd = await engine.dividend(['AAPL US Equity'], '20230101', '20231231');
const turn = await engine.turnover(['AAPL US Equity'], '20240101', '20240131', {
  ccy: 'USD',
});
const holdings = await engine.etfHoldings('SPY US Equity');
const realizedYield = await engine.dividendYield('AAPL US Equity', '20230101', '20231231', {
  dividendTypes: ['Regular Cash'],
});
const members = await engine.indexMembers('SPX Index', { field: 'INDX_MWEIGHT', asof: '20240102' });
const surface = await engine.volSurface('SPX Index', '20240102', '20240105', {
  preset: 'MONEYNESS_30D',
  includeDerived: true,
  riskFreeRate: 0.05,
});
const resolved = await engine.resolveIsins(['US0378331005', 'INVALIDISIN000']);
const issuers = await engine.issuerIsins(['US037833FB15', 'INVALIDISIN000']);

// Currency-converted prices
const px = await engine.currencyConversion('700 HK Equity', 'USD', '20240101', '20240131');
```

## Engine configuration

`connect()` and `configure()` accept a structured `EngineConfig` object. The most important connection controls are:

- `host` / `port` for a local Terminal session or an already-provisioned direct endpoint
- `servers` for ordered failover across already-provisioned direct hosts
- `auth` for Bloomberg session identity auth: `user`, `app`, `userapp`, `dir`, `manual`, or `token`
- `tls` for encrypted B-PIPE/direct sessions and as a required input for ZFP
- `zfpRemote` (`'8194'` or `'8196'`) for Bloomberg-assigned ZFP endpoints in an entitled environment; do not combine it with `host`/`port`/`servers`/`socks5`
- `socks5` for proxied access to already-provisioned direct Bloomberg endpoints
- `retryPolicy`, `numStartAttempts`, and recovery settings for reconnect behavior
- `shardRequests`, `shardThreshold`, `shardChunkSize`, and `shardMaxConcurrent` for opt-in sharding of wide multi-security `bdp`/`bdh` requests

The JS binding forwards these fields directly to the Rust engine, so Node can configure the same auth and transport features already available in the core runtime. Invalid transport combinations such as `zfpRemote` plus direct hosts fail during configuration instead of silently connecting to `localhost:8194`.

## Features

- Native N-API bindings (no HTTP overhead)
- Zero-copy Arrow buffers via `apache-arrow`
- Async/await with proper backpressure
- TypeScript-first with full type definitions
- Cross-platform prebuilt addon packaging: macOS arm64, Linux x64, Windows x64
