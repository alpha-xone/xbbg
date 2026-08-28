# xbbg-mcp

Stdio MCP server for Bloomberg request/response workflows backed by `xbbg-async`.

This binary is intended for coding agents such as Claude Code and OpenCode that can launch a local MCP server process and call tools over stdio.

## What it exposes

The current server exposes request/response tools only:

- `bdp` - reference data
- `bdh` - historical data
- `bds` - bulk data
- `bdib` - intraday bars
- `bql` - Bloomberg Query Language
- `bsrch` - Bloomberg search
- `bflds` - field metadata lookup
- `check_entitlements` - entitlement-ID check for a Bloomberg service
- `request` - generic raw/custom request path

Responses are returned as bounded structured JSON with Arrow schema metadata so an agent can inspect the shape without receiving an unbounded payload.

## Entitlement IDs

Set `return_eids: true` only on routes backed by Bloomberg operations that support `returnEids`:

| MCP route | Bloomberg operation | EID option |
| --- | --- | --- |
| `bdp`, `bds` | `ReferenceDataRequest` (including BDS/bulk) | `return_eids: true` |
| `bdh` | `HistoricalDataRequest` | `return_eids: true` |
| `bdib` | `IntradayBarRequest` | `return_eids: true` |
| `request` | `IntradayTickRequest` | `return_eids: true` |

The dedicated `bdp`, `bds`, `bdh`, and `bdib` tools advertise `return_eids` directly. Use the generic `request` tool for intraday ticks:

```json
{
  "service": "//blp/refdata",
  "operation": "IntradayTickRequest",
  "extractor": "intraday_tick",
  "security": "AAPL US Equity",
  "start_datetime": "2024-01-15T09:30:00",
  "end_datetime": "2024-01-15T10:00:00",
  "event_types": ["TRADE"],
  "return_eids": true
}
```

Bounded results expose the EID map as structured JSON at `metadata["xbbg.eid_data"]`, alongside `schema`, `row_count`, `returned_rows`, `truncated`, and `rows`. Pass the collected IDs to the read-only `check_entitlements` tool; `service` defaults to `//blp/refdata`:

```json
{ "eids": [101, 202], "service": "//blp/refdata" }
```

The check result includes `service`, `eids`, `entitled`, and `failed_eids`.

## Install from GitHub Releases

For macOS arm64 and Linux amd64, install the latest wrapper + binary pair with:

```bash
curl -fsSL https://raw.githubusercontent.com/xbbg-org/xbbg/main/scripts/install-xbbg-mcp.sh | sh
```

To install a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/xbbg-org/xbbg/main/scripts/install-xbbg-mcp.sh | sh -s -- 1.0.0
```

The installer places two files in `~/.local/bin/` by default:

- `xbbg-mcp` - launcher wrapper
- `xbbg-mcp-real` - compiled binary

GitHub release assets include only the launcher wrapper and compiled xbbg binary. They do **not** include Bloomberg SDK files or the Bloomberg runtime. You must provide those locally from a source you are authorized to use under your Bloomberg agreements and entitlements.

GitHub Release tar/zip assets remain the raw binary channel. The MCPB asset is the directory and one-click local connector channel for MCPB-aware clients and registries.

The wrapper locates the Bloomberg runtime in this order:

1. `XBBG_MCP_LIB_DIR`
2. `BLPAPI_LIB_DIR`
3. `BLPAPI_ROOT`
4. locally staged authorized SDK under `vendor/blpapi-sdk/`
5. the official Python `blpapi` package

If you install Bloomberg's Python package, the wrapper can usually run without any extra shell configuration:

```bash
pip install blpapi --index-url https://blpapi.bloomberg.com/repository/releases/python/simple/
```

Windows release assets are attached as `.zip` files, but the convenience installer currently targets macOS/Linux only.

## Install MCPB

Claude Desktop and MCPB-aware registries can use the `xbbg-mcp-v<VERSION>.mcpb` asset attached to GitHub Releases. The MCPB currently supports macOS arm64, Linux amd64, and Windows amd64. The raw platform tar/zip assets remain available for manual installs and troubleshooting.

The MCPB does not include Bloomberg SDK files, Bloomberg runtime libraries, credentials, or market data. Configure `XBBG_MCP_LIB_DIR`, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`, or install Bloomberg's official Python `blpapi` package so the launcher can find the authorized Bloomberg runtime locally. On Windows, the MCPB PowerShell launcher prepends the resolved Bloomberg runtime directory to `PATH` before starting `xbbg-mcp.exe`.

## Build from source

```bash
bash ./scripts/sdktool.sh
cargo build --release -p xbbg-mcp --locked
./scripts/xbbg-mcp
```

## Claude Code

```bash
claude mcp add --transport stdio xbbg -- ~/.local/bin/xbbg-mcp
```

## OpenCode

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "xbbg": {
      "type": "local",
      "command": ["/Users/you/.local/bin/xbbg-mcp"],
      "enabled": true
    }
  }
}
```

## Directory and registry publishing

Official MCP Registry: publish the generated `server.json` after the release contains the `.mcpb` asset.

GitHub MCP Registry: verify the server appears after official registry ingestion.

Claude Connectors Directory: submit the MCPB as a desktop extension; remote connector submission is not the default for Bloomberg-local workflows.

Smithery: publish the MCPB bundle.

Glama: verify indexing after official registry publication; if automated sandbox build cannot run without Bloomberg runtime, keep the listing metadata-focused.

PulseMCP: wait for official registry ingestion or use the Pulse submit page.

MCP.so: submit the GitHub repo/release after canonical registry metadata is live.

## Runtime environment

`xbbg-mcp` accepts the same engine-oriented connection settings as the Rust core, with MCP-prefixed names taking precedence where available.

Common settings:

- `XBBG_MCP_HOST` / `XBBG_HOST`
- `XBBG_MCP_PORT` / `XBBG_PORT`
- `XBBG_MCP_AUTH_METHOD` / `XBBG_AUTH_METHOD`
- `XBBG_MCP_APP_NAME`
- `XBBG_MCP_DIR_PROPERTY`
- `XBBG_MCP_USER_ID`
- `XBBG_MCP_IP_ADDRESS`
- `XBBG_MCP_TOKEN`
- `XBBG_MCP_REQUEST_POOL_SIZE`
- `XBBG_MCP_MAX_ROWS`
- `XBBG_MCP_MAX_STRING_CHARS`
- `XBBG_MCP_VALIDATION_MODE` / `XBBG_VALIDATION_MODE` – `disabled` (default), `lenient`, `strict`
- `XBBG_MCP_SDK_LOG_LEVEL` / `XBBG_SDK_LOG_LEVEL` – `off` (default), `fatal`, `error`, `warn`, `info`, `debug`, `trace`
- `XBBG_MCP_OVERFLOW_POLICY` / `XBBG_OVERFLOW_POLICY` – `drop_newest` (default), `block`

Supported auth methods:

- `none`
- `user`
- `app`
- `userapp`
- `dir`
- `directory`
- `manual`
- `token`

## Privacy Policy

`xbbg-mcp` runs locally as a stdio MCP server. It does not send telemetry or usage data to xbbg-org, GitHub, Anthropic, Smithery, Glama, or any other third party.

Tool calls are handled on the user's machine and forwarded only to the Bloomberg runtime/API endpoint configured by the user (`XBBG_MCP_HOST` / `XBBG_HOST`, default `localhost:8194`) under that user's Bloomberg agreements and entitlements. Results are returned only to the MCP client process that launched the server.

Bloomberg validates entitlements at request time, including per-security and per-field access. A valid local session can still return empty or partial results when the user's DAPI, SAPI/B-PIPE, or ZFP entitlement set does not cover the requested data.

The server reads configuration from environment variables and does not persist request data, credentials, or Bloomberg responses to disk.

## Smoke test

After building locally, verify the stdio handshake and a few live requests with:

```bash
uv run python -X utf8 scripts/xbbg_mcp_smoke.py
```

That script expects a live Bloomberg connection and a locally built `target/debug/xbbg-mcp` binary, or `target/release/xbbg-mcp` if no debug build is present.
