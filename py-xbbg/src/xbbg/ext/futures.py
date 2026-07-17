"""Futures and CDX resolver extension functions.

Functions for resolving generic futures/CDX tickers to specific contracts.
Uses high-performance Rust utilities from xbbg._core for parsing and resolution.

Sync functions (wrap async with asyncio.run):
    - fut_ticker(): Resolve generic futures ticker to specific contract
    - active_futures(): Get most active futures contract for a date
    - futures_curve(): Build futures chain table with metadata and carry
    - cdx_ticker(): Resolve generic CDX ticker to specific series
    - active_cdx(): Get most active CDX contract for a date

Async functions (primary implementation):
    - afut_ticker(): Async resolve generic futures ticker
    - aactive_futures(): Async get most active futures contract
    - afutures_curve(): Async futures chain table
    - acdx_ticker(): Async resolve generic CDX ticker
    - aactive_cdx(): Async get most active CDX contract
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import logging
import re

import narwhals.stable.v1 as nw

# Import Rust date parser (shared with other ext modules)
from xbbg._core import ext_get_futures_months, ext_parse_date
from xbbg.ext._utils import (
    DateLike,
    _call_native_recipe,
    _canonical_column_name,
    _fmt_date,
    _normalize_to_datetime,
    _syncify,
)

logger = logging.getLogger(__name__)


def _parse_date(dt: DateLike) -> datetime:
    """Parse a date-like value (str / date / datetime / pd.Timestamp) to datetime."""
    if isinstance(dt, str):
        year, month, day = ext_parse_date(dt)
        return datetime(year, month, day)
    if dt is None:
        raise ValueError("Cannot parse date: None")
    return _normalize_to_datetime(dt)


_FLD_ROLLING_SERIES = "ROLLING_SERIES"
_FLD_OTR_INDICATOR = "ON_THE_RUN_CURRENT_BD_INDICATOR"
_FLD_ACCRUAL_START = "CDS_FIRST_ACCRUAL_START_DATE"
_FLD_VERSION = "VERSION"
_CDX_MAX_U32 = 2**32 - 1

_FUTURES_MONTH_CODES = "".join(ext_get_futures_months().values())

_CDX_FIELDS = [_FLD_ROLLING_SERIES, _FLD_OTR_INDICATOR, _FLD_ACCRUAL_START, _FLD_VERSION]


def _parse_generic_ticker(gen_ticker: str) -> tuple[str, int, str]:
    """Parse generic futures ticker into ``(root, n, asset_type)``."""
    parts = gen_ticker.split()
    if len(parts) < 2:
        raise ValueError(f"Unknown asset type for generic ticker: {gen_ticker}")

    asset = parts[-1]

    if asset in ["Index", "Curncy", "Comdty"]:
        ticker = " ".join(parts[:-1])
        root = ticker[:-1]
        n = int(ticker[-1])
        return root, n, asset

    if asset == "Equity":
        ticker = parts[0]
        root = ticker[:-1]
        n = int(ticker[-1])
        return root, n, " ".join(parts[1:])

    raise ValueError(f"Unknown asset type for generic ticker: {gen_ticker}")


def _find_col(columns: list[str], candidates: list[str]) -> str | None:
    """Find a column by raw label or wrapper-internal canonical alias."""
    by_exact_lower = {col.casefold(): col for col in columns}
    by_canonical = {_canonical_column_name(col): col for col in columns}

    for candidate in candidates:
        match = by_exact_lower.get(candidate.casefold())
        if match is not None:
            return match

        match = by_canonical.get(_canonical_column_name(candidate))
        if match is not None:
            return match

    return None


def _coerce_datetime(value) -> datetime | None:
    """Convert Bloomberg date-like values to ``datetime``."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    text = str(value).strip()
    if not text:
        return None

    candidates = [text]
    if "T" in text:
        candidates.append(text.split("T", 1)[0])
    if " " in text:
        candidates.append(text.split(" ", 1)[0])

    for candidate in candidates:
        try:
            return _parse_date(candidate)
        except (ValueError, TypeError):
            continue

    return None


async def _resolve_chain(gen_ticker: str, dt: datetime, **kwargs) -> list[tuple[str, datetime]]:
    """Resolve futures chain via ``FUT_CHAIN_LAST_TRADE_DATES`` at ``CHAIN_DATE``."""
    from xbbg import abds

    chain_date = dt.strftime("%Y%m%d")
    overrides = {"CHAIN_DATE": chain_date}

    try:
        chain = await abds(
            tickers=gen_ticker,
            flds="FUT_CHAIN_LAST_TRADE_DATES",
            overrides=overrides,
            **kwargs,
        )
    except (ValueError, TypeError, KeyError):
        logger.warning("Failed to get futures chain for %s", gen_ticker)
        return []

    nw_chain = nw.from_native(chain)
    if len(nw_chain) == 0:
        logger.warning("Empty futures chain for %s at %s", gen_ticker, chain_date)
        return []

    ticker_col = _find_col(
        list(nw_chain.columns),
        ["future's_ticker", "futures_ticker", "security_description", "ticker"],
    )
    date_col = _find_col(
        list(nw_chain.columns),
        ["last_trade_date", "last_tradeable_dt", "date"],
    )

    if ticker_col is None or date_col is None:
        logger.warning("Unexpected FUT_CHAIN_LAST_TRADE_DATES columns: %s", list(nw_chain.columns))
        return []

    contracts: list[tuple[str, datetime]] = []
    for row in nw_chain.iter_rows(named=True):
        ticker = row.get(ticker_col)
        expiry_raw = row.get(date_col)
        expiry = _coerce_datetime(expiry_raw)
        if ticker is None or expiry is None:
            continue
        if expiry > dt:
            contracts.append((str(ticker).strip(), expiry))

    contracts.sort(key=lambda item: item[1])
    return contracts


def _extract_field_value(nw_df, field_name: str):
    """Extract a scalar value from a SEMI_LONG frame by field name."""
    field_upper = field_name.upper()

    # LONG / SEMI_LONG format: ticker, field, value
    if "field" in nw_df.columns and "value" in nw_df.columns:
        rows = nw_df.filter(nw.col("field").str.to_uppercase() == field_upper).select("value")
        if len(rows) == 0:
            return None
        return rows.item(0, 0)

    # Wide fallback
    if field_name in nw_df.columns and len(nw_df) > 0:
        return nw_df[field_name][0]

    lower_name = field_name.lower()
    if lower_name in nw_df.columns and len(nw_df) > 0:
        return nw_df[lower_name][0]

    return None


def _parse_positive_cdx_number(value: object) -> int | None:
    """Parse Bloomberg CDX numeric metadata without truncation."""
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None

    if not parsed.is_finite() or parsed != parsed.to_integral_value() or not 1 <= parsed <= _CDX_MAX_U32:
        return None
    return int(parsed)


def _parse_series_token(tok: str) -> int | None:
    """Parse a positive ``S{n}`` token and return its series number."""
    if not tok.startswith("S"):
        return None
    digits = tok[1:]
    if not digits.isdigit():
        return None
    return _parse_positive_cdx_number(digits)


def _find_series_token_index(tokens: list[str]) -> int | None:
    """Find series token index (``S{n}``) in tokenized CDX ticker."""
    for idx, token in enumerate(tokens):
        if _parse_series_token(token) is not None:
            return idx
    return None


def _append_version_to_ticker(ticker: str, version: int) -> str:
    """Insert ``V{version}`` token after series token."""
    tokens = ticker.split()
    series_idx = _find_series_token_index(tokens)
    if series_idx is None:
        return ticker

    if series_idx + 1 < len(tokens) and tokens[series_idx + 1].startswith("V") and tokens[series_idx + 1][1:].isdigit():
        tokens.pop(series_idx + 1)

    tokens.insert(series_idx + 1, f"V{version}")
    return " ".join(tokens)


def _strip_version_from_ticker(ticker: str) -> str:
    """Remove ``V{n}`` token from resolved CDX ticker if present."""
    tokens = ticker.split()
    series_idx = _find_series_token_index(tokens)
    if series_idx is None:
        return ticker

    if series_idx + 1 < len(tokens) and tokens[series_idx + 1].startswith("V") and tokens[series_idx + 1][1:].isdigit():
        tokens.pop(series_idx + 1)
    return " ".join(tokens)


def _present_cdx_ticker(ticker: str, *, versionless: bool) -> str:
    """Apply the requested presentation to an explicitly versioned CDX ticker."""
    return _strip_version_from_ticker(ticker) if versionless else ticker


async def _resolve_version_for_ticker(ticker: str, **kwargs) -> str:
    """Resolve a series ticker to an explicit, positive ``V{n}`` identity."""
    from xbbg import abdp

    if _find_series_token_index(ticker.split()) is None:
        return ""

    try:
        meta = await abdp(tickers=ticker, flds=[_FLD_VERSION], **kwargs)
    except (ValueError, TypeError, KeyError):
        return ""

    nw_meta = nw.from_native(meta)
    if len(nw_meta) == 0:
        return ""

    version = _parse_positive_cdx_number(_extract_field_value(nw_meta, _FLD_VERSION))
    if version is None:
        return ""
    return _append_version_to_ticker(ticker, version)


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def afut_ticker(
    gen_ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async resolve generic futures ticker to specific contract.

    Maps a generic futures ticker (e.g., 'ES1 Index') to the specific
    contract for a given date using Bloomberg's futures chain bulk field
    (``FUT_CHAIN_LAST_TRADE_DATES``) with ``CHAIN_DATE``.

    Args:
        gen_ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
        dt: Reference date for contract resolution.
        **kwargs: Additional arguments passed to abds.

    Returns:
        Specific contract ticker (e.g., 'ESH24 Index').

    Example::

        import asyncio
        from xbbg.ext.futures import afut_ticker


        async def main():
            # Get March 2024 E-mini S&P contract
            ticker = await afut_ticker("ES1 Index", "2024-01-15")
            # Returns: 'ESH24 Index'


        asyncio.run(main())
    """
    dt_parsed = _parse_date(dt)

    try:
        _root, n, _asset_type = _parse_generic_ticker(gen_ticker)
    except ValueError as exc:
        logger.error(str(exc))
        return ""

    contracts = await _resolve_chain(gen_ticker, dt_parsed, **kwargs)

    if len(contracts) < n:
        logger.warning(
            "Not enough contracts expiring after %s for %s (need %d, found %d)",
            dt_parsed.date(),
            gen_ticker,
            n,
            len(contracts),
        )
        return ""

    result = contracts[n - 1][0]
    logger.debug("Resolved %s @ %s -> %s", gen_ticker, dt_parsed.date(), result)
    return result


async def aactive_futures(
    ticker: str,
    dt: DateLike,
    **kwargs,
) -> str:
    """Async get the most active futures contract for a date.

    Selects the most active contract based on volume, typically choosing
    between the front month and second month contract.

    Args:
        ticker: Generic futures ticker (e.g., 'ES1 Index', 'CL1 Comdty').
            Must be a generic contract (e.g., 'ES1'), not specific (e.g., 'ESH24').
        dt: Reference date.
        **kwargs: Additional arguments passed to abdp/abdh.

    Returns:
        Most active contract ticker based on recent volume.

    Raises:
        ValueError: If ticker appears to be a specific contract instead of generic.

    Example::

        import asyncio
        from xbbg.ext.futures import aactive_futures


        async def main():
            # Get most active E-mini S&P contract
            ticker = await aactive_futures("ES1 Index", "2024-01-15")


        asyncio.run(main())
    """
    from xbbg import abdh

    dt_parsed = _parse_date(dt)

    # Reject specific contracts (e.g., UXZ24 Index)
    ticker_base = ticker.rsplit(" ", 1)[0]
    month_code_pattern = rf"[{re.escape(_FUTURES_MONTH_CODES)}]"
    match = re.search(rf"(.+)({month_code_pattern})(\d{{1,2}})$", ticker_base)
    if match:
        _prefix, _month_char, digits = match.groups()
        if len(digits) == 2:
            msg = (
                f"'{ticker}' appears to be a specific contract "
                f"(ends with month code + 2-digit year), not a generic one. "
                f"Use a generic ticker like 'UX1 Index' instead of 'UXZ24 Index'."
            )
            raise ValueError(msg)
        if len(digits) == 1 and len(ticker_base) > 3:
            msg = (
                f"'{ticker}' appears to be a specific contract, "
                f"not a generic one. Use a generic ticker like "
                f"'UX1 Index' instead of 'UXZ5 Index'."
            )
            raise ValueError(msg)

    # Parse ticker components
    t_info = ticker.split()
    prefix, asset = " ".join(t_info[:-1]), t_info[-1]

    gen_1 = f"{prefix[:-1]}1 {asset}"
    contracts = await _resolve_chain(gen_1, dt_parsed, **kwargs)

    if not contracts:
        logger.error("Failed to resolve chain for %s", gen_1)
        return ""

    fut_1, fut_1_expiry = contracts[0]

    if len(contracts) < 2:
        return fut_1

    fut_2 = contracts[1][0]

    # If date is well before first expiry, keep front month
    if dt_parsed.month < fut_1_expiry.month and dt_parsed.year == fut_1_expiry.year:
        return fut_1

    # Compare latest volume over recent window
    start_date = dt_parsed - timedelta(days=15)
    volume = await abdh(
        tickers=[fut_1, fut_2],
        flds="volume",
        start_date=start_date,
        end_date=dt_parsed,
        **kwargs,
    )
    nw_vol = nw.from_native(volume)

    if len(nw_vol) == 0:
        return fut_1

    latest_volumes: dict[str, float] = {}

    # LONG format
    if "field" in nw_vol.columns and "value" in nw_vol.columns:
        vol_rows = nw_vol.filter(nw.col("field").str.to_lowercase() == "volume")
        if "date" in vol_rows.columns:
            vol_rows = vol_rows.sort("date", descending=True)

        for tk in [fut_1, fut_2]:
            tk_rows = vol_rows.filter(nw.col("ticker") == tk)
            if len(tk_rows) > 0:
                with contextlib.suppress(ValueError, TypeError):
                    latest_volumes[tk] = float(tk_rows["value"][0])

    # Wide fallback
    else:
        vol_col = "volume" if "volume" in nw_vol.columns else "VOLUME" if "VOLUME" in nw_vol.columns else None
        if vol_col is not None and "date" in nw_vol.columns:
            for tk in [fut_1, fut_2]:
                tk_rows = nw_vol.filter(nw.col("ticker") == tk).sort("date", descending=True)
                if len(tk_rows) > 0:
                    with contextlib.suppress(ValueError, TypeError):
                        latest_volumes[tk] = float(tk_rows[vol_col][0])

    if not latest_volumes:
        return fut_1

    return max(latest_volumes, key=lambda key: latest_volumes.get(key, 0.0))


async def acdx_ticker(
    gen_ticker: str,
    dt: DateLike,
    versionless: bool = False,
    **kwargs,
) -> str:
    """Async resolve generic CDX ticker to specific series.

    Methodology matches the release/0.x resolver logic:
    - Fetch ``ROLLING_SERIES``, ``VERSION``, ``ON_THE_RUN_CURRENT_BD_INDICATOR``,
      and ``CDS_FIRST_ACCRUAL_START_DATE``.
    - Resolve ``GEN`` to ``S{series}`` and always append a positive ``V{n}``.
    - If the requested date is before accrual start, independently resolve the
      prior series version.

    Args:
        gen_ticker: Generic CDX ticker (e.g., 'CDX IG CDSI GEN 5Y Corp').
        dt: Reference date.
        versionless: Strip the version token from the selected final result.
        **kwargs: Additional arguments passed to abdp.

    Returns:
        Specific series ticker (e.g., ``CDX IG CDSI S45 V1 5Y Corp`` or
        ``CDX HY CDSI S44 V2 5Y Corp``).

    Example::

        import asyncio
        from xbbg.ext.futures import acdx_ticker


        async def main():
            ticker = await acdx_ticker("CDX IG CDSI GEN 5Y Corp", "2024-01-15")


        asyncio.run(main())
    """
    from xbbg import abdp

    dt_parsed = _parse_date(dt)

    try:
        info = await abdp(tickers=gen_ticker, flds=_CDX_FIELDS, **kwargs)
    except (ValueError, TypeError, KeyError):
        logger.warning("Failed to get CDX info")
        return ""

    nw_info = nw.from_native(info)

    if len(nw_info) == 0:
        return ""

    ticker_data = nw_info
    if "ticker" in nw_info.columns:
        ticker_data = nw_info.filter(nw.col("ticker") == gen_ticker)
        if len(ticker_data) == 0:
            ticker_data = nw_info

    otr = _extract_field_value(ticker_data, _FLD_OTR_INDICATOR)
    if otr is not None and str(otr).upper() not in ("Y", "TRUE"):
        logger.warning(
            "Generic ticker %s has ON_THE_RUN_CURRENT_BD_INDICATOR=%r (expected 'Y' or 'true')",
            gen_ticker,
            otr,
        )

    series = _parse_positive_cdx_number(_extract_field_value(ticker_data, _FLD_ROLLING_SERIES))
    if series is None:
        return ""

    start_dt = None
    start_dt_raw = _extract_field_value(ticker_data, _FLD_ACCRUAL_START)
    if start_dt_raw is not None:
        try:
            start_dt = _parse_date(start_dt_raw)
        except (ValueError, TypeError):
            start_dt = None

    tokens = gen_ticker.split()
    if "GEN" not in tokens:
        logger.warning("Generic ticker %s does not contain GEN token", gen_ticker)
        return ""

    gen_idx = tokens.index("GEN")
    tokens[gen_idx] = f"S{series}"
    current_alias = " ".join(tokens)

    # If request date is before current-series accrual start, use previous series
    if start_dt is not None and dt_parsed < start_dt and series > 1:
        prev_tokens = current_alias.split()
        series_idx = _find_series_token_index(prev_tokens)
        if series_idx is None:
            return ""
        prev_tokens[series_idx] = f"S{series - 1}"
        resolved = await _resolve_version_for_ticker(" ".join(prev_tokens), **kwargs)
        if not resolved:
            return ""
    else:
        version = _parse_positive_cdx_number(_extract_field_value(ticker_data, _FLD_VERSION))
        if version is None:
            return ""
        resolved = _append_version_to_ticker(current_alias, version)

    return _present_cdx_ticker(resolved, versionless=versionless)


async def aactive_cdx(
    gen_ticker: str,
    dt: DateLike,
    lookback_days: int = 10,
    versionless: bool = False,
    **kwargs,
) -> str:
    """Async get the most active CDX contract for a date.

    Resolves current and prior series to explicit positive versions before any
    metadata or history request. ``versionless`` strips the version token only
    from the selected final result.
    """
    from xbbg import abdh

    cur = await acdx_ticker(gen_ticker=gen_ticker, dt=dt, versionless=False, **kwargs)
    if not cur:
        return ""

    dt_parsed = _parse_date(dt)

    prev = ""
    prev_base = _strip_version_from_ticker(cur)
    parts = prev_base.split()
    idx = _find_series_token_index(parts)
    if idx is not None:
        series = _parse_series_token(parts[idx])
        if series is not None and series > 1:
            parts[idx] = f"S{series - 1}"
            prev = " ".join(parts)

    if not prev:
        return _present_cdx_ticker(cur, versionless=versionless)

    prev = await _resolve_version_for_ticker(prev, **kwargs)
    if not prev:
        return _present_cdx_ticker(cur, versionless=versionless)

    # Compare activity using latest non-null PX_LAST date
    start = dt_parsed - timedelta(days=lookback_days)
    end = dt_parsed

    try:
        px = await abdh(tickers=[cur, prev], flds=["PX_LAST"], start_date=start, end_date=end, **kwargs)
        nw_px = nw.from_native(px)

        if len(nw_px) == 0:
            return _present_cdx_ticker(cur, versionless=versionless)

        latest_dates: dict[str, str] = {}

        # LONG format: ticker/date/field/value
        if "field" in nw_px.columns and "value" in nw_px.columns:
            px_rows = nw_px.filter(nw.col("field").str.to_uppercase() == "PX_LAST")
            px_rows = px_rows.filter(~nw.col("value").is_null())

            if len(px_rows) == 0 or "date" not in px_rows.columns:
                return _present_cdx_ticker(cur, versionless=versionless)

            for ticker in [cur, prev]:
                tk_rows = px_rows.filter(nw.col("ticker") == ticker).sort("date", descending=True)
                if len(tk_rows) > 0:
                    latest_dates[ticker] = str(tk_rows["date"][0])

        # Wide format: ticker/date/PX_LAST
        else:
            px_col = None
            if "PX_LAST" in nw_px.columns:
                px_col = "PX_LAST"
            elif "px_last" in nw_px.columns:
                px_col = "px_last"

            if px_col is None or "date" not in nw_px.columns:
                return _present_cdx_ticker(cur, versionless=versionless)

            px_rows = nw_px.filter(~nw.col(px_col).is_null())
            for ticker in [cur, prev]:
                tk_rows = px_rows.filter(nw.col("ticker") == ticker).sort("date", descending=True)
                if len(tk_rows) > 0:
                    latest_dates[ticker] = str(tk_rows["date"][0])

        best_ticker = cur
        best_date = latest_dates.get(cur, "")
        if prev in latest_dates and latest_dates[prev] > best_date:
            best_ticker = prev
        return _present_cdx_ticker(best_ticker, versionless=versionless)

    except (ValueError, TypeError, KeyError):
        logger.debug("Failed to compare CDX activity")
    return _present_cdx_ticker(cur, versionless=versionless)


async def afutures_curve(
    gen_ticker: str,
    *,
    asof: DateLike = None,
    chain_field: str | None = None,
    fields: list[str] | None = None,
    max_contracts: int | None = None,
    backend=None,
    **_kwargs,
):
    """Async futures chain table with contract metadata, mid, and annualized carry."""
    asof_fmt = _fmt_date(asof) if asof is not None else None
    return await _call_native_recipe(
        "recipe_futures_curve",
        gen_ticker,
        asof_fmt,
        chain_field,
        list(fields) if fields is not None else None,
        max_contracts,
        backend=backend,
    )


fut_ticker = _syncify(afut_ticker)
active_futures = _syncify(aactive_futures)
cdx_ticker = _syncify(acdx_ticker)
active_cdx = _syncify(aactive_cdx)
futures_curve = _syncify(afutures_curve)
