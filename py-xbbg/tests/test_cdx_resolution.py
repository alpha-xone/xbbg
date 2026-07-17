from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pytest

import xbbg
from xbbg.ext.futures import aactive_cdx, acdx_ticker

_MISSING = object()


def _reference_rows(ticker: str, values: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": [ticker] * len(values),
            "field": list(values),
            "value": list(values.values()),
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "version"),
    [("IG", 1), ("IG", 2), ("HY", 2)],
)
async def test_cdx_ticker_is_explicit_by_default_and_optionally_versionless(
    monkeypatch: pytest.MonkeyPatch,
    family: str,
    version: int,
) -> None:
    generic = f"CDX {family} CDSI GEN 5Y Corp"

    async def fake_abdp(*, tickers: str, flds: Sequence[str], **_kwargs):
        assert tickers == generic
        assert "VERSION" in flds
        return _reference_rows(
            generic,
            {
                "ROLLING_SERIES": 46,
                "VERSION": version,
                "ON_THE_RUN_CURRENT_BD_INDICATOR": True,
                "CDS_FIRST_ACCRUAL_START_DATE": "2026-03-20",
            },
        )

    monkeypatch.setattr(xbbg, "abdp", fake_abdp)

    canonical = await acdx_ticker(generic, "20260717")
    versionless = await acdx_ticker(generic, "20260717", versionless=True)

    assert canonical == f"CDX {family} CDSI S46 V{version} 5Y Corp"
    assert versionless == f"CDX {family} CDSI S46 5Y Corp"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version",
    [
        _MISSING,
        None,
        "not-a-number",
        0,
        -1,
        1.5,
        "1.5",
        2**32,
        "4294967296.0",
        float("nan"),
        float("inf"),
    ],
    ids=[
        "missing",
        "null",
        "non-numeric",
        "zero",
        "negative",
        "fractional-number",
        "fractional-text",
        "overflow-number",
        "overflow-text",
        "nan",
        "infinity",
    ],
)
async def test_cdx_ticker_rejects_unresolved_version(
    monkeypatch: pytest.MonkeyPatch,
    version: object,
) -> None:
    generic = "CDX HY CDSI GEN 5Y Corp"

    async def fake_abdp(*, tickers: str, flds: Sequence[str], **_kwargs):
        assert tickers == generic
        assert "VERSION" in flds
        values: dict[str, object] = {
            "ROLLING_SERIES": 46,
            "ON_THE_RUN_CURRENT_BD_INDICATOR": True,
            "CDS_FIRST_ACCRUAL_START_DATE": "2026-03-20",
        }
        if version is not _MISSING:
            values["VERSION"] = version
        return _reference_rows(generic, values)

    monkeypatch.setattr(xbbg, "abdp", fake_abdp)

    assert await acdx_ticker(generic, "20260717") == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "series",
    [0, -1, 45.5, 2**32, float("nan")],
    ids=["zero", "negative", "fractional", "overflow", "nan"],
)
async def test_cdx_ticker_rejects_invalid_series(
    monkeypatch: pytest.MonkeyPatch,
    series: object,
) -> None:
    generic = "CDX HY CDSI GEN 5Y Corp"

    async def fake_abdp(*, tickers: str, flds: Sequence[str], **_kwargs):
        assert tickers == generic
        return _reference_rows(
            generic,
            {
                "ROLLING_SERIES": series,
                "VERSION": 2,
                "ON_THE_RUN_CURRENT_BD_INDICATOR": True,
                "CDS_FIRST_ACCRUAL_START_DATE": "2026-03-20",
            },
        )

    monkeypatch.setattr(xbbg, "abdp", fake_abdp)

    assert await acdx_ticker(generic, "20260717") == ""


@pytest.mark.asyncio
async def test_cdx_ticker_resolves_pre_accrual_series_version_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generic = "CDX HY CDSI GEN 5Y Corp"
    previous_alias = "CDX HY CDSI S45 5Y Corp"
    previous = "CDX HY CDSI S45 V1 5Y Corp"
    requested_tickers: list[str] = []

    async def fake_abdp(*, tickers: str, flds: Sequence[str], **_kwargs):
        requested_tickers.append(tickers)
        if tickers == generic:
            assert "VERSION" in flds
            return _reference_rows(
                generic,
                {
                    "ROLLING_SERIES": 46,
                    "ON_THE_RUN_CURRENT_BD_INDICATOR": True,
                    "CDS_FIRST_ACCRUAL_START_DATE": "2026-09-20",
                },
            )
        if tickers == previous_alias:
            assert list(flds) == ["VERSION"]
            return _reference_rows(previous_alias, {"VERSION": 1})
        raise AssertionError(f"unexpected BDP ticker: {tickers}")

    monkeypatch.setattr(xbbg, "abdp", fake_abdp)

    assert await acdx_ticker(generic, "20260717") == previous
    assert await acdx_ticker(generic, "20260717", versionless=True) == previous_alias
    assert requested_tickers == [generic, previous_alias, generic, previous_alias]


@pytest.mark.asyncio
async def test_active_cdx_resolves_each_series_version_before_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generic = "CDX HY CDSI GEN 5Y Corp"
    current = "CDX HY CDSI S46 V2 5Y Corp"
    previous_alias = "CDX HY CDSI S45 5Y Corp"
    previous = "CDX HY CDSI S45 V1 5Y Corp"
    historical_tickers: list[tuple[str, ...]] = []
    reference_tickers: list[str] = []

    async def fake_abdp(*, tickers: str, flds: Sequence[str], **_kwargs):
        reference_tickers.append(tickers)
        if tickers == generic:
            return _reference_rows(
                generic,
                {
                    "ROLLING_SERIES": 46,
                    "VERSION": 2,
                    "ON_THE_RUN_CURRENT_BD_INDICATOR": True,
                    "CDS_FIRST_ACCRUAL_START_DATE": "2026-03-20",
                },
            )
        if tickers == previous_alias:
            assert list(flds) == ["VERSION"]
            return _reference_rows(previous_alias, {"VERSION": 1})
        raise AssertionError(f"unexpected BDP ticker: {tickers}")

    async def fake_abdh(
        *,
        tickers: Sequence[str],
        flds: Sequence[str],
        start_date,
        end_date,
        **_kwargs,
    ):
        del start_date, end_date
        assert list(flds) == ["PX_LAST"]
        historical_tickers.append(tuple(tickers))
        return pd.DataFrame(
            {
                "ticker": [current, previous],
                "date": ["2026-07-16", "2026-07-15"],
                "field": ["PX_LAST", "PX_LAST"],
                "value": [107.75, 106.25],
            }
        )

    monkeypatch.setattr(xbbg, "abdp", fake_abdp)
    monkeypatch.setattr(xbbg, "abdh", fake_abdh)

    assert await aactive_cdx(generic, "20260717") == current
    assert await aactive_cdx(generic, "20260717", versionless=True) == "CDX HY CDSI S46 5Y Corp"
    assert historical_tickers == [(current, previous), (current, previous)]
    assert reference_tickers == [generic, previous_alias, generic, previous_alias]
