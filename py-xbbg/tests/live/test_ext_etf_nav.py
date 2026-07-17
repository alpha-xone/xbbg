"""Live Bloomberg tests for ETF NAV / iNAV extension workflows.

Contracts under test (require Bloomberg Desktop/DAPI on localhost:8194):
    - ``QQQ US Equity`` resolves to ``QQQNV Index`` / ``QXV Index``.
    - ``AT1 LN Equity`` resolves to a null daily NAV and ``AT1IN Index``;
      snapshot/history use ``FUND_NET_ASSET_VAL`` on the source fund instead
      of inventing an ``AT1NV Index`` suffix ticker.
    - The iNAV subscription opens on the resolved ``QXV Index`` topic and is
      always unsubscribed.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import pandas as pd
import pytest

QQQ = "QQQ US Equity"
AT1 = "AT1 LN Equity"
QQQ_NAV = "QQQNV Index"
QQQ_INAV = "QXV Index"
AT1_INAV = "AT1IN Index"

RELATIONSHIP_COLUMNS = [
    "input_order",
    "etf_ticker",
    "nav_ticker",
    "nav_market_sector_des",
    "nav_name",
    "nav_validation_error",
    "inav_ticker",
    "inav_market_sector_des",
    "inav_name",
    "inav_validation_error",
]
SNAPSHOT_COLUMNS = [
    "input_order",
    "etf_ticker",
    "nav_ticker",
    "nav_value",
    "nav_source_ticker",
    "nav_source_field",
    "inav_ticker",
    "inav_value",
]
HISTORY_COLUMNS = [
    "input_order",
    "etf_ticker",
    "date",
    "nav_ticker",
    "nav_value",
    "nav_source_ticker",
    "nav_source_field",
    "inav_ticker",
    "inav_value",
]


def _recent_window() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=14)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _pdf(frame: object) -> pd.DataFrame:
    """Narrow a backend='pandas' result for type checkers via a runtime check."""
    assert isinstance(frame, pd.DataFrame)
    return frame


def test_etf_nav_relationships_live_qqq_at1():
    from xbbg.ext import etf_nav_relationships

    pdf = _pdf(etf_nav_relationships([QQQ, AT1], backend="pandas"))

    assert list(pdf.columns) == RELATIONSHIP_COLUMNS
    assert pdf["input_order"].tolist() == [0, 1]
    assert pdf["etf_ticker"].tolist() == [QQQ, AT1]

    qqq = pdf.iloc[0]
    assert qqq["nav_ticker"] == QQQ_NAV
    assert qqq["inav_ticker"] == QQQ_INAV
    assert qqq["nav_market_sector_des"].strip().lower() == "index"
    assert qqq["inav_market_sector_des"].strip().lower() == "index"
    assert isinstance(qqq["nav_name"], str) and qqq["nav_name"].strip()
    assert isinstance(qqq["inav_name"], str) and qqq["inav_name"].strip()
    assert pd.isna(qqq["nav_validation_error"])
    assert pd.isna(qqq["inav_validation_error"])

    at1 = pdf.iloc[1]
    assert pd.isna(at1["nav_ticker"]), "AT1 has no daily NAV Index relationship"
    assert pd.isna(at1["nav_market_sector_des"])
    assert pd.isna(at1["nav_name"])
    assert pd.isna(at1["nav_validation_error"])
    assert at1["inav_ticker"] == AT1_INAV
    assert at1["inav_market_sector_des"].strip().lower() == "index"
    assert isinstance(at1["inav_name"], str) and at1["inav_name"].strip()
    assert pd.isna(at1["inav_validation_error"])


def test_etf_nav_snapshot_live_source_fields():
    from xbbg.ext import etf_nav_snapshot

    pdf = _pdf(etf_nav_snapshot([QQQ, AT1], backend="pandas"))

    assert list(pdf.columns) == SNAPSHOT_COLUMNS
    assert pdf["etf_ticker"].tolist() == [QQQ, AT1]

    qqq = pdf.iloc[0]
    assert qqq["nav_ticker"] == QQQ_NAV
    assert qqq["nav_source_ticker"] == QQQ_NAV
    assert qqq["nav_source_field"] == "PX_LAST"
    assert qqq["inav_ticker"] == QQQ_INAV
    assert float(qqq["nav_value"]) > 0
    assert float(qqq["inav_value"]) > 0

    at1 = pdf.iloc[1]
    assert pd.isna(at1["nav_ticker"])
    assert at1["nav_source_ticker"] == AT1, "fallback prices the source fund itself"
    assert at1["nav_source_field"] == "FUND_NET_ASSET_VAL"
    assert at1["inav_ticker"] == AT1_INAV
    assert float(at1["nav_value"]) > 0
    assert float(at1["inav_value"]) > 0


def test_etf_nav_history_live_daily_union_with_fund_nav_fallback():
    from xbbg.ext import etf_nav_history

    start, end = _recent_window()
    pdf = _pdf(etf_nav_history([QQQ, AT1], start_date=start, end_date=end, backend="pandas"))

    assert list(pdf.columns) == HISTORY_COLUMNS
    assert not pdf.empty

    qqq = pdf[pdf["etf_ticker"] == QQQ]
    assert not qqq.empty
    assert (qqq["nav_ticker"] == QQQ_NAV).all()
    assert (qqq["nav_source_ticker"] == QQQ_NAV).all()
    assert (qqq["nav_source_field"] == "PX_LAST").all()
    assert (qqq["inav_ticker"] == QQQ_INAV).all()
    assert qqq["nav_value"].notna().any()
    dates = qqq["date"].tolist()
    assert dates == sorted(dates)
    assert len(set(dates)) == len(dates), "one row per observed date"

    at1 = pdf[pdf["etf_ticker"] == AT1]
    assert not at1.empty
    assert at1["nav_ticker"].isna().all(), "AT1 never invents a NAV Index ticker"
    assert (at1["nav_source_ticker"] == AT1).all()
    assert (at1["nav_source_field"] == "FUND_NET_ASSET_VAL").all()
    assert (at1["inav_ticker"] == AT1_INAV).all()
    assert at1["nav_value"].notna().any(), "fund NAV history must contribute points"


@pytest.mark.asyncio
async def test_subscribe_etf_inav_live_opens_on_qxv():
    from xbbg.ext import asubscribe_etf_inav

    subscription = await asubscribe_etf_inav(QQQ)
    try:
        assert subscription.tickers == [QQQ_INAV], "stream topics are resolved iNAV tickers"

        saw_qxv_last_price = False
        deadline = asyncio.get_event_loop().time() + 60.0
        while not saw_qxv_last_price:
            remaining = deadline - asyncio.get_event_loop().time()
            assert remaining > 0, "no QXV Index LAST_PRICE update within 60s"
            table = await asyncio.wait_for(subscription.__anext__(), timeout=remaining)
            for row in table.to_pylist():
                if row.get("topic") == QQQ_INAV and row.get("LAST_PRICE") is not None:
                    assert float(row["LAST_PRICE"]) > 0
                    saw_qxv_last_price = True
                    break
    finally:
        await subscription.unsubscribe()
