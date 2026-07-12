"""Live verification for issue #301 request-element aliases.

These tests intentionally keep Bloomberg usage small:

- Historical aliases are covered by eight BDH requests over a one-year window, each
  capped with ``Points=1``. Each request includes compatible aliases so every key
  and enum-value spelling is exercised without one request per spelling, while
  still returning one raw row for schema/value assertions.
- Presentation aliases are covered by one BDH request; they should be consumed
  locally for output shaping and never sent to Bloomberg.
- Intraday aliases use recent five-minute UTC windows on ES1 Index and
  ``Points=1`` to cap returned bars/ticks to one row.

Run with an active Bloomberg Terminal or B-PIPE connection:

    pytest py-xbbg/tests/live/test_issue_301_aliases.py -q -s
"""

from __future__ import annotations

from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any

import pytest

from xbbg import blp

pytestmark = pytest.mark.live

_HISTORICAL_TICKER = "IBM US Equity"
_HISTORICAL_FIELD = "PX_LAST"
_INTRADAY_TICKER = "ES1 Index"


_HISTORICAL_ALIAS_BATCHES = [
    (
        "PeriodAdj/Period/Currency/Days=N/Fill=C/Quote=A/QuoteType=P/Calendar",
        {
            "PeriodAdj": "A",
            "Period": "D",
            "Currency": "USD",
            "Days": "N",
            "Fill": "C",
            "Points": 1,
            "Quote": "A",
            "QuoteType": "P",
            "CshAdjNormal": True,
            "CshAdjAbnormal": True,
            "CapChg": True,
            "UseDPDF": True,
            "Calendar": "US",
        },
    ),
    (
        "PerAdj/Per/Curr/Days=W/Fill=P/Quote=G/QtTyp=Price",
        {
            "PerAdj": "C",
            "Per": "W",
            "Curr": "USD",
            "Days": "W",
            "Fill": "P",
            "Points": 1,
            "Quote": "G",
            "QtTyp": "Price",
            "CshAdjNormal": True,
            "CshAdjAbnormal": True,
            "CapChg": True,
            "UseDPDF": True,
        },
    ),
    (
        "PeriodAdj=F/Period=Q/FX/Days=Weekdays/Fill=Previous/Quote=Average/QuoteType=Y",
        {
            "PeriodAdj": "F",
            "Period": "Q",
            "FX": "USD",
            "Days": "Weekdays",
            "Fill": "Previous",
            "Points": 1,
            "Quote": "Average",
            "QuoteType": "Y",
            "CshAdjNormal": True,
            "CshAdjAbnormal": True,
            "CapChg": True,
            "UseDPDF": True,
        },
    ),
    (
        "PerAdj=A/Per=M/Days=C/Fill=B/Quote=C/QtTyp=Yield",
        {
            "PerAdj": "A",
            "Per": "M",
            "Currency": "USD",
            "Days": "C",
            "Fill": "B",
            "Points": 1,
            "Quote": "C",
            "QtTyp": "Yield",
        },
    ),
    (
        "PeriodAdj=C/Period=S/Curr/Days=A/Fill=Blank/Quote=Close",
        {
            "PeriodAdj": "C",
            "Period": "S",
            "Curr": "USD",
            "Days": "A",
            "Fill": "Blank",
            "Points": 1,
            "Quote": "Close",
            "QuoteType": "P",
        },
    ),
    (
        "PerAdj=F/Per=Y/FX/Days=All/Fill=NA",
        {
            "PerAdj": "F",
            "Per": "Y",
            "FX": "USD",
            "Days": "All",
            "Fill": "NA",
            "Points": 1,
            "Quote": "A",
            "QtTyp": "Price",
        },
    ),
    (
        "Days=T/QuoteType=Y",
        {
            "PeriodAdj": "A",
            "Period": "D",
            "Currency": "USD",
            "Days": "T",
            "Fill": "C",
            "Points": 1,
            "Quote": "G",
            "QuoteType": "Y",
        },
    ),
    (
        "Days=Trading/QtTyp=Yield",
        {
            "PerAdj": "C",
            "Per": "W",
            "Curr": "USD",
            "Days": "Trading",
            "Fill": "P",
            "Points": 1,
            "Quote": "Average",
            "QtTyp": "Yield",
        },
    ),
]


def _require_usable_backend(backend: str, required_module: str | None) -> None:
    if required_module is None:
        return
    pytest.importorskip(required_module)
    if backend.startswith("polars"):
        from xbbg.backend import check_backend

        try:
            check_backend(backend)
        except ImportError as exc:
            pytest.skip(f"Optional backend {backend!r} is not usable in this environment: {exc}")


@lru_cache(maxsize=1)
def _recent_intraday_window() -> tuple[str, str]:
    """Return a short UTC market-open window with observed ES1 bar data."""
    day = datetime.now() - timedelta(days=1)
    checked_dates: list[str] = []

    while len(checked_dates) < 7:
        if day.weekday() < 5:
            start = day.replace(hour=14, minute=30, second=0, microsecond=0)
            end = start + timedelta(minutes=5)
            start_text = start.isoformat()
            end_text = end.isoformat()
            date_text = day.strftime("%Y-%m-%d")
            checked_dates.append(date_text)

            probe = blp.bdib(
                _INTRADAY_TICKER,
                start_datetime=start_text,
                end_datetime=end_text,
                backend="native",
                typ="TRADE",
                interval=5,
                maxDataPoints=1,
            )
            if _raw_rows(probe):
                return start_text, end_text

        day -= timedelta(days=1)

    pytest.skip("No non-empty ES1 intraday bar window found for recent weekdays: " + ", ".join(checked_dates))
    raise AssertionError("unreachable after pytest.skip")


@lru_cache(maxsize=1)
def _historical_window() -> tuple[str, str]:
    """Return a broad historical window ending on an observed IBM trading day."""
    day = datetime.now() - timedelta(days=1)
    checked_dates: list[str] = []

    while len(checked_dates) < 7:
        if day.weekday() < 5:
            end_text = day.strftime("%Y-%m-%d")
            checked_dates.append(end_text)

            probe = blp.bdh(
                _HISTORICAL_TICKER,
                _HISTORICAL_FIELD,
                start_date=end_text,
                end_date=end_text,
                backend="native",
                maxDataPoints=1,
            )
            rows = _raw_rows(probe)
            if rows and isinstance(rows[0].get("value"), int | float):
                start_day = day - timedelta(days=370)
                return start_day.strftime("%Y-%m-%d"), end_text

        day -= timedelta(days=1)

    pytest.skip("No non-empty IBM historical row found for recent weekdays: " + ", ".join(checked_dates))
    raise AssertionError("unreachable after pytest.skip")


def _materialize_frame(frame: Any) -> Any:
    if hasattr(frame, "collect"):
        return frame.collect()
    return frame


def _row_count(frame: Any) -> int:
    """Return row count for arrow/narwhals/pandas/polars-like live API results."""
    materialized = _materialize_frame(frame)
    if hasattr(materialized, "num_rows"):
        return int(materialized.num_rows)
    return len(materialized)


def _column_names(frame: Any) -> list[str]:
    materialized = _materialize_frame(frame)
    if hasattr(materialized, "column_names"):
        return list(materialized.column_names)
    if hasattr(materialized, "columns"):
        return list(materialized.columns)
    if hasattr(materialized, "schema") and hasattr(materialized.schema, "names"):
        return list(materialized.schema.names)
    raise TypeError(f"Unsupported backend result type: {type(frame)!r}")


def _raw_rows(frame: Any) -> list[dict[str, Any]]:
    materialized = _materialize_frame(frame)
    assert materialized is not None
    if hasattr(materialized, "to_pylist"):
        return materialized.to_pylist()
    if hasattr(materialized, "rows"):
        return materialized.rows(named=True)
    if hasattr(materialized, "to_pandas"):
        return materialized.to_pandas().to_dict("records")
    return materialized.to_dict("records")


def _assert_raw_rows(frame: Any, expected_columns: list[str]) -> list[dict[str, Any]]:
    columns = _column_names(frame)
    missing = [column for column in expected_columns if column not in columns]
    assert not missing, f"missing raw columns {missing}; got {columns}"
    materialized = _materialize_frame(frame)
    if materialized.__class__.__name__ == "ArrowTable":
        assert materialized.num_columns == len(materialized.column_names)
        batches = materialized.to_batches()
        assert batches
        assert all(batch.__class__.__name__ == "ArrowRecordBatch" for batch in batches)
    rows = _raw_rows(frame)
    assert rows, f"expected at least one raw row with columns {columns}"
    assert _row_count(frame) == len(rows)
    return rows


def _assert_bdh_raw(frame: Any) -> None:
    rows = _assert_raw_rows(frame, ["ticker", "date", "field", "value"])
    assert len(rows) == 1, f"Points=1 should cap BDH to one row, got {len(rows)}"
    row = rows[0]
    assert row["ticker"] == _HISTORICAL_TICKER
    assert row["date"] is not None
    assert row["field"] == _HISTORICAL_FIELD
    assert isinstance(row["value"], int | float)


def _assert_bdib_raw(frame: Any) -> None:
    rows = _assert_raw_rows(frame, ["ticker", "time", "open", "high", "low", "close", "volume", "numEvents", "value"])
    assert len(rows) == 1, f"Points=1 should cap BDIB to one row, got {len(rows)}"
    row = rows[0]
    assert row["ticker"] == _INTRADAY_TICKER
    assert row["time"] is not None
    for column in ("open", "high", "low", "close", "volume", "value"):
        assert isinstance(row[column], int | float), f"{column}={row[column]!r}"
    assert row["low"] <= row["high"]
    assert row["numEvents"] is None or row["numEvents"] >= 0


def _assert_bdtick_raw(frame: Any) -> None:
    rows = _assert_raw_rows(frame, ["ticker", "time", "type", "value", "size", "exchangeCode"])
    assert len(rows) == 1, f"Points=1 should cap BDTICK to one row, got {len(rows)}"
    row = rows[0]
    assert row["ticker"] == _INTRADAY_TICKER
    assert row["time"] is not None
    assert row["type"] == "TRADE"
    assert isinstance(row["value"], int | float)
    assert row["size"] is None or row["size"] >= 0


@pytest.mark.parametrize(
    ("case_name", "kwargs"),
    _HISTORICAL_ALIAS_BATCHES,
    ids=[case_name for case_name, _kwargs in _HISTORICAL_ALIAS_BATCHES],
)
def test_bdh_live_accepts_historical_request_aliases(case_name: str, kwargs: dict[str, Any]) -> None:
    """BDH accepts every historical key alias and value alias from issue #301."""
    start_date, end_date = _historical_window()

    frame = blp.bdh(
        _HISTORICAL_TICKER,
        _HISTORICAL_FIELD,
        start_date=start_date,
        end_date=end_date,
        backend="native",
        **kwargs,
    )

    assert case_name
    _assert_bdh_raw(frame)


def test_bdh_live_applies_excel_presentation_aliases_locally() -> None:
    """Excel-only aliases are consumed locally and still return sane raw BDH data."""
    start_date, end_date = _historical_window()

    frame = blp.bdh(
        _HISTORICAL_TICKER,
        _HISTORICAL_FIELD,
        start_date=start_date,
        end_date=end_date,
        backend="native",
        Points=1,
        Dts="Show",
        Dates="S",
        show_date=True,
        DtFmt="D",
        DateFormat="Date",
        date_format="D",
        Sort="A",
        sort="Ascend",
        Orientation="V",
        Direction="Vertical",
        Dir="V",
        orientation="Vertical",
    )

    _assert_bdh_raw(frame)


@pytest.mark.parametrize(
    ("backend", "required_module"),
    [
        ("native", None),
        ("pandas", "pandas"),
        ("polars", "polars"),
        ("polars_lazy", "polars"),
    ],
)
def test_bdh_live_presentation_aliases_shape_representative_backends(backend: str, required_module: str | None) -> None:
    """A capped live smoke matrix proves presentation shaping survives backend conversion."""
    _require_usable_backend(backend, required_module)

    start_date, end_date = _historical_window()
    frame = blp.bdh(
        _HISTORICAL_TICKER,
        _HISTORICAL_FIELD,
        start_date=start_date,
        end_date=end_date,
        backend=backend,
        Points=1,
        Period="M",
        DtFmt="Both",
        Sort="A",
        Direction="V",
    )

    assert _column_names(frame) == ["ticker", "date", "period", "field", "value"]
    assert _row_count(frame) == 1
    rows = _raw_rows(frame)
    assert len(rows) == 1
    row = rows[0]
    assert row["ticker"] == _HISTORICAL_TICKER
    assert row["date"] is not None
    assert row["period"]
    assert row["field"] == _HISTORICAL_FIELD
    assert isinstance(row["value"], int | float)


@pytest.mark.parametrize(
    ("bar_size_alias", "bar_type_alias", "event_value"),
    [
        ("BarSz", "BarTp", "B"),
        ("BarSize", "BarType", "Bid"),
        ("BarSz", "BarTp", "A"),
        ("BarSize", "BarType", "Ask"),
        ("BarSz", "BarTp", "T"),
        ("BarSize", "BarType", "Trade"),
    ],
)
def test_bdib_live_accepts_bar_aliases_and_event_value_aliases(
    bar_size_alias: str,
    bar_type_alias: str,
    event_value: str,
) -> None:
    """BDIB accepts BarSz/BarSize, BarTp/BarType, and all eventType aliases."""
    start, end = _recent_intraday_window()
    alias_kwargs: dict[str, Any] = {bar_size_alias: 5, bar_type_alias: event_value}

    frame = blp.bdib(
        _INTRADAY_TICKER,
        start_datetime=start,
        end_datetime=end,
        backend="native",
        Points=1,
        **alias_kwargs,
    )

    _assert_bdib_raw(frame)


@pytest.mark.parametrize(("bar_type_alias", "event_value"), [("BarTp", "T"), ("BarType", "Trade")])
def test_bdtick_live_accepts_bar_type_aliases_and_exchange_codes(
    bar_type_alias: str,
    event_value: str,
) -> None:
    """BDTICK accepts BarTp/BarType and IncludeExchangeCodes with a one-tick cap."""
    start, end = _recent_intraday_window()
    alias_kwargs: dict[str, Any] = {bar_type_alias: event_value}

    frame = blp.bdtick(
        _INTRADAY_TICKER,
        start_datetime=start,
        end_datetime=end,
        backend="native",
        IncludeExchangeCodes=True,
        Points=1,
        **alias_kwargs,
    )

    _assert_bdtick_raw(frame)
