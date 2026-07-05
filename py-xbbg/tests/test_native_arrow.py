"""Focused tests for xbbg native Arrow carrier objects."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from xbbg._core import ArrowColumn, ArrowRecordBatch, ArrowTable


@pytest.fixture
def arrow_table() -> Any:
    return ArrowTable.from_pylist(
        [
            {"ticker": "AAPL US Equity", "date": date(2024, 1, 1), "px_last": 150.0, "volume": 10},
            {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20},
            {"ticker": "IBM US Equity", "date": date(2024, 1, 3), "px_last": None, "volume": 30},
        ]
    )


def test_from_pylist_preserves_first_seen_columns_and_backfills_late_nulls() -> None:
    table = ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "PX_LAST": 123.0},
            {"ticker": "MSFT US Equity", "VOLUME": 10},
            {"ticker": "AAPL US Equity", "NAME": "Apple", "PX_LAST": 150.0},
        ]
    )

    assert table.column_names == ["ticker", "PX_LAST", "VOLUME", "NAME"]
    assert table.to_pylist() == [
        {"ticker": "IBM US Equity", "PX_LAST": 123.0, "VOLUME": None, "NAME": None},
        {"ticker": "MSFT US Equity", "PX_LAST": None, "VOLUME": 10, "NAME": None},
        {"ticker": "AAPL US Equity", "PX_LAST": 150.0, "VOLUME": None, "NAME": "Apple"},
    ]


def test_table_carrier_properties_and_columns_are_arrow_backed(arrow_table: Any) -> None:
    assert len(arrow_table) == 3
    assert arrow_table.shape == (3, 4)
    assert arrow_table.num_rows == 3
    assert arrow_table.num_columns == 4
    assert arrow_table.chunk_lengths == [3]
    assert arrow_table.nbytes > 0

    assert arrow_table.column_names == ["ticker", "date", "px_last", "volume"]
    assert [column.name for column in arrow_table.columns] == arrow_table.column_names

    ticker = arrow_table.column("ticker")
    assert isinstance(ticker, ArrowColumn)
    assert ticker == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert arrow_table.get_column(0).to_pylist() == ticker.to_pylist()
    assert arrow_table["ticker"].to_pylist() == ticker.to_pylist()
    assert arrow_table[-1].name == "volume"


def test_column_api_materializes_lazily_and_supports_slices(arrow_table: Any) -> None:
    prices = arrow_table.column("px_last")

    assert prices.name == "px_last"
    assert prices.field.name == "px_last"
    assert "Float64" in prices.data_type
    assert prices.null_count == 1
    assert prices.nbytes > 0
    assert len(prices) == 3
    assert prices[0] == 150.0
    assert prices[-1] is None
    assert list(prices) == [150.0, 380.0, None]
    assert prices.slice(1, 1).to_pylist() == [380.0]
    assert prices.slice(99).to_pylist() == []


def test_table_projection_drop_rename_slice_head_tail(arrow_table: Any) -> None:
    selected = arrow_table.select(["ticker", "px_last"])
    assert selected.column_names == ["ticker", "px_last"]
    assert selected.to_pylist() == [
        {"ticker": "AAPL US Equity", "px_last": 150.0},
        {"ticker": "MSFT US Equity", "px_last": 380.0},
        {"ticker": "IBM US Equity", "px_last": None},
    ]

    assert arrow_table[["ticker", "volume"]].column_names == ["ticker", "volume"]
    assert arrow_table[(0, 2)].column_names == ["ticker", "px_last"]
    assert arrow_table.drop_columns(["volume"]).column_names == ["ticker", "date", "px_last"]
    assert arrow_table.rename_columns({"px_last": "last"}).column_names == [
        "ticker",
        "date",
        "last",
        "volume",
    ]
    assert arrow_table.rename({"px_last": "last"}).column_names == [
        "ticker",
        "date",
        "last",
        "volume",
    ]
    assert arrow_table.slice(1, 1).to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]
    assert arrow_table.head(1).column("ticker") == ["AAPL US Equity"]
    assert arrow_table.tail(1).column("ticker") == ["IBM US Equity"]


def test_record_batch_api_matches_table_carrier_surface(arrow_table: Any) -> None:
    batch = arrow_table.to_batches()[0]

    assert isinstance(batch, ArrowRecordBatch)
    assert len(batch) == 3
    assert batch.shape == (3, 4)
    assert batch.nbytes > 0
    assert [column.name for column in batch.columns] == arrow_table.column_names
    assert batch.column("ticker") == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert batch.get_column(0).to_pylist() == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
    assert batch["px_last"].to_pylist() == [150.0, 380.0, None]
    assert batch.select(["ticker", "volume"]).column_names == ["ticker", "volume"]
    assert batch.slice(1, 1).to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]
    assert batch.to_table().to_pylist() == arrow_table.to_pylist()


def test_sort_filter_and_column_mutation_still_delegate_to_native_arrow(arrow_table: Any) -> None:
    sorted_table = arrow_table.sort_by([("volume", "descending")])
    assert sorted_table.column("ticker") == ["IBM US Equity", "MSFT US Equity", "AAPL US Equity"]

    filtered = arrow_table.filter_eq("ticker", "MSFT US Equity")
    assert filtered.to_pylist() == [
        {"ticker": "MSFT US Equity", "date": date(2024, 1, 2), "px_last": 380.0, "volume": 20}
    ]

    added = arrow_table.add_column(1, "side", ["A", "B", "C"])
    assert added.column_names == ["ticker", "side", "date", "px_last", "volume"]
    assert added.column("side") == ["A", "B", "C"]

    replaced = added.set_column(1, "side2", ["ask", "bid", "mid"])
    assert replaced.column_names == ["ticker", "side2", "date", "px_last", "volume"]
    assert replaced.column("side2") == ["ask", "bid", "mid"]


def test_pyarrow_helpers_are_lazy_optional_conversions(arrow_table: Any) -> None:
    pa = pytest.importorskip("pyarrow")

    pyarrow_table = arrow_table.to_pyarrow()
    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.column_names == arrow_table.column_names

    pyarrow_batch = arrow_table.to_batches()[0].to_pyarrow()
    assert isinstance(pyarrow_batch, pa.RecordBatch)

    pyarrow_column = arrow_table.column("ticker").to_pyarrow()
    assert isinstance(pyarrow_column, pa.ChunkedArray)
    assert pyarrow_column.to_pylist() == ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
