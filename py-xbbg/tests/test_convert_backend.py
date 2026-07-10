"""Tests for conversion from xbbg native Arrow objects to public backends."""

from __future__ import annotations

import builtins
import importlib.util
from typing import Any

import narwhals.stable.v1 as nw
import pytest

from xbbg import blp
from xbbg._core import ArrowTable
from xbbg.backend import _attach_xbbg_metadata_attrs, check_backend, convert_backend_frame
from xbbg.blp import Backend


@pytest.fixture
def arrow_table() -> Any:
    return ArrowTable.from_pylist(
        [
            {"ticker": "AAPL US Equity", "date": "2024-01-01", "px_last": 150.0},
            {"ticker": "MSFT US Equity", "date": "2024-01-01", "px_last": 380.0},
        ]
    )


def _block_imports(monkeypatch: pytest.MonkeyPatch, *roots: str) -> None:
    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        root = name.split(".", 1)[0]
        if root in roots:
            raise ImportError(f"blocked optional dataframe backend: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


class TestConvertBackendNative:
    def test_convert_native_returns_identity(self, arrow_table: Any):
        result = convert_backend_frame(arrow_table, Backend.NATIVE)
        assert result is arrow_table

    def test_facade_conversion_defaults_to_narwhals(self, arrow_table: Any):
        result = blp._convert_result_backend(arrow_table, None)
        assert isinstance(result, nw.DataFrame)

        native = result.to_native()
        if importlib.util.find_spec("pyarrow") is not None and check_backend(Backend.PYARROW, raise_on_error=False):
            pa = pytest.importorskip("pyarrow")
            assert isinstance(native, pa.Table)
            assert native.column_names == arrow_table.column_names
        elif importlib.util.find_spec("pandas") is not None and check_backend(Backend.PANDAS, raise_on_error=False):
            pd = pytest.importorskip("pandas")
            assert isinstance(native, pd.DataFrame)
            assert list(native.columns) == arrow_table.column_names
        elif importlib.util.find_spec("polars") is not None and check_backend(Backend.POLARS, raise_on_error=False):
            pl = pytest.importorskip("polars")
            assert isinstance(native, pl.DataFrame)
            assert native.columns == arrow_table.column_names
        else:
            assert native is arrow_table

    def test_facade_conversion_honors_set_backend(self, arrow_table: Any):
        original = blp.get_backend()
        try:
            blp.set_backend(Backend.NATIVE)
            assert blp._convert_result_backend(arrow_table, None) is arrow_table
        finally:
            blp.set_backend(original)

    def test_convert_record_batch_to_native_table(self, arrow_table: Any):
        batch = arrow_table.to_batches()[0]
        result = convert_backend_frame(batch, Backend.NATIVE)
        assert result.column_names == arrow_table.column_names
        assert result.to_pylist() == arrow_table.to_pylist()

    def test_convert_pyarrow_record_batch_to_table(self):
        pa = pytest.importorskip("pyarrow")
        batch = pa.record_batch(
            [["IBM US Equity"], [123.45]],
            names=["ticker", "px_last"],
        )

        result = convert_backend_frame(batch, Backend.NATIVE)

        assert isinstance(result, pa.Table)
        assert result.column_names == ["ticker", "px_last"]
        assert result.to_pylist() == [{"ticker": "IBM US Equity", "px_last": 123.45}]

    def test_record_batch_and_table_indexing_return_arrow_columns(self, arrow_table: Any):
        batch = arrow_table.to_batches()[0]

        batch_column = batch["ticker"]
        assert batch_column.name == "ticker"
        assert batch_column.to_pylist() == ["AAPL US Equity", "MSFT US Equity"]
        assert batch_column[0] == "AAPL US Equity"
        assert batch_column[-1] == "MSFT US Equity"
        assert len(batch_column) == 2
        assert len(batch) == 2

        table_column = arrow_table["px_last"]
        assert table_column.name == "px_last"
        assert table_column.to_pylist() == [150.0, 380.0]
        assert len(arrow_table) == 2

    def test_rejects_non_native_inputs(self):
        with pytest.raises(TypeError, match="Expected xbbg ArrowTable or ArrowRecordBatch"):
            convert_backend_frame({"ticker": ["IBM"]}, Backend.NATIVE)


class TestConvertBackendPyArrow:
    def test_convert_pyarrow_returns_table(self, arrow_table: Any):
        pa = pytest.importorskip("pyarrow")
        result = convert_backend_frame(arrow_table, Backend.PYARROW)
        assert isinstance(result, pa.Table)
        assert result.column_names == arrow_table.column_names
        assert result.num_rows == arrow_table.num_rows


class TestConvertBackendPandas:
    def test_convert_pandas_prefers_pyarrow_when_available(self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch):
        pd = pytest.importorskip("pandas")
        pa = pytest.importorskip("pyarrow")
        import xbbg.backend as backend_module

        real_import_backend = backend_module._import_backend_module
        calls: list[str] = []

        class PyArrowProxy:
            def table(self, value: Any) -> Any:
                calls.append("pyarrow.table")
                return pa.table(value)

        def import_backend(backend: Backend | str, *, feature: str | None = None) -> Any:
            if Backend(backend) is Backend.PYARROW:
                calls.append("import pyarrow")
                return PyArrowProxy()
            return real_import_backend(backend, feature=feature)

        monkeypatch.setattr(backend_module, "_import_backend_module", import_backend)

        result = convert_backend_frame(arrow_table, Backend.PANDAS)

        assert isinstance(result, pd.DataFrame)
        assert result.shape == (arrow_table.num_rows, arrow_table.num_columns)
        assert list(result.columns) == arrow_table.column_names
        assert result["ticker"].tolist() == ["AAPL US Equity", "MSFT US Equity"]
        assert result["px_last"].tolist() == [150.0, 380.0]
        assert calls == ["import pyarrow", "pyarrow.table"]

    def test_attach_xbbg_metadata_attrs_copies_present_native_metadata(self):
        pd = pytest.importorskip("pandas")

        class FakeTable:
            metadata = {
                "xbbg.eid_data": '{"IBM US Equity":[101,202]}',
                "xbbg.security_errors": '{"BAD Ticker":{"message":"bad security"}}',
            }
            eid_data = {"IBM US Equity": [101, 202]}
            security_errors = {"BAD Ticker": {"message": "bad security"}}
            field_exceptions = None

        frame = pd.DataFrame({"ticker": ["IBM US Equity"]})

        result = _attach_xbbg_metadata_attrs(frame, FakeTable())

        assert result is frame
        assert frame.attrs["xbbg_eid_data"] == {"IBM US Equity": [101, 202]}
        assert frame.attrs["xbbg_security_errors"] == {"BAD Ticker": {"message": "bad security"}}
        assert "xbbg_field_exceptions" not in frame.attrs

    def test_convert_pandas_falls_back_when_pyarrow_import_raises(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch
    ):
        pd = pytest.importorskip("pandas")
        import xbbg.backend as backend_module

        real_import_backend = backend_module._import_backend_module
        attempted_pyarrow = False

        def import_backend(backend: Backend | str, *, feature: str | None = None) -> Any:
            nonlocal attempted_pyarrow
            if Backend(backend) is Backend.PYARROW:
                attempted_pyarrow = True
                raise ImportError("pyarrow unavailable")
            return real_import_backend(backend, feature=feature)

        monkeypatch.setattr(backend_module, "_import_backend_module", import_backend)

        result = convert_backend_frame(arrow_table, Backend.PANDAS)

        assert attempted_pyarrow is True
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (arrow_table.num_rows, arrow_table.num_columns)
        assert list(result.columns) == arrow_table.column_names
        assert result["ticker"].tolist() == ["AAPL US Equity", "MSFT US Equity"]
        assert result["px_last"].tolist() == [150.0, 380.0]


class TestConvertBackendPolars:
    def test_convert_polars_returns_dataframe(self, arrow_table: Any):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.POLARS, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")
        result = convert_backend_frame(arrow_table, Backend.POLARS)
        assert isinstance(result, pl.DataFrame)
        assert result.columns == arrow_table.column_names

    def test_convert_polars_lazy_returns_lazyframe(self, arrow_table: Any):
        pl = pytest.importorskip("polars")
        if not check_backend(Backend.POLARS_LAZY, raise_on_error=False):
            pytest.skip("polars package is not usable in this environment")
        result = convert_backend_frame(arrow_table, Backend.POLARS_LAZY)
        assert isinstance(result, pl.LazyFrame)


class TestConvertBackendDuckDB:
    def test_convert_duckdb_relation(self, arrow_table: Any):
        duckdb = pytest.importorskip("duckdb")
        result = convert_backend_frame(arrow_table, Backend.DUCKDB)
        assert result.fetchone() is not None
        assert result.columns == arrow_table.column_names
        del duckdb


class TestConvertBackendNarwhals:
    def _block_dataframe_backend_imports(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _block_imports(monkeypatch, "pyarrow", "pandas", "polars", "arro3")
        import xbbg.backend as backend_module

        backend_module._best_narwhals_backend.cache_clear()
        monkeypatch.setattr("xbbg.backend._native_narwhals_fallback_warned", True)

    def test_convert_narwhals_prefers_pyarrow_when_available(self, arrow_table: Any):
        pa = pytest.importorskip("pyarrow")
        result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        assert isinstance(result, nw.DataFrame)
        native = result.to_native()
        assert isinstance(native, pa.Table)
        assert native.column_names == arrow_table.column_names

    def test_convert_narwhals_falls_back_to_xbbg_plugin(self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch):
        self._block_dataframe_backend_imports(monkeypatch)
        monkeypatch.setattr("xbbg.backend._native_narwhals_fallback_warned", False)
        with pytest.warns(RuntimeWarning, match="limited xbbg native ArrowTable plugin"):
            result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        assert isinstance(result, nw.DataFrame)
        assert result.to_native() is arrow_table
        assert result.columns == arrow_table.column_names

    def test_narwhals_select_delegates_to_xbbg_table_when_native_fallback(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch
    ):
        self._block_dataframe_backend_imports(monkeypatch)
        result = convert_backend_frame(arrow_table, Backend.NARWHALS)
        selected = result.select("ticker", "px_last")
        native = selected.to_native()
        assert native.column_names == ["ticker", "px_last"]
        assert native.to_pylist() == [
            {"ticker": "AAPL US Equity", "px_last": 150.0},
            {"ticker": "MSFT US Equity", "px_last": 380.0},
        ]

    def test_convert_narwhals_lazy_collects_to_xbbg_plugin_when_native_fallback(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch
    ):
        self._block_dataframe_backend_imports(monkeypatch)
        result = convert_backend_frame(arrow_table, Backend.NARWHALS_LAZY)
        collected = result.select("ticker").collect()
        native = collected.to_native()
        assert native.column_names == ["ticker"]
        assert native.column("ticker") == ["AAPL US Equity", "MSFT US Equity"]


class TestConvertBackendInvalid:
    def test_invalid_string_backend_raises(self, arrow_table: Any):
        with pytest.raises(ValueError):
            convert_backend_frame(arrow_table, "invalid_backend")

    @pytest.mark.parametrize(
        ("backend", "blocked_root", "extra"),
        [
            (Backend.PYARROW, "pyarrow", "xbbg[pyarrow]"),
            (Backend.PANDAS, "pandas", "xbbg[pandas]"),
            (Backend.POLARS, "polars", "xbbg[polars]"),
            (Backend.POLARS_LAZY, "polars", "xbbg[polars]"),
            (Backend.DUCKDB, "duckdb", "xbbg[duckdb]"),
        ],
    )
    def test_missing_explicit_backend_raises_actionable_error(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch, backend: Backend, blocked_root: str, extra: str
    ):
        _block_imports(monkeypatch, blocked_root)

        with pytest.raises(ImportError) as exc_info:
            convert_backend_frame(arrow_table, backend)

        msg = str(exc_info.value)
        assert f"Backend '{backend.value}' requires" in msg
        assert f"pip install {blocked_root}" in msg
        assert extra in msg

    @pytest.mark.parametrize(
        "backend",
        [
            Backend.CUDF,
            Backend.MODIN,
            Backend.DASK,
            Backend.IBIS,
            Backend.PYSPARK,
            Backend.SQLFRAME,
        ],
    )
    def test_selectable_unimplemented_backends_raise_instead_of_falling_through(
        self, arrow_table: Any, monkeypatch: pytest.MonkeyPatch, backend: Backend
    ):
        monkeypatch.setattr("xbbg.backend.check_backend", lambda *_args, **_kwargs: True)

        with pytest.raises(NotImplementedError, match=f"Backend '{backend.value}'.*not implemented"):
            convert_backend_frame(arrow_table, backend)

    def test_set_backend_missing_optional_dependency_errors_before_state_change(self, monkeypatch: pytest.MonkeyPatch):
        from xbbg.blp import get_backend, set_backend

        original = get_backend()
        _block_imports(monkeypatch, "pandas")

        with pytest.raises(ImportError, match="Backend 'pandas' requires"):
            set_backend(Backend.PANDAS)

        assert get_backend() is original
