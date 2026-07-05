"""Narwhals plugin implementation for xbbg native Arrow objects.

This module intentionally delegates dataframe operations to ``xbbg.ArrowTable``
methods instead of materializing through pandas, Polars, Apache Arrow Python bindings, or arro3.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import ModuleType
from typing import Any

from narwhals._utils import Implementation, Version

from xbbg.backend import Backend, _import_backend_module, check_backend, is_backend_available


def _arrow_table_class() -> type[Any]:
    from xbbg._core import ArrowTable

    return ArrowTable


def _arrow_record_batch_class() -> type[Any]:
    from xbbg._core import ArrowRecordBatch

    return ArrowRecordBatch


def _is_arrow_table(value: Any) -> bool:
    return value.__class__.__name__ == "ArrowTable" and hasattr(value, "__arrow_c_stream__")


def _is_arrow_record_batch(value: Any) -> bool:
    return value.__class__.__name__ == "ArrowRecordBatch" and hasattr(value, "__arrow_c_array__")


def _ensure_table(value: Any) -> Any:
    if _is_arrow_table(value):
        return value
    if _is_arrow_record_batch(value):
        return value.to_table()
    raise TypeError(f"Expected xbbg ArrowTable or ArrowRecordBatch, got {type(value).__name__}")


def _native_namespace() -> ModuleType:
    import xbbg

    return xbbg


def _unknown_schema(table: Any, version: Version) -> dict[str, Any]:
    unknown = version.dtypes.Unknown
    return {name: unknown() for name in table.column_names}


def _flatten_columns(columns: Sequence[str] | Sequence[Iterable[str]]) -> list[str]:
    out: list[str] = []
    for item in columns:
        if isinstance(item, str):
            out.append(item)
        else:
            out.extend(str(value) for value in item)
    return out


class XbbgNamespace:
    """Minimal Narwhals namespace for xbbg native Arrow frames."""

    _implementation = Implementation.UNKNOWN

    def __init__(self, *, version: Version) -> None:
        self._version = version

    @property
    def _expr(self) -> type[Any]:
        raise NotImplementedError("xbbg Narwhals expression execution is not implemented for this operation")

    def from_native(self, data: Any, /) -> XbbgDataFrame:
        return XbbgDataFrame(_ensure_table(data), version=self._version)

    def is_native(self, obj: Any, /) -> bool:
        return _is_arrow_table(obj) or _is_arrow_record_batch(obj)

    def concat(self, items: Iterable[XbbgDataFrame], *, how: str) -> XbbgDataFrame:
        if how != "vertical":
            raise NotImplementedError("xbbg Narwhals plugin currently supports vertical concat only")
        tables = [item.native for item in items]
        return XbbgDataFrame(_arrow_table_class().concat_tables(tables), version=self._version)


class XbbgDataFrame:
    """Narwhals-compliant eager frame backed by ``xbbg.ArrowTable``."""

    _implementation = Implementation.UNKNOWN

    def __init__(self, table: Any, *, version: Version) -> None:
        self._native_frame = _ensure_table(table)
        self._version = version

    def __narwhals_dataframe__(self) -> XbbgDataFrame:
        return self

    def __narwhals_namespace__(self) -> XbbgNamespace:
        return XbbgNamespace(version=self._version)

    def __native_namespace__(self) -> ModuleType:
        return _native_namespace()

    @classmethod
    def from_native(cls, data: Any, /, *, context: Any) -> XbbgDataFrame:
        return cls(_ensure_table(data), version=context._version)

    def to_narwhals(self) -> Any:
        return self._version.dataframe(self, level="full")

    @property
    def native(self) -> Any:
        return self._native_frame

    @property
    def columns(self) -> list[str]:
        return list(self.native.column_names)

    @property
    def schema(self) -> Mapping[str, Any]:
        return self.collect_schema()

    @property
    def shape(self) -> tuple[int, int]:
        return (self.native.num_rows, self.native.num_columns)

    def __len__(self) -> int:
        return self.native.num_rows

    def _with_native(self, table: Any) -> XbbgDataFrame:
        return type(self)(_ensure_table(table), version=self._version)

    def _with_version(self, version: Version) -> XbbgDataFrame:
        return type(self)(self.native, version=version)

    def collect_schema(self) -> Mapping[str, Any]:
        return _unknown_schema(self.native, self._version)

    def clone(self) -> XbbgDataFrame:
        return self._with_native(self.native)

    def simple_select(self, *column_names: str) -> XbbgDataFrame:
        return self._with_native(self.native.select_columns(list(column_names)))

    def select(self, *exprs: Any) -> XbbgDataFrame:
        if all(isinstance(expr, str) for expr in exprs):
            return self.simple_select(*(str(expr) for expr in exprs))
        raise NotImplementedError("xbbg Narwhals plugin supports direct string column selection only")

    def drop(self, columns: Sequence[str] | Sequence[Iterable[str]], *, strict: bool) -> XbbgDataFrame:
        names = _flatten_columns(columns)
        if strict:
            missing = [name for name in names if name not in self.columns]
            if missing:
                raise KeyError(f"unknown columns: {missing}")
        return self._with_native(self.native.drop_columns(names))

    def rename(self, mapping: Mapping[str, str]) -> XbbgDataFrame:
        return self._with_native(self.native.rename_columns(dict(mapping)))

    def head(self, n: int) -> XbbgDataFrame:
        return self._with_native(self.native.head(max(n, 0)))

    def sort(self, *by: str, descending: bool | Sequence[bool], nulls_last: bool) -> XbbgDataFrame:
        del nulls_last
        if isinstance(descending, bool):
            directions = ["descending" if descending else "ascending"] * len(by)
        else:
            directions = ["descending" if value else "ascending" for value in descending]
        return self._with_native(self.native.sort_by(list(zip(by, directions, strict=False))))

    def lazy(self, backend: Any = None, *, session: Any = None) -> XbbgLazyFrame:
        del backend, session
        return XbbgLazyFrame(self.native, version=self._version)

    def to_pandas(self) -> Any:
        pd = _import_backend_module(Backend.PANDAS, feature="XbbgDataFrame.to_pandas()")
        try:
            pa = _import_backend_module(Backend.PYARROW, feature="XbbgDataFrame.to_pandas()")
        except ImportError:
            # Slow fallback for installations without pyarrow: materializes one
            # Python dict per cell path instead of consuming the Arrow C stream.
            return pd.DataFrame.from_records(self.native.to_pylist(), columns=self.columns)

        return pa.table(self.native).to_pandas(split_blocks=True)

    def to_arrow(self) -> Any:
        pa = _import_backend_module(Backend.PYARROW, feature="XbbgDataFrame.to_arrow()")

        return pa.table(self.native)

    def to_polars(self) -> Any:
        pl = _import_backend_module(Backend.POLARS, feature="XbbgDataFrame.to_polars()")

        if is_backend_available(Backend.PYARROW) and check_backend(Backend.PYARROW, raise_on_error=False):
            pa = _import_backend_module(Backend.PYARROW)
            return pl.from_arrow(pa.table(self.native))

        return pl.DataFrame(self.native.to_pylist(), schema=self.columns)

    def to_dict(self, *, as_series: bool) -> dict[str, Any]:
        if as_series:
            raise NotImplementedError("xbbg Narwhals plugin does not expose Series objects yet")
        return {name: self.native.column(name).to_pylist() for name in self.columns}

    def rows(self, *, named: bool) -> Sequence[tuple[Any, ...]] | Sequence[Mapping[str, Any]]:
        if named:
            return self.native.to_pylist()
        names = self.columns
        return [tuple(row.get(name) for name in names) for row in self.native.to_pylist()]

    def iter_rows(self, *, named: bool, buffer_size: int) -> Iterator[tuple[Any, ...]] | Iterator[Mapping[str, Any]]:
        del buffer_size
        return iter(self.rows(named=named))

    def row(self, index: int) -> tuple[Any, ...]:
        rows = self.rows(named=False)
        return rows[index]

    def item(self, row: int | None, column: int | str | None) -> Any:
        row_idx = 0 if row is None else row
        if isinstance(column, str):
            return self.native.column(column)[row_idx]
        col_idx = 0 if column is None else column
        return self.row(row_idx)[col_idx]


class XbbgLazyFrame:
    """Narwhals-compliant lazy frame backed by xbbg Arrow operations.

    The current implementation stores the native Arrow table and applies supported
    operations eagerly to that table; ``collect`` returns an xbbg-backed eager
    Narwhals frame without crossing into third-party dataframe libraries.
    """

    _implementation = Implementation.UNKNOWN

    def __init__(self, table: Any, *, version: Version) -> None:
        self._native_frame = _ensure_table(table)
        self._version = version

    def __narwhals_lazyframe__(self) -> XbbgLazyFrame:
        return self

    def __narwhals_namespace__(self) -> XbbgNamespace:
        return XbbgNamespace(version=self._version)

    def __native_namespace__(self) -> ModuleType:
        return _native_namespace()

    @property
    def native(self) -> Any:
        return self._native_frame

    @property
    def columns(self) -> list[str]:
        return list(self.native.column_names)

    @property
    def schema(self) -> Mapping[str, Any]:
        return self.collect_schema()

    def _with_native(self, table: Any) -> XbbgLazyFrame:
        return type(self)(_ensure_table(table), version=self._version)

    def _with_version(self, version: Version) -> XbbgLazyFrame:
        return type(self)(self.native, version=version)

    def collect_schema(self) -> Mapping[str, Any]:
        return _unknown_schema(self.native, self._version)

    def collect(self, backend: Any = None, **kwargs: Any) -> XbbgDataFrame:
        del backend, kwargs
        return XbbgDataFrame(self.native, version=self._version)

    def simple_select(self, *column_names: str) -> XbbgLazyFrame:
        return self._with_native(self.native.select_columns(list(column_names)))

    def select(self, *exprs: Any) -> XbbgLazyFrame:
        if all(isinstance(expr, str) for expr in exprs):
            return self.simple_select(*(str(expr) for expr in exprs))
        raise NotImplementedError("xbbg Narwhals plugin supports direct string column selection only")

    def drop(self, columns: Sequence[str] | Sequence[Iterable[str]], *, strict: bool) -> XbbgLazyFrame:
        names = _flatten_columns(columns)
        if strict:
            missing = [name for name in names if name not in self.columns]
            if missing:
                raise KeyError(f"unknown columns: {missing}")
        return self._with_native(self.native.drop_columns(names))

    def rename(self, mapping: Mapping[str, str]) -> XbbgLazyFrame:
        return self._with_native(self.native.rename_columns(dict(mapping)))

    def head(self, n: int) -> XbbgLazyFrame:
        return self._with_native(self.native.head(max(n, 0)))

    def sort(self, *by: str, descending: bool | Sequence[bool], nulls_last: bool) -> XbbgLazyFrame:
        del nulls_last
        if isinstance(descending, bool):
            directions = ["descending" if descending else "ascending"] * len(by)
        else:
            directions = ["descending" if value else "ascending" for value in descending]
        return self._with_native(self.native.sort_by(list(zip(by, directions, strict=False))))

    def lazy(self) -> XbbgLazyFrame:
        return self
