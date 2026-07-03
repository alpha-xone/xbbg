"""Backend enum and availability infrastructure for xbbg.

This module provides the canonical ``Backend`` enum for selecting
DataFrame output backends, along with availability checking, version
validation, and format compatibility helpers.

The Rust layer returns Arrow tables; this module is a Python-only
concern that governs how those tables are converted to the user's
preferred DataFrame type.

Ported from ``release/0.x`` ``xbbg/backend.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import sys
from typing import Any, TypeAlias
import warnings

import narwhals.stable.v1 as nw

from xbbg.services import Format

logger = logging.getLogger(__name__)
DataFrameResult: TypeAlias = Any


# =============================================================================
# Backend Enum
# =============================================================================


class Backend(str, Enum):
    """Enum for selecting the data processing backend.

    The backend determines which library is used for data manipulation
    and storage.  Each backend has different performance characteristics
    and memory usage patterns.

    **Eager backends** (full API support):

    - ``NATIVE`` – xbbg native Arrow carrier object
    - ``PYARROW`` – Apache Arrow Python ``pyarrow.Table``
    - ``NARWHALS`` – Backend-agnostic DataFrame API
    - ``PANDAS`` – Most widely used DataFrame library
    - ``POLARS`` – High-performance Rust-based DataFrames
    - ``CUDF`` – GPU-accelerated DataFrames (NVIDIA RAPIDS)
    - ``MODIN`` – Distributed pandas-like DataFrames

    **Lazy backends** (deferred execution):

    - ``NARWHALS_LAZY`` – Narwhals with lazy evaluation
    - ``POLARS_LAZY`` – Polars LazyFrame for query optimisation
    - ``DUCKDB`` – Embedded analytical SQL database
    - ``DASK`` – Parallel computing with task scheduling
    - ``IBIS`` – Portable DataFrame expressions (SQL backends)
    - ``PYSPARK`` – Apache Spark distributed DataFrames
    - ``SQLFRAME`` – SQL-based DataFrame abstraction
    """

    # Eager backends (full API support)
    NATIVE = "native"
    PYARROW = "pyarrow"
    NARWHALS = "narwhals"
    PANDAS = "pandas"
    POLARS = "polars"
    CUDF = "cudf"
    MODIN = "modin"

    # Lazy backends (deferred execution)
    NARWHALS_LAZY = "narwhals_lazy"
    POLARS_LAZY = "polars_lazy"
    DUCKDB = "duckdb"
    DASK = "dask"
    IBIS = "ibis"
    PYSPARK = "pyspark"
    SQLFRAME = "sqlframe"

    @classmethod
    def _missing_(cls, value: object) -> Backend | None:
        if isinstance(value, str):
            lowered = value.lower()
            for member in cls:
                if member.value == lowered:
                    return member
        return None


# =============================================================================
# Backend Descriptors
# =============================================================================


class BackendConversion(str, Enum):
    """Conversion strategy for xbbg native Arrow carriers."""

    NATIVE = "native"
    PYARROW = "pyarrow"
    PANDAS = "pandas"
    POLARS = "polars"
    POLARS_LAZY = "polars_lazy"
    NARWHALS = "narwhals"
    NARWHALS_LAZY = "narwhals_lazy"
    DUCKDB = "duckdb"
    PLANNED = "planned"


@dataclass(frozen=True)
class BackendDescriptor:
    """Single source of truth for backend metadata and conversion status."""

    backend: Backend
    package_name: str | None
    module_name: str | None
    extra_name: str | None
    min_version: tuple[int, ...] | None
    supported_formats: frozenset[Format]
    conversion: BackendConversion

    @property
    def implemented(self) -> bool:
        return self.conversion is not BackendConversion.PLANNED


_ALL_FORMATS = frozenset(
    {
        Format.LONG,
        Format.SEMI_LONG,
        Format.LONG_TYPED,
        Format.LONG_WITH_METADATA,
    }
)


def _descriptor(
    backend: Backend,
    *,
    package_name: str | None = None,
    module_name: str | None = None,
    extra_name: str | None = None,
    min_version: tuple[int, ...] | None = None,
    conversion: BackendConversion,
) -> BackendDescriptor:
    return BackendDescriptor(
        backend=backend,
        package_name=package_name,
        module_name=module_name,
        extra_name=extra_name,
        min_version=min_version,
        supported_formats=_ALL_FORMATS,
        conversion=conversion,
    )


BACKEND_DESCRIPTORS: dict[Backend, BackendDescriptor] = {
    Backend.NATIVE: _descriptor(Backend.NATIVE, conversion=BackendConversion.NATIVE),
    Backend.PYARROW: _descriptor(
        Backend.PYARROW,
        package_name="pyarrow",
        module_name="pyarrow",
        extra_name="pyarrow",
        min_version=(22, 0),
        conversion=BackendConversion.PYARROW,
    ),
    Backend.NARWHALS: _descriptor(
        Backend.NARWHALS,
        package_name="narwhals",
        module_name="narwhals",
        conversion=BackendConversion.NARWHALS,
    ),
    Backend.NARWHALS_LAZY: _descriptor(
        Backend.NARWHALS_LAZY,
        package_name="narwhals",
        module_name="narwhals",
        conversion=BackendConversion.NARWHALS_LAZY,
    ),
    Backend.PANDAS: _descriptor(
        Backend.PANDAS,
        package_name="pandas",
        module_name="pandas",
        extra_name="pandas",
        min_version=(2, 0),
        conversion=BackendConversion.PANDAS,
    ),
    Backend.POLARS: _descriptor(
        Backend.POLARS,
        package_name="polars",
        module_name="polars",
        extra_name="polars",
        min_version=(0, 20),
        conversion=BackendConversion.POLARS,
    ),
    Backend.POLARS_LAZY: _descriptor(
        Backend.POLARS_LAZY,
        package_name="polars",
        module_name="polars",
        extra_name="polars",
        min_version=(0, 20),
        conversion=BackendConversion.POLARS_LAZY,
    ),
    Backend.DUCKDB: _descriptor(
        Backend.DUCKDB,
        package_name="duckdb",
        module_name="duckdb",
        extra_name="duckdb",
        min_version=(1, 0),
        conversion=BackendConversion.DUCKDB,
    ),
    Backend.CUDF: _descriptor(
        Backend.CUDF,
        package_name="cudf-cu12",
        module_name="cudf",
        min_version=(24, 10),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.MODIN: _descriptor(
        Backend.MODIN,
        package_name="modin[all]",
        module_name="modin",
        min_version=(0, 25),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.DASK: _descriptor(
        Backend.DASK,
        package_name="dask[dataframe]",
        module_name="dask",
        min_version=(2024, 1),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.IBIS: _descriptor(
        Backend.IBIS,
        package_name="ibis-framework",
        module_name="ibis",
        min_version=(6, 0),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.PYSPARK: _descriptor(
        Backend.PYSPARK,
        package_name="pyspark",
        module_name="pyspark",
        min_version=(3, 5),
        conversion=BackendConversion.PLANNED,
    ),
    Backend.SQLFRAME: _descriptor(
        Backend.SQLFRAME,
        package_name="sqlframe",
        module_name="sqlframe",
        min_version=(3, 22),
        conversion=BackendConversion.PLANNED,
    ),
}

MIN_VERSIONS: dict[Backend, tuple[int, ...]] = {
    backend: descriptor.min_version
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.min_version is not None
}
PACKAGE_NAMES: dict[Backend, str] = {
    backend: descriptor.package_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.package_name is not None
}
EXTRA_NAMES: dict[Backend, str] = {
    backend: descriptor.extra_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.extra_name is not None
}
MODULE_NAMES: dict[Backend, str] = {
    backend: descriptor.module_name
    for backend, descriptor in BACKEND_DESCRIPTORS.items()
    if descriptor.module_name is not None
}
SUPPORTED_FORMATS: dict[Backend, frozenset[Format]] = {
    backend: descriptor.supported_formats for backend, descriptor in BACKEND_DESCRIPTORS.items()
}


# =============================================================================
# Internal Helpers
# =============================================================================


def _parse_version(version_str: str) -> tuple[int, ...]:
    """Parse a version string into a comparable tuple of ints.

    Strips pre-release suffixes (``a1``, ``b2``, ``rc1``, ``+local``).

    >>> _parse_version("2.0.1")
    (2, 0, 1)
    >>> _parse_version("0.20.4a1")
    (0, 20, 4)
    """
    version_str = version_str.split("+")[0]  # remove local
    version_str = version_str.split("a")[0]  # remove alpha
    version_str = version_str.split("b")[0]  # remove beta
    version_str = version_str.split("rc")[0]  # remove rc

    parts: list[int] = []
    for part in version_str.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            break
    return tuple(parts)


def _format_version(version: tuple[int, ...]) -> str:
    """Format a version tuple as a dotted string."""
    return ".".join(str(v) for v in version)


def _get_module(backend: Backend) -> Any | None:
    """Return the backend's module if it has already been imported.

    Checks ``sys.modules`` without triggering an import, following
    the narwhals convention.
    """
    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        return None
    return sys.modules.get(module_name)


def _get_module_version(module: Any) -> tuple[int, ...] | None:
    """Return a module's ``__version__`` as a parsed tuple, or ``None``."""
    version_str = getattr(module, "__version__", None)
    if version_str is None:
        version_str = getattr(module, "VERSION", None)
    if not version_str:
        return None
    return _parse_version(str(version_str))


# =============================================================================
# Backend Availability
# =============================================================================


def is_backend_available(backend: Backend | str) -> bool:
    """Check whether a backend is installed and importable.

    The native xbbg carrier and bundled Narwhals plugin always return ``True``.

    Args:
        backend: A :class:`Backend` member or its string value.

    Returns:
        ``True`` when the backend package can be imported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)

    # Core/native dependencies — always available.
    if backend in (Backend.NATIVE, Backend.NARWHALS, Backend.NARWHALS_LAZY):
        return True

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        return False

    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


def check_backend(backend: Backend | str, *, raise_on_error: bool = True) -> bool:
    """Check that a backend is available *and* meets minimum version requirements.

    Provides actionable error messages with ``pip install`` instructions.

    Args:
        backend: A :class:`Backend` member or its string value.
        raise_on_error: Raise on failure instead of returning ``False``.

    Returns:
        ``True`` when the backend is usable.

    Raises:
        ImportError: Package not installed (when *raise_on_error*).
        ValueError: Installed version too old (when *raise_on_error*).
    """
    if isinstance(backend, str):
        backend = Backend(backend)

    # Native/core dependencies — always OK.
    if backend in (Backend.NATIVE, Backend.NARWHALS, Backend.NARWHALS_LAZY):
        return True

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        msg = f"Unknown backend: {backend.value}"
        if raise_on_error:
            raise ValueError(msg)
        logger.warning(msg)
        return False

    package_name = PACKAGE_NAMES.get(backend, module_name)
    min_version = MIN_VERSIONS.get(backend)

    # Try to import.
    try:
        module = __import__(module_name)
    except ImportError:
        msg = (
            f"Backend '{backend.value}' requires the '{package_name}' package, "
            f"which is not installed.\n\n"
            f"To install, run:\n"
            f"    pip install {package_name}"
        )
        if min_version:
            msg += f">={_format_version(min_version)}"

        extra_name = EXTRA_NAMES.get(backend)
        if extra_name is not None:
            msg += "\n\nOr install with xbbg extras:\n"
            msg += f"    pip install xbbg[{extra_name}]"

        if raise_on_error:
            raise ImportError(msg) from None
        logger.warning(msg)
        return False

    if backend in (Backend.POLARS, Backend.POLARS_LAZY) and getattr(module, "__version__", None) == "":
        msg = (
            f"Backend '{backend.value}' requires a usable '{package_name}' package, "
            "but the installed Polars package is missing its native binary.\n\n"
            "To reinstall, run:\n"
            f"    pip install --force-reinstall {package_name}"
        )
        if min_version:
            msg += f">={_format_version(min_version)}"
        extra_name = EXTRA_NAMES.get(backend)
        if extra_name is not None:
            msg += "\n\nOr install with xbbg extras:\n"
            msg += f"    pip install xbbg[{extra_name}]"
        if raise_on_error:
            raise ImportError(msg) from None
        logger.warning(msg)
        return False

    # Version check.
    if min_version:
        version = _get_module_version(module)
        if version is None:
            logger.debug("Could not determine version for %s", module_name)
        elif version < min_version:
            msg = (
                f"Backend '{backend.value}' requires {package_name} >= "
                f"{_format_version(min_version)}, but version "
                f"{_format_version(version)} is installed.\n\n"
                f"To upgrade, run:\n"
                f"    pip install --upgrade {package_name}>={_format_version(min_version)}"
            )
            if raise_on_error:
                raise ValueError(msg)
            logger.warning(msg)
            return False

    return True


def _import_backend_module(backend: Backend | str, *, feature: str | None = None) -> Any:
    """Import an optional backend module after xbbg availability checks."""
    if isinstance(backend, str):
        backend = Backend(backend)

    try:
        check_backend(backend)
    except (ImportError, ValueError) as exc:
        if feature is not None:
            raise type(exc)(f"{feature} requires optional backend '{backend.value}'.\n\n{exc}") from None
        raise

    module_name = MODULE_NAMES.get(backend)
    if module_name is None:
        raise ValueError(f"Unknown backend: {backend.value}")
    return __import__(module_name)


def is_arrow_table(value: Any) -> bool:
    return value.__class__.__name__ == "ArrowTable" and hasattr(value, "__arrow_c_stream__")


def _is_arrow_record_batch(value: Any) -> bool:
    return value.__class__.__name__ == "ArrowRecordBatch" and hasattr(value, "__arrow_c_array__")


def _is_pyarrow_table(value: Any) -> bool:
    return value.__class__.__module__.startswith("pyarrow.") and value.__class__.__name__ == "Table"


def _is_pyarrow_record_batch(value: Any) -> bool:
    return value.__class__.__module__.startswith("pyarrow.") and value.__class__.__name__ == "RecordBatch"


def ensure_arrow_table(frame: Any) -> Any:
    if is_arrow_table(frame) or _is_pyarrow_table(frame):
        return frame
    if _is_arrow_record_batch(frame):
        return frame.to_table()
    if _is_pyarrow_record_batch(frame):
        import pyarrow as pa

        return pa.Table.from_batches([frame])
    raise TypeError(f"Expected xbbg ArrowTable or ArrowRecordBatch, got {type(frame).__name__}")


_XBBG_METADATA_ATTRS = {
    "xbbg.eid_data": ("eid_data", "xbbg_eid_data"),
    "xbbg.security_errors": ("security_errors", "xbbg_security_errors"),
    "xbbg.field_exceptions": ("field_exceptions", "xbbg_field_exceptions"),
}


def _attach_xbbg_metadata_attrs(dataframe: Any, table: Any) -> Any:
    """Attach native xbbg metadata to pandas ``DataFrame.attrs`` when present.

    Only pandas frames expose ``attrs`` in the conversion path.  The polars
    conversion intentionally skips this helper because polars frames do not
    provide an equivalent stable metadata side channel.
    """
    attrs = getattr(dataframe, "attrs", None)
    metadata = getattr(table, "metadata", None)
    if attrs is None or not metadata:
        return dataframe

    for metadata_key, (source_attr, frame_attr) in _XBBG_METADATA_ATTRS.items():
        if metadata_key not in metadata:
            continue
        value = getattr(table, source_attr, None)
        if value is not None:
            attrs[frame_attr] = value
    return dataframe


def _to_pyarrow_table(table: Any) -> Any:
    pa = _import_backend_module(Backend.PYARROW)
    return pa.table(table)


def _to_pandas_frame(table: Any) -> Any:
    pd = _import_backend_module(Backend.PANDAS)
    frame = pd.DataFrame.from_records(table.to_pylist(), columns=table.column_names)
    return _attach_xbbg_metadata_attrs(frame, table)


def _to_polars_frame(table: Any) -> Any:
    """Convert to polars; xbbg Arrow metadata is not attached as frame attrs."""
    pl = _import_backend_module(Backend.POLARS)

    if is_backend_available(Backend.PYARROW) and check_backend(Backend.PYARROW, raise_on_error=False):
        pa = _import_backend_module(Backend.PYARROW)
        return pl.from_arrow(pa.table(table))

    return pl.DataFrame(table.to_pylist(), schema=table.column_names)


_native_narwhals_fallback_warned = False


def _warn_native_narwhals_fallback() -> None:
    global _native_narwhals_fallback_warned
    if _native_narwhals_fallback_warned:
        return
    _native_narwhals_fallback_warned = True
    warnings.warn(
        "No optional dataframe backend is installed for xbbg's Narwhals output; "
        "falling back to the limited xbbg native ArrowTable plugin. "
        "Install `xbbg[pyarrow]`, `xbbg[pandas]`, or `xbbg[polars]` for full dataframe behavior, "
        "or request `backend='native'` explicitly if the raw xbbg ArrowTable is intended.",
        RuntimeWarning,
        stacklevel=3,
    )


def _best_narwhals_native(table: Any) -> Any:
    """Return the richest installed native object for Narwhals wrapping."""
    candidates = (
        (Backend.PYARROW, _to_pyarrow_table),
        (Backend.PANDAS, _to_pandas_frame),
        (Backend.POLARS, _to_polars_frame),
    )
    for candidate, convert in candidates:
        if not is_backend_available(candidate):
            continue
        if not check_backend(candidate, raise_on_error=False):
            continue
        try:
            return convert(table)
        except ImportError:
            continue
    _warn_native_narwhals_fallback()
    return table


def resolve_backend(
    backend: Backend | str | None,
    default_backend: Backend | str | None = None,
) -> Backend | None:
    """Resolve an optional backend selection against an optional configured default."""
    selected = default_backend if backend is None else backend
    if selected is None:
        return None
    return Backend(selected) if isinstance(selected, str) else selected


def effective_backend(
    backend: Backend | str | None,
    default_backend: Backend | str | None = None,
) -> Backend:
    """Resolve a backend selection, falling back to the package default if unset."""
    return resolve_backend(backend, default_backend) or get_default_backend()


def convert_backend_frame_with_default(
    frame: Any,
    backend: Backend | str | None,
    default_backend: Backend | str | None = None,
) -> DataFrameResult:
    """Convert an Arrow-like result using a per-call backend and configured default."""
    return convert_backend_frame(frame, effective_backend(backend, default_backend))


def convert_backend_frame(frame: Any, backend: Backend | str) -> DataFrameResult:
    """Convert an xbbg ArrowTable to the requested public backend."""
    effective = Backend(backend) if isinstance(backend, str) else backend
    descriptor = BACKEND_DESCRIPTORS[effective]
    table = ensure_arrow_table(frame)

    if effective not in (Backend.NATIVE, Backend.NARWHALS, Backend.NARWHALS_LAZY):
        check_backend(effective)

    match descriptor.conversion:
        case BackendConversion.NATIVE:
            return table
        case BackendConversion.PYARROW:
            return _to_pyarrow_table(table)
        case BackendConversion.PANDAS:
            return _to_pandas_frame(table)
        case BackendConversion.POLARS:
            return _to_polars_frame(table)
        case BackendConversion.POLARS_LAZY:
            return _to_polars_frame(table).lazy()
        case BackendConversion.NARWHALS:
            return nw.from_native(_best_narwhals_native(table))
        case BackendConversion.NARWHALS_LAZY:
            return nw.from_native(_best_narwhals_native(table)).lazy()
        case BackendConversion.DUCKDB:
            import duckdb

            con = duckdb.connect()
            con.register("xbbg_arrow", table)
            return con.sql("select * from xbbg_arrow")
        case BackendConversion.PLANNED:
            raise NotImplementedError(
                f"Backend '{effective.value}' is selectable but conversion from xbbg native Arrow "
                "is not implemented yet. Choose one of: native, pyarrow, pandas, polars, "
                "polars_lazy, narwhals, narwhals_lazy, duckdb."
            )

    raise AssertionError(f"Unhandled backend conversion: {descriptor.conversion!r}")


def get_available_backends() -> list[Backend]:
    """Return every :class:`Backend` whose package is currently importable."""
    return [b for b in Backend if is_backend_available(b)]


DEFAULT_BACKEND = Backend.NARWHALS


def get_default_backend() -> Backend:
    """Return the default public result backend for xbbg calls."""
    if check_backend(DEFAULT_BACKEND, raise_on_error=False):
        return DEFAULT_BACKEND
    raise ImportError(
        "No xbbg result backend is available. The core narwhals dependency should provide "
        "Backend.NARWHALS; if it is unavailable, reinstall xbbg or explicitly request "
        "a backend such as native, pyarrow, pandas, or polars."
    )


def print_backend_status() -> None:
    """Print a diagnostic table of all backends to the logger.

    Shows installed/missing status, current version, and minimum
    requirement for each backend.
    """
    logger.info("xbbg Backend Status")
    logger.info("=" * 60)
    logger.info("")

    for backend in Backend:
        module_name = MODULE_NAMES.get(backend, "")

        if backend == Backend.NATIVE:
            status = "OK (native)"
            version_info = "provided by xbbg"
        elif backend in (Backend.NARWHALS, Backend.NARWHALS_LAZY):
            module = _get_module(backend) or __import__(module_name)
            version = _get_module_version(module)
            status = "OK (core)"
            version_info = f"v{_format_version(version)}" if version else ""
        elif is_backend_available(backend):
            module = __import__(module_name)
            version = _get_module_version(module)
            min_ver = MIN_VERSIONS.get(backend)

            if version and min_ver and version < min_ver:
                status = "OUTDATED"
                version_info = f"v{_format_version(version)} (need >={_format_version(min_ver)})"
            else:
                status = "OK"
                version_info = f"v{_format_version(version)}" if version else ""
        else:
            status = "NOT INSTALLED"
            package = PACKAGE_NAMES.get(backend, module_name or "?")
            version_info = f"pip install {package}"

        logger.info("  %s %s %s", f"{backend.value:15}", f"{status:15}", version_info)

    logger.info("")
    logger.info("=" * 60)


# =============================================================================
# Format Compatibility Checking
# =============================================================================


def is_format_supported(backend: Backend | str, fmt: Format | str) -> bool:
    """Check whether *backend* supports *fmt*.

    Args:
        backend: Backend to check.
        fmt: Output format to check.

    Returns:
        ``True`` if the format is supported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)
    if isinstance(fmt, str):
        fmt = Format(fmt)
    return fmt in SUPPORTED_FORMATS.get(backend, frozenset())


def get_supported_formats(backend: Backend | str) -> frozenset[Format]:
    """Return the set of :class:`~xbbg.services.Format` variants supported by *backend*."""
    if isinstance(backend, str):
        backend = Backend(backend)
    return SUPPORTED_FORMATS.get(backend, frozenset({Format.LONG, Format.SEMI_LONG}))


def check_format_compatibility(
    backend: Backend | str,
    fmt: Format | str,
    *,
    raise_on_error: bool = True,
) -> bool:
    """Validate that *backend* supports *fmt*, with actionable messages.

    Args:
        backend: Backend to use.
        fmt: Desired output format.
        raise_on_error: Raise :class:`ValueError` on mismatch.

    Returns:
        ``True`` if compatible.

    Raises:
        ValueError: When *raise_on_error* and the combination is unsupported.
    """
    if isinstance(backend, str):
        backend = Backend(backend)
    if isinstance(fmt, str):
        fmt = Format(fmt)

    if is_format_supported(backend, fmt):
        return True

    supported = get_supported_formats(backend)
    supported_str = ", ".join(f.value for f in sorted(supported, key=lambda x: x.value))

    msg = (
        f"Backend '{backend.value}' does not support format '{fmt.value}'.\n\n"
        f"Supported formats for {backend.value}: {supported_str}\n\n"
    )

    if fmt in (Format.LONG_TYPED, Format.LONG_WITH_METADATA):
        msg += "Hint: LONG_TYPED and LONG_WITH_METADATA are v1.0 preview formats."

    if raise_on_error:
        raise ValueError(msg)

    logger.warning(msg)
    return False


def validate_backend_format(
    backend: Backend | str | None,
    fmt: Format | str | None,
    *,
    raise_on_error: bool = True,
) -> tuple[Backend, Format]:
    """Normalise and validate a backend/format pair.

    This is the main entry-point for API functions.  It:

    1. Converts string values to enums.
    2. Falls back to the session defaults (``set_backend`` / ``set_format``).
    3. Checks backend availability and version.
    4. Checks format compatibility.

    Args:
        backend: Backend enum, string, or ``None`` for the session default.
        fmt: Format enum, string, or ``None`` for the session default.
        raise_on_error: Raise on invalid combinations.

    Returns:
        A ``(Backend, Format)`` tuple of validated values.

    Raises:
        ImportError: Backend package missing.
        ValueError: Version too old or format unsupported.
    """
    # Lazy import to avoid circular dependency (blp → backend → blp).
    from xbbg.blp import get_backend as _get_backend

    # Normalise backend.
    if backend is None:
        backend = _get_backend() or get_default_backend()
    elif isinstance(backend, str):
        backend = Backend(backend)

    # Normalise format.
    if fmt is None:
        fmt = Format.LONG
    elif isinstance(fmt, str):
        fmt = Format(fmt)

    # Validate.
    check_backend(backend, raise_on_error=raise_on_error)
    check_format_compatibility(backend, fmt, raise_on_error=raise_on_error)

    return backend, fmt
