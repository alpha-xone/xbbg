"""Offline benchmarks for native xbbg binding handoff conversions.

The fixture setup intentionally builds native ``xbbg.ArrowTable`` objects once and
keeps that setup outside the measured sections.  Timed lanes therefore isolate
Rust-native Arrow carrier handoff into Python dataframe/Arrow objects, plus the
explicit materialising ``to_pylist()`` slow path for contrast.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import importlib
import importlib.util
import ctypes
import json
import logging
import os
import shutil
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
from typing import Any

sys.stdout.reconfigure(encoding="utf-8")

if "BLPAPI_ROOT" not in os.environ:
    repo_root = Path(__file__).resolve().parents[2]
    for bundled_sdk in (
        repo_root / "vendor" / "blpapi-sdk" / "3.26.5.1",
        repo_root / "vendor" / "blpapi-sdk" / "3.26.5.1" / "Darwin",
        repo_root / "js-xbbg" / "lib",
    ):
        if (bundled_sdk / "libblpapi3_64.so").is_file() or (bundled_sdk / "lib" / "libblpapi3_64.so").is_file():
            os.environ["BLPAPI_ROOT"] = str(bundled_sdk)
            break

known_sdk_lib = Path(__file__).resolve().parents[2] / "vendor" / "blpapi-sdk" / "3.26.5.1" / "Darwin" / "libblpapi3_64.so"
known_runtime_lib = Path(__file__).resolve().parents[2] / ".pixi" / "envs" / "default" / "lib" / "libblpapi3_64.so"
if known_sdk_lib.is_file() and known_runtime_lib.parent.is_dir() and not known_runtime_lib.exists():
    try:
        known_runtime_lib.symlink_to(known_sdk_lib)
    except OSError:
        try:
            shutil.copy2(known_sdk_lib, known_runtime_lib)
        except OSError:
            pass

sdk_root = os.environ.get("BLPAPI_ROOT")
if sdk_root:
    sdk_path = Path(sdk_root)
    sdk_lib = sdk_path / "libblpapi3_64.so"
    if not sdk_lib.is_file():
        sdk_lib = sdk_path / "lib" / "libblpapi3_64.so"
    if not sdk_lib.is_file():
        sdk_lib = sdk_path / "Darwin" / "libblpapi3_64.so"
    if sdk_lib.is_file():
        runtime_libs = (
            Path(sys.executable).parent.parent / "lib" / "libblpapi3_64.so",
            Path(__file__).resolve().parents[2] / ".pixi" / "envs" / "default" / "lib" / "libblpapi3_64.so",
        )
        for runtime_lib in runtime_libs:
            if runtime_lib.exists() or not runtime_lib.parent.is_dir():
                continue
            try:
                runtime_lib.symlink_to(sdk_lib)
            except OSError:
                try:
                    shutil.copy2(sdk_lib, runtime_lib)
                except OSError:
                    pass
        try:
            ctypes.CDLL(str(sdk_lib), mode=ctypes.RTLD_GLOBAL)
        except OSError:
            pass

if sys.platform == "darwin" and os.environ.get("XBBG_HANDOFF_DYLD_REEXEC") != "1":
    sdk_root = os.environ.get("BLPAPI_ROOT")
    if sdk_root:
        sdk_path = Path(sdk_root)
        sdk_lib_dir = sdk_path
        if not (sdk_lib_dir / "libblpapi3_64.so").is_file():
            sdk_lib_dir = sdk_path / "lib"
        if not (sdk_lib_dir / "libblpapi3_64.so").is_file() and (sdk_path / "Darwin" / "libblpapi3_64.so").is_file():
            sdk_lib_dir = sdk_path / "Darwin"
        if (sdk_lib_dir / "libblpapi3_64.so").is_file():
            existing_dirs = [item for item in os.environ.get("DYLD_LIBRARY_PATH", "").split(":") if item]
            if str(sdk_lib_dir) not in existing_dirs:
                env = os.environ.copy()
                env["DYLD_LIBRARY_PATH"] = ":".join([str(sdk_lib_dir), *existing_dirs])
                env["XBBG_HANDOFF_DYLD_REEXEC"] = "1"
                os.execvpe(sys.executable, [sys.executable, *sys.argv], env)

import xbbg
from xbbg.backend import Backend, convert_backend_frame

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).resolve().parent / "results"
DEFAULT_SHAPES: tuple[tuple[int, int], ...] = ((100, 5), (10_000, 10), (100_000, 10))
DEFAULT_WARMUP = 2
DEFAULT_ITERATIONS = 10
QUICK_WARMUP = 1
QUICK_ITERATIONS = 3


@dataclass(frozen=True)
class BenchmarkResult:
    """Result from one offline handoff benchmark lane."""

    package: str
    operation: str
    cold_start_ms: float
    warm_mean_ms: float
    warm_median_ms: float
    warm_p95_ms: float
    warm_p99_ms: float
    warm_std_ms: float
    memory_peak_mb: float
    data_shape: tuple[int, int]
    iterations: int
    warm_min_ms: float
    warmup_iterations: int
    offline: bool
    scenario: str
    rows: int
    columns: int
    build_profile: str
    extension_path: str | None


@dataclass(frozen=True)
class FixtureTable:
    """Pre-built native table for one shape."""

    rows: int
    columns: int
    native: Any
    setup_ms: float


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline xbbg native handoff benchmark")
    parser.add_argument("--quick", action="store_true", help="Run fewer iterations for smoke checks")
    parser.add_argument("--iterations", type=positive_int, default=None, help="Measured iterations per lane")
    parser.add_argument("--warmup", type=non_negative_int, default=None, help="Warmup iterations per lane")
    parser.add_argument(
        "--shape",
        action="append",
        default=None,
        metavar="ROWSxCOLS",
        help="Shape to benchmark; may be repeated (default: 100x5, 10000x10, 100000x10)",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=RESULTS_DIR,
        help="Directory for JSON results (default: py-xbbg/benchmarks/results)",
    )
    parser.add_argument("--json", action="store_true", help="Print only the JSON result document")
    return parser.parse_args(argv)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_shape(value: str) -> tuple[int, int]:
    normalized = value.lower().replace("_", "")
    try:
        rows_text, columns_text = normalized.split("x", 1)
        rows = int(rows_text)
        columns = int(columns_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid shape {value!r}; expected ROWSxCOLS") from exc
    if rows <= 0 or columns <= 0:
        raise argparse.ArgumentTypeError("shape dimensions must be positive")
    return rows, columns


def selected_shapes(shape_args: Sequence[str] | None) -> tuple[tuple[int, int], ...]:
    if not shape_args:
        return DEFAULT_SHAPES
    return tuple(parse_shape(shape) for shape in shape_args)


def detect_extension_profile() -> tuple[str, str | None]:
    module = sys.modules.get("xbbg._core")
    module_file = getattr(module, "__file__", None)
    extension_path = str(module_file) if module_file else None
    parts = {part.lower() for part in Path(extension_path).parts} if extension_path else set()
    if "debug" in parts or (extension_path and "debug" in extension_path.lower()):
        return "debug", extension_path
    if "release" in parts or (extension_path and "release" in extension_path.lower()):
        return "release", extension_path
    return "unknown", extension_path


def column_name(index: int) -> str:
    kind = index % 4
    prefix = ("float", "int", "string", "timestamp")[kind]
    return f"{prefix}_{index:02d}"


def fixture_value(row_index: int, column_index: int) -> Any:
    kind = column_index % 4
    if kind == 0:
        return row_index * 0.25 + column_index / 10
    if kind == 1:
        return row_index * 10_000 + column_index
    if kind == 2:
        return f"SEC{row_index % 10_000:04d}:{column_index:02d}"
    base = datetime(2024, 1, 2, 9, 30)
    return base + timedelta(microseconds=row_index * 1_000 + column_index)


def build_rows(rows: int, columns: int) -> list[dict[str, Any]]:
    names = [column_name(index) for index in range(columns)]
    return [
        {name: fixture_value(row_index, column_index) for column_index, name in enumerate(names)}
        for row_index in range(rows)
    ]


def build_fixture(rows: int, columns: int) -> FixtureTable:
    started = time.perf_counter()
    native = xbbg.ArrowTable.from_pylist(build_rows(rows, columns))
    setup_ms = (time.perf_counter() - started) * 1000
    return FixtureTable(rows=rows, columns=columns, native=native, setup_ms=setup_ms)


def shape_of(result: Any, fallback: tuple[int, int]) -> tuple[int, int]:
    shape = getattr(result, "shape", None)
    if isinstance(shape, tuple) and len(shape) >= 2:
        return int(shape[0]), int(shape[1])
    num_rows = getattr(result, "num_rows", None)
    num_columns = getattr(result, "num_columns", None)
    if num_rows is not None and num_columns is not None:
        return int(num_rows), int(num_columns)
    if isinstance(result, list):
        return len(result), len(result[0]) if result and isinstance(result[0], dict) else fallback[1]
    return fallback


def percentile(sorted_values: Sequence[float], percentile_value: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int((percentile_value / 100) * len(sorted_values)))
    return sorted_values[index]


def round_ms(value: float) -> float:
    return round(value, 6)


def benchmark_conversion(
    *,
    fixture: FixtureTable,
    scenario: str,
    converter: Callable[[Any], Any],
    iterations: int,
    warmup: int,
    package_label: str,
    build_profile: str,
    extension_path: str | None,
) -> BenchmarkResult:
    durations: list[float] = []
    result: Any = None

    for _ in range(warmup):
        result = converter(fixture.native)

    tracemalloc.start()
    for _ in range(iterations):
        started = time.perf_counter()
        result = converter(fixture.native)
        durations.append((time.perf_counter() - started) * 1000)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    sorted_durations = sorted(durations)
    warm_mean = statistics.mean(durations) if durations else 0.0
    warm_median = statistics.median(durations) if durations else 0.0
    warm_std = statistics.stdev(durations) if len(durations) > 1 else 0.0
    data_shape = shape_of(result, (fixture.rows, fixture.columns))

    return BenchmarkResult(
        package=package_label,
        operation=f"handoff_offline:{scenario}:{fixture.rows}x{fixture.columns}",
        cold_start_ms=round_ms(durations[0] if durations else 0.0),
        warm_mean_ms=round_ms(warm_mean),
        warm_median_ms=round_ms(warm_median),
        warm_p95_ms=round_ms(percentile(sorted_durations, 95)),
        warm_p99_ms=round_ms(percentile(sorted_durations, 99)),
        warm_std_ms=round_ms(warm_std),
        memory_peak_mb=round(peak / 1024 / 1024, 6),
        data_shape=data_shape,
        iterations=iterations,
        warm_min_ms=round_ms(min(durations) if durations else 0.0),
        warmup_iterations=warmup,
        offline=True,
        scenario=scenario,
        rows=fixture.rows,
        columns=fixture.columns,
        build_profile=build_profile,
        extension_path=extension_path,
    )


def pyarrow_converter() -> Callable[[Any], Any]:
    pa = importlib.import_module("pyarrow")
    return pa.table


def pandas_converter(table: Any) -> Any:
    return convert_backend_frame(table, Backend.PANDAS)


def polars_converter(table: Any) -> Any:
    return convert_backend_frame(table, Backend.POLARS)


def to_pylist_converter(table: Any) -> list[dict[str, Any]]:
    return table.to_pylist()


def available_converters() -> Iterable[tuple[str, Callable[[Any], Any]]]:
    yield "native_to_pyarrow_c_stream", pyarrow_converter()
    yield "native_to_pandas_xbbg_backend", pandas_converter
    if importlib.util.find_spec("polars") is not None:
        yield "native_to_polars", polars_converter
    yield "native_to_pylist_slow_path", to_pylist_converter


def result_document(results: Sequence[BenchmarkResult], output_path: Path | None) -> dict[str, Any]:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "version": getattr(xbbg, "__version__", "unknown"),
        "timestamp": timestamp,
        "offline": True,
        "benchmark_file": Path(__file__).name,
        "output_path": str(output_path) if output_path is not None else None,
        "benchmarks": {
            "Binding Handoff - Offline": [asdict(result) for result in results],
        },
    }


def write_results(document: dict[str, Any], results_dir: Path) -> Path:
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp_short = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = results_dir / f"handoff_offline_{timestamp_short}.json"
    document["output_path"] = str(output_path)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(document, file, indent=2, default=str)
    latest_path = results_dir / "handoff_offline_latest.json"
    with latest_path.open("w", encoding="utf-8") as file:
        json.dump(document, file, indent=2, default=str)
    return output_path


def run(args: argparse.Namespace) -> tuple[list[BenchmarkResult], dict[str, Any], Path]:
    iterations = args.iterations if args.iterations is not None else (QUICK_ITERATIONS if args.quick else DEFAULT_ITERATIONS)
    warmup = args.warmup if args.warmup is not None else (QUICK_WARMUP if args.quick else DEFAULT_WARMUP)
    shapes = selected_shapes(args.shape)
    build_profile, extension_path = detect_extension_profile()
    package_label = f"xbbg-python-extension ({build_profile})"

    logger.info("=" * 70)
    logger.info("Offline Native Handoff Benchmark")
    logger.info("=" * 70)
    logger.info("Iterations: %s", iterations)
    logger.info("Warmup: %s", warmup)
    logger.info("Extension profile: %s", build_profile)
    if extension_path:
        logger.info("Extension path: %s", extension_path)

    converters = tuple(available_converters())
    results: list[BenchmarkResult] = []

    for rows, columns in shapes:
        logger.info("\nBuilding fixture %sx%s (setup excluded from timings)...", rows, columns)
        fixture = build_fixture(rows, columns)
        logger.info("  setup: %.2fms", fixture.setup_ms)
        for scenario, converter in converters:
            logger.info("  Running %s...", scenario)
            result = benchmark_conversion(
                fixture=fixture,
                scenario=scenario,
                converter=converter,
                iterations=iterations,
                warmup=warmup,
                package_label=package_label,
                build_profile=build_profile,
                extension_path=extension_path,
            )
            results.append(result)
            logger.info(
                "    median %.4fms, min %.4fms, peak %.2fMB",
                result.warm_median_ms,
                result.warm_min_ms,
                result.memory_peak_mb,
            )

    document = result_document(results, None)
    output_path = write_results(document, args.results_dir)
    logger.info("\nWrote JSON results: %s", output_path)
    return results, document, output_path


def main(argv: Sequence[str] | None = None) -> list[BenchmarkResult]:
    args = parse_args(argv)
    results, document, _output_path = run(args)
    if args.json:
        print(json.dumps(document, indent=2, default=str))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
