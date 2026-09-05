"""Shared benchmark harness for xbbg vs polars-bloomberg.

Provides BenchmarkResult dataclass and benchmark_func() utility used
by the individual bench_*.py scripts.

Data usage is minimal - each test uses 1-3 data points, then repeats
the same query multiple times to measure performance (cached/warm).

Total estimated data points: ~15-20 per full run.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import statistics
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# Single ticker/field for minimal data usage
TICKER = "IBM US Equity"
FIELD = "PX_LAST"

# For BDH - just 3 trading days
BDH_START = "2025-01-02"
BDH_END = "2025-01-06"

# Number of timing iterations (reuses cached data)
ITERATIONS = 5


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    library: str
    first_call_same_process_ms: float
    warm_mean_ms: float  # Mean of subsequent calls
    warm_std_ms: float  # Std dev of subsequent calls
    iterations: int
    data_shape: tuple


def benchmark_func(name: str, library: str, func: Callable, iterations: int = ITERATIONS) -> BenchmarkResult:
    """Benchmark a first same-process call and subsequent warm calls."""
    start = time.perf_counter()
    result = func()
    first_call_same_process_ms = (time.perf_counter() - start) * 1000

    # Get shape
    if hasattr(result, "shape"):
        shape = result.shape
    elif hasattr(result, "__len__"):
        shape = (len(result),)
    else:
        shape = (1,)

    # Warm iterations
    warm_times = []
    for _ in range(iterations - 1):
        start = time.perf_counter()
        func()
        warm_times.append((time.perf_counter() - start) * 1000)

    return BenchmarkResult(
        name=name,
        library=library,
        first_call_same_process_ms=first_call_same_process_ms,
        warm_mean_ms=statistics.mean(warm_times) if warm_times else first_call_same_process_ms,
        warm_std_ms=statistics.stdev(warm_times) if len(warm_times) > 1 else 0,
        iterations=iterations,
        data_shape=shape,
    )


def print_comparison(xbbg_result: BenchmarkResult, plbbg_result: BenchmarkResult):
    """Print side-by-side comparison."""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"  {xbbg_result.name}")
    logger.info(f"{'=' * 60}")
    logger.info(f"  {'Metric':<20} {'xbbg':>15} {'polars-bbg':>15} {'Ratio':>10}")
    logger.info(f"  {'-' * 60}")

    warm_ratio = xbbg_result.warm_mean_ms / plbbg_result.warm_mean_ms if plbbg_result.warm_mean_ms > 0 else 0

    logger.info(
        f"  {'First call, same process (ms)':<30} "
        f"{xbbg_result.first_call_same_process_ms:>15.2f} "
        f"{plbbg_result.first_call_same_process_ms:>15.2f} {'n/a':>10}"
    )
    logger.info(
        "  First-call ratio withheld: xbbg may lazily initialize its process-global "
        "engine, while BQuery is established before timing."
    )
    logger.info(
        f"  {'Warm mean (ms)':<20} {xbbg_result.warm_mean_ms:>15.2f} {plbbg_result.warm_mean_ms:>15.2f} {warm_ratio:>10.2f}x"
    )
    logger.info(f"  {'Warm std (ms)':<20} {xbbg_result.warm_std_ms:>15.2f} {plbbg_result.warm_std_ms:>15.2f}")
    logger.info(f"  {'Data shape':<20} {xbbg_result.data_shape!s:>15} {plbbg_result.data_shape!s:>15}")

    winner = "xbbg" if warm_ratio < 1 else "polars-bbg"
    speedup = max(warm_ratio, 1 / warm_ratio) if warm_ratio > 0 else 0
    logger.info(f"\n  Winner: {winner} ({speedup:.1f}x faster)")


def run_benchmarks():
    """Run all benchmarks."""
    logger.info("\n" + "=" * 60)
    logger.info("  XBBG vs POLARS-BLOOMBERG BENCHMARK")
    logger.info("=" * 60)
    logger.info("\n  Config:")
    logger.info(f"    Ticker: {TICKER}")
    logger.info(f"    Field: {FIELD}")
    logger.info(f"    BDH range: {BDH_START} to {BDH_END}")
    logger.info(f"    Iterations per test: {ITERATIONS}")
    logger.info("\n  Estimated data points: ~15-20 total")

    results = []

    # Import libraries
    import polars_bloomberg as plbbg

    import xbbg

    # Use polars-bloomberg with context manager
    with plbbg.BQuery() as bquery:
        # ============================================================
        # BDP - Reference Data (1 ticker, 1 field = 1 data point)
        # ============================================================
        logger.info("\n\n  Running BDP benchmark...")

        xbbg_bdp = benchmark_func("BDP (1 ticker, 1 field)", "xbbg", lambda: xbbg.bdp(TICKER, FIELD))

        plbbg_bdp = benchmark_func(
            "BDP (1 ticker, 1 field)",
            "polars-bloomberg",
            lambda: bquery.bdp([TICKER], [FIELD]),
        )

        print_comparison(xbbg_bdp, plbbg_bdp)
        results.append((xbbg_bdp, plbbg_bdp))

        # ============================================================
        # BDP Multi - Reference Data (3 tickers, 2 fields = 6 data points)
        # ============================================================
        logger.info("\n\n  Running BDP Multi benchmark...")

        tickers_multi = ["IBM US Equity", "AAPL US Equity", "MSFT US Equity"]
        fields_multi = ["PX_LAST", "VOLUME"]

        xbbg_bdp_multi = benchmark_func(
            "BDP (3 tickers, 2 fields)",
            "xbbg",
            lambda: xbbg.bdp(tickers_multi, fields_multi),
        )

        plbbg_bdp_multi = benchmark_func(
            "BDP (3 tickers, 2 fields)",
            "polars-bloomberg",
            lambda: bquery.bdp(tickers_multi, fields_multi),
        )

        print_comparison(xbbg_bdp_multi, plbbg_bdp_multi)
        results.append((xbbg_bdp_multi, plbbg_bdp_multi))

        # ============================================================
        # BDH - Historical Data (1 ticker, 1 field, ~3 days = 3 data points)
        # ============================================================
        logger.info("\n\n  Running BDH benchmark...")

        from datetime import datetime

        bdh_start_dt = datetime.strptime(BDH_START, "%Y-%m-%d")
        bdh_end_dt = datetime.strptime(BDH_END, "%Y-%m-%d")

        xbbg_bdh = benchmark_func(
            "BDH (1 ticker, 3 days)",
            "xbbg",
            lambda: xbbg.bdh(TICKER, FIELD, BDH_START, BDH_END),
        )

        plbbg_bdh = benchmark_func(
            "BDH (1 ticker, 3 days)",
            "polars-bloomberg",
            lambda: bquery.bdh([TICKER], [FIELD], bdh_start_dt, bdh_end_dt),
        )

        print_comparison(xbbg_bdh, plbbg_bdh)
        results.append((xbbg_bdh, plbbg_bdh))

        # ============================================================
        # BQL - Bloomberg Query Language (1 simple query)
        # Note: polars-bloomberg doesn't have bds(), so we test BQL instead
        # ============================================================
        logger.info("\n\n  Running BQL benchmark...")

        bql_expr = "get(px_last) for(['IBM US Equity'])"

        xbbg_bql = benchmark_func("BQL (simple query)", "xbbg", lambda: xbbg.bql(bql_expr))

        plbbg_bql = benchmark_func(
            "BQL (simple query)",
            "polars-bloomberg",
            lambda: bquery.bql(bql_expr),
        )

        print_comparison(xbbg_bql, plbbg_bql)
        results.append((xbbg_bql, plbbg_bql))

    # ============================================================
    # Summary
    # ============================================================
    logger.info("\n\n" + "=" * 60)
    logger.info("  SUMMARY")
    logger.info("=" * 60)

    xbbg_wins = sum(1 for x, p in results if x.warm_mean_ms < p.warm_mean_ms)
    plbbg_wins = len(results) - xbbg_wins

    logger.info(f"\n  xbbg wins: {xbbg_wins}/{len(results)}")
    logger.info(f"  polars-bloomberg wins: {plbbg_wins}/{len(results)}")

    # Overall speedup
    total_xbbg = sum(x.warm_mean_ms for x, _ in results)
    total_plbbg = sum(p.warm_mean_ms for _, p in results)
    overall_ratio = total_xbbg / total_plbbg if total_plbbg > 0 else 0

    logger.info("\n  Total warm time:")
    logger.info(f"    xbbg: {total_xbbg:.2f} ms")
    logger.info(f"    polars-bloomberg: {total_plbbg:.2f} ms")
    logger.info(f"    Ratio: {overall_ratio:.2f}x")

    overall_winner = "xbbg" if overall_ratio < 1 else "polars-bloomberg"
    overall_speedup = max(overall_ratio, 1 / overall_ratio)
    logger.info(f"\n  Overall winner: {overall_winner} ({overall_speedup:.1f}x faster)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_benchmarks()
