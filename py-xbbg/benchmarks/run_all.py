"""Run all benchmarks and generate comprehensive report.

This runs the complete benchmark suite comparing xbbg Rust against all
competing packages.

Data usage: ~200-350 Bloomberg data points total per run.

Results are saved per version:
- benchmark_v{version}.json (overwrites for same version)
- benchmark_v{version}_{timestamp}.json (timestamped archive)
- latest.json (symlink/copy to latest version)
"""

from __future__ import annotations
import argparse
import inspect

from datetime import datetime
import json
import logging
from pathlib import Path
import sys

logger = logging.getLogger(__name__)

# Create results directory
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def get_xbbg_version():
    """Get the current xbbg version.

    Returns:
        Version string (e.g., "1.0.0") or "unknown"
    """
    try:
        import xbbg

        if hasattr(xbbg, "__version__"):
            version_str = str(xbbg.__version__)
            return version_str
        else:
            # Try to get version from package metadata
            try:
                from importlib.metadata import version

                version_str = str(version("xbbg"))
                return version_str
            except Exception:
                return "unknown"
    except ImportError:
        return "unknown"


def run_benchmark_module(module_name: str):
    """Run a benchmark module and return results."""
    logger.info(f"\n{'=' * 70}")
    logger.info(f"Running {module_name}")
    logger.info(f"{'=' * 70}\n")

    try:
        module = __import__(module_name)
        if hasattr(module, "main"):
            main_func = module.main
            if inspect.signature(main_func).parameters:
                return main_func([])
            return main_func()
        else:
            logger.warning(f"{module_name} has no main() function")
            return []
    except Exception as e:
        logger.error(f"Error running {module_name}: {e}")
        import traceback

        traceback.print_exc()
        return []


def generate_markdown_report(all_results: dict, output_path: Path, version: str, timestamp: str):
    """Generate markdown report from results."""
    with output_path.open("w") as f:
        f.write("# xbbg Benchmark Results\n\n")
        f.write(f"**Version:** {version}\n")
        f.write(f"**Generated:** {timestamp}\n\n")
        f.write("---\n\n")

        for operation, results in all_results.items():
            f.write(f"## {operation}\n\n")

            if not results:
                f.write("*No results*\n\n")
                continue

            # Table header
            f.write("| Package | Cold Start (ms) | Warm Mean (ms) | Warm Std (ms) | Memory (MB) | Shape |\n")
            f.write("|---------|-----------------|----------------|---------------|-------------|-------|\n")

            # Find best warm mean for highlighting
            warm_means = [r.warm_mean_ms for r in results if hasattr(r, "warm_mean_ms")]
            best_warm = min(warm_means) if warm_means else 0

            for result in results:
                is_best = abs(result.warm_mean_ms - best_warm) < 0.01
                marker = " ✅" if is_best else ""

                f.write(
                    f"| {result.package}{marker} | "
                    f"{result.cold_start_ms:.2f} | "
                    f"{result.warm_mean_ms:.2f} | "
                    f"{result.warm_std_ms:.2f} | "
                    f"{result.memory_peak_mb:.2f} | "
                    f"{result.data_shape} |\n"
                )

            f.write("\n")

            # Calculate speedups
            rust_results = [r for r in results if "rust" in r.package.lower()]
            legacy_results = [r for r in results if "legacy" in r.package.lower()]

            if rust_results and legacy_results:
                rust_time = rust_results[0].warm_mean_ms
                legacy_time = legacy_results[0].warm_mean_ms
                speedup = legacy_time / rust_time if rust_time > 0 else 0

                f.write(f"**Speedup vs legacy:** {speedup:.2f}x faster\n\n")

            f.write("---\n\n")

        # Summary section
        f.write("## Summary\n\n")

        rust_total = 0
        legacy_total = 0
        pdblp_total = 0

        for results in all_results.values():
            for result in results:
                if "rust" in result.package.lower():
                    rust_total += result.warm_mean_ms
                elif "legacy" in result.package.lower():
                    legacy_total += result.warm_mean_ms
                elif "pdblp" in result.package.lower():
                    pdblp_total += result.warm_mean_ms

        f.write("**Total execution time (warm):**\n\n")
        f.write(f"- xbbg (Rust): {rust_total:.2f}ms\n")
        if legacy_total > 0:
            f.write(f"- xbbg (legacy): {legacy_total:.2f}ms ({legacy_total / rust_total:.2f}x slower)\n")
        if pdblp_total > 0:
            f.write(f"- pdblp: {pdblp_total:.2f}ms ({pdblp_total / rust_total:.2f}x slower)\n")

        f.write("\n")


def generate_json_report(all_results: dict, output_path: Path, version: str, timestamp: str):
    """Generate JSON report from results."""
    json_data = {
        "version": version,
        "timestamp": timestamp,
        "benchmarks": {},
    }

    for operation, results in all_results.items():
        entries = []
        for r in results:
            entry = {
                "package": r.package,
                "operation": r.operation,
                "cold_start_ms": r.cold_start_ms,
                "warm_mean_ms": r.warm_mean_ms,
                "warm_median_ms": r.warm_median_ms,
                "warm_p95_ms": r.warm_p95_ms,
                "warm_p99_ms": r.warm_p99_ms,
                "warm_std_ms": r.warm_std_ms,
                "memory_peak_mb": r.memory_peak_mb,
                "data_shape": r.data_shape,
                "iterations": r.iterations,
            }
            for extra in (
                "warm_min_ms",
                "warmup_iterations",
                "offline",
                "scenario",
                "rows",
                "columns",
                "build_profile",
                "extension_path",
            ):
                if hasattr(r, extra):
                    entry[extra] = getattr(r, extra)
            entries.append(entry)
        json_data["benchmarks"][operation] = entries

    with output_path.open("w") as f:
        json.dump(json_data, f, indent=2)


def parse_args(argv: list[str] | None = None):
    """Parse benchmark runner arguments."""
    parser = argparse.ArgumentParser(description="Run xbbg benchmark suite")
    parser.add_argument(
        "--offline",
        dest="include_offline",
        action="store_true",
        default=True,
        help="Include offline binding handoff benchmarks (default)",
    )
    parser.add_argument(
        "--no-offline",
        dest="include_offline",
        action="store_false",
        help="Skip offline binding handoff benchmarks",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    """Run all benchmarks and generate reports."""
    logger.info("=" * 70)
    logger.info("xbbg Comprehensive Benchmark Suite")
    logger.info("=" * 70)
    args = parse_args(argv)

    # Get version
    version = get_xbbg_version()
    logger.info(f"\nxbbg version: {version}")
    logger.info("\nRunning benchmarks with live Bloomberg data...")
    logger.info("Estimated live data usage: ~200-350 data points")
    logger.info("Offline binding handoff benchmark included: %s\n", args.include_offline)

    all_results = {}

    # Run each benchmark module
    benchmarks = [
        ("bench_bdp", "BDP - Reference Data"),
        ("bench_bdh", "BDH - Historical Data"),
        ("bench_bdib", "BDIB - Intraday Bars"),
        ("bench_bdtick", "BDTICK - Tick Data"),
        ("bench_bql", "BQL - Query Language"),
    ]
    if args.include_offline:
        benchmarks.append(("bench_handoff_offline", "Binding Handoff - Offline"))

    for module_name, description in benchmarks:
        try:
            results = run_benchmark_module(module_name)
            all_results[description] = results
        except Exception as e:
            logger.error(f"Failed to run {module_name}: {e}")
            all_results[description] = []

    # Generate reports with version-based naming
    timestamp_full = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp_short = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Version-specific files (OVERWRITES for same version)
    version_json = RESULTS_DIR / f"benchmark_v{version}.json"
    version_md = RESULTS_DIR / f"benchmark_v{version}.md"

    # Timestamped archive (NEVER overwritten)
    archive_json = RESULTS_DIR / f"benchmark_v{version}_{timestamp_short}.json"
    archive_md = RESULTS_DIR / f"benchmark_v{version}_{timestamp_short}.md"

    logger.info(f"\n\n{'=' * 70}")
    logger.info("Generating Reports")
    logger.info(f"{'=' * 70}\n")

    # Generate version-specific files (overwrites)
    generate_json_report(all_results, version_json, version, timestamp_full)
    logger.info(f"✓ Version JSON: {version_json}")

    generate_markdown_report(all_results, version_md, version, timestamp_full)
    logger.info(f"✓ Version MD:   {version_md}")

    # Generate timestamped archives
    import shutil

    shutil.copy(version_json, archive_json)
    shutil.copy(version_md, archive_md)
    logger.info(f"✓ Archive JSON: {archive_json}")
    logger.info(f"✓ Archive MD:   {archive_md}")

    # Update latest symlinks/copies
    latest_json = RESULTS_DIR / "latest.json"
    latest_md = RESULTS_DIR / "latest.md"

    if latest_json.exists():
        latest_json.unlink()
    if latest_md.exists():
        latest_md.unlink()

    try:
        latest_json.symlink_to(version_json.name)
        latest_md.symlink_to(version_md.name)
        logger.info("✓ Latest symlinks updated")
    except OSError:
        # Windows may not support symlinks, just copy
        shutil.copy(version_json, latest_json)
        shutil.copy(version_md, latest_md)
        logger.info("✓ Latest files copied (symlinks not supported)")

    logger.info(f"\n{'=' * 70}")
    logger.info("Benchmarks Complete!")
    logger.info(f"{'=' * 70}\n")
    logger.info("Results saved:")
    logger.info(f"  - Version-specific (overwrites): {version_md}")
    logger.info(f"  - Timestamped archive (keeps):   {archive_md}")
    logger.info("  - Latest:                        latest.md")
    logger.info("\nCommit these files to git for version tracking.")

    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
