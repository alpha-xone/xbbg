"""Behavioral boundaries for statistically supported benchmark percentiles."""

from __future__ import annotations

from benchmark_contracts import empirical_percentile
import pytest


def test_p95_requires_twenty_observations() -> None:
    assert empirical_percentile(range(19), 95) is None
    assert empirical_percentile(range(20), 95) is not None


def test_p99_requires_one_hundred_observations() -> None:
    assert empirical_percentile(range(99), 99) is None
    assert empirical_percentile(range(100), 99) is not None


def test_other_percentiles_are_rejected() -> None:
    with pytest.raises(ValueError):
        empirical_percentile([1.0] * 100, 90)
