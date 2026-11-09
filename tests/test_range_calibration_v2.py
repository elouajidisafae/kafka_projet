"""Calibration selection checks use synthetic rows, never heldout benchmark results."""
import pytest
from bench.range_calibration_v2 import bounds, fit_factors, score
from core.range_groups import factor_for, group_key, history_bin, horizon


def row(remaining=100, predicted=100, quality="HIGH", window_seconds=299):
    return dict(remaining=remaining, predicted=predicted, quality=quality, window_seconds=window_seconds)


def test_live_group_boundaries_and_out_of_domain():
    assert [horizon(x) for x in (0, 300, 301, 900, 901, 1800, 1801)] == [None, "0-5", "5-15", "5-15", "15-30", "15-30", None]
    assert [history_bin(x) for x in (299, 300, 1799, 1800)] == ["under-5", "5-30", "5-30", "30+"]
    assert group_key("C3", 300, "HIGH", 300) == "0-5|HIGH|5-30"
    assert group_key("C3", 300, "HIGH", None) is None


@pytest.mark.parametrize("count", [199, 200])
def test_sparse_groups_fall_back_at_exact_200_boundary(count):
    train = [row(remaining=100)]*count + [row(remaining=1000, quality="LOW")]*250
    model = fit_factors(train, "C1")
    factor = factor_for(model, 100, "HIGH", 299)
    expected = model["horizon_factors"]["0-5"] if count < 200 else model["groups"]["0-5|HIGH"]
    assert factor == expected
    assert bounds(row(), model) == (expected["q05"]*100, expected["q95"]*100)
    assert bounds(row(predicted=1801), model) == (None, None)


def test_no_range_is_uncovered_not_a_zero_width_observation():
    measured = score([row(), row(predicted=2000)], lambda r: (50, 150) if r["predicted"] == 100 else (None, None))
    assert measured == dict(n=2, range_n=1, covered=1, coverage=.5, shown_coverage=1, median_width_sec=100)


def test_unknown_live_group_falls_back_without_workload_identity():
    model = fit_factors([row()]*250, "C3")
    assert factor_for(model, 100, "UNKNOWN", None) == model["horizon_factors"]["0-5"]
