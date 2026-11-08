from bench.stackcmp_coverage import coverage


def test_coverage_missing_predictions_stay_in_denominator():
    def row(second,eta): return dict(recorded_at=f"2026-01-01T00:00:{second:02d}+00:00",eta_critical_sec=eta)
    from bench.common import timestamp
    truth=timestamp("2026-01-01T00:00:30+00:00")
    result=coverage([row(5,10),row(10,None),row(15,-2),row(30,20)],truth)
    assert result==dict(eligible=3,positive=1,fraction=1/3)


def test_no_observed_crossing_is_not_applicable():
    assert coverage([dict(recorded_at="2026-01-01T00:00:00+00:00",eta_critical_sec=20)],None)["fraction"] is None
