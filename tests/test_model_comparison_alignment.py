from datetime import datetime, timedelta, timezone
import pytest
from bench.model_comparison_alignment import aligned_model, false_crossing


def test_burst_alignment_reuses_end_of_burst_not_onset():
    base=datetime(2026,1,1,tzinfo=timezone.utc)
    stamp=lambda seconds:(base+timedelta(seconds=seconds)).isoformat()
    inputs={"g":dict(rep={"group_id":"g","pattern":"burst"},
        metadata={"started_at":stamp(0),"rates":{"burst_at":10,"burst_seconds":10}},
        history=[{"recorded_at":stamp(0),"total_lag":0},{"recorded_at":stamp(100),"total_lag":100}])}
    forecasts=[dict(id=str(i),group_id="g",recorded_at=stamp(seconds),eta_critical_sec=eta,
                     confidence="NOT_APPLICABLE",enough_data=True)
               for i,(seconds,eta) in enumerate([(15,1000),(20,90),(30,80)])]
    result=aligned_model(inputs,forecasts,{"warning_threshold":50,"critical_threshold":100})
    assert result["table"][0]["horizon"]=="after-burst 0-15"
    assert result["table"][0]["median"]==10


def test_false_crossing_uses_same_ungated_eta_definition_for_v2():
    rows=[dict(eta_critical_sec=eta,enough_data=True,advice_eligible=False) for eta in [-1,-2,0,100,1800,1801]]
    rows += [dict(eta_critical_sec=None,enough_data=False),dict(eta_critical_sec=100,enough_data=False)]
    result=false_crossing(rows)
    assert result["available"]==6 and result["missing"]==2
    assert result["predicted"]==2 and result["fraction"]==pytest.approx(1/3)
