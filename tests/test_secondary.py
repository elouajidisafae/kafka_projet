"""Hand-computed cases for post-baseline secondary metrics."""
from datetime import datetime, timezone
import pytest
from bench.secondary import secondary_metrics


def iso(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def test_phase_boundary_and_warning_interpolation():
    history=[dict(recorded_at=iso(0),total_lag=0),dict(recorded_at=iso(100),total_lag=200)]
    forecasts=[dict(id=str(i),recorded_at=iso(t),eta_critical_sec=50-t,confidence='HIGH')
               for i,t in enumerate([10,20,60])]
    advice=[dict(recorded_at=iso(t),rule='scale',priority='MEDIUM') for t in [11,61]]
    metadata=dict(started_at=iso(0),rates=dict(fill_seconds=60))
    metrics=secondary_metrics(dict(pattern='flat_high'),history,forecasts,advice,metadata,40,100)
    assert metrics['S2_T_warn']==20
    assert metrics['S2_T_critical']==50
    assert metrics['S2_critical_minus_warning']==30
    assert metrics['S2_critical_minus_advice']==39
    assert metrics['S2_warning_minus_advice']==9
    assert metrics['S3_first_advice_eta_error_seconds']==0
    assert metrics['S1_fill_advice_fraction']==.5
    assert metrics['S1_after_fill_advice_fraction']==1


def test_burst_phases_use_generator_metadata_and_exclude_during_burst():
    history=[dict(recorded_at=iso(0),total_lag=0),dict(recorded_at=iso(100),total_lag=200)]
    forecasts=[dict(id=str(i),recorded_at=iso(t),eta_critical_sec=eta,confidence='HIGH')
               for i,(t,eta) in enumerate([(10,45),(20,100),(30,15)])]
    metrics=secondary_metrics(dict(pattern='burst'),history,forecasts,[],
        dict(started_at=iso(0),rates=dict(burst_at=20,burst_seconds=10)),40,100)
    assert metrics['S5_before_M2_0-15']==5
    assert metrics['S5_after_M2_0-15']==-5
    assert metrics['S4_abs_error_0-5_HIGH']==5
    assert metrics['S3_first_advice_eta_error_seconds'] is None


def test_statistics_use_configured_database(tmp_path, monkeypatch):
    from core import db, stats
    monkeypatch.setattr(db, "DB_PATH", tmp_path/"separate"/"history.db")
    db.init_db()
    db.save_lag("bench", "g", "orders", 1200, "WARNING", "STABLE")
    result = stats.get_global_stats()
    assert result["meta"]["total_records"] == 1
    assert result["cluster_summary"][0]["total_lag"] == 1200
