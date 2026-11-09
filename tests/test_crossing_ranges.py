from datetime import datetime, timedelta, timezone
import pytest
from core import forecasting as fc
from core.multiwindow import prediction_interval
from interfaces.cli import _critical_range


@pytest.mark.parametrize('method', ['baseline', 'multiwindow'])
def test_ranges_preserve_single_estimates_and_use_selected_window(monkeypatch, method):
    settings=dict(fc.CONFIG['forecast'], method=method, short_window_minutes=1)
    monkeypatch.setitem(fc.CONFIG,'forecast',settings)
    origin=datetime(2026,1,1,tzinfo=timezone.utc)
    rows=[dict(recorded_at=(origin+timedelta(seconds=i*5)).isoformat(),total_lag=i*10+(i%3)) for i in range(30)]
    before=fc.fit_forecast(rows,'c','g','t')
    result=fc.fit_configured_forecast(rows,'c','g','t')
    selected=rows[-13:] if result.get('window_used')=='short' else rows
    if method=='baseline':
        assert {key:result[key] for key in before}==before
    for name in ['warning','critical']:
        assert (result[f'eta_{name}_formula_low_sec'],result[f'eta_{name}_formula_high_sec'])==prediction_interval(selected,fc.CONFIG['alerts'][name+'_threshold'])
    assert result['eta_low_sec']==result['eta_critical_low_sec']
    assert result['eta_high_sec']==result['eta_critical_high_sec']


def test_range_display_retains_open_ended_and_missing_distinction():
    assert _critical_range(dict(eta_critical_low_sec=240,eta_critical_high_sec=420))=='4.0 to 7.0 min'
    assert _critical_range(dict(eta_critical_low_sec=240,eta_critical_high_sec=None))=='4.0 min or later'
    assert _critical_range({})=='Range unavailable'


def test_calibration_missing_file_and_boundaries(monkeypatch, tmp_path):
    from core import range_calibration as rc
    rc.load.cache_clear()
    monkeypatch.setattr(rc, "PATH", tmp_path / "missing.yml")
    f=dict(enough_data=True, eta_critical_sec=300, eta_critical_low_sec=299, eta_critical_high_sec=301)
    result=rc.apply(dict(f), "baseline", {})
    assert result["eta_critical_low_sec"] is None
    assert result["eta_critical_formula_low_sec"] == 299
    rc.load.cache_clear()


def test_frozen_runtime_factors_and_measured_coverage():
    import json, yaml
    from pathlib import Path
    from core import range_calibration as rc
    rc.load.cache_clear()
    from core.range_groups import factor_for
    frozen=yaml.safe_load(Path("bench/range_calibration_v2.yml").read_text())
    runtime=rc.load()
    assert runtime["modes"] == frozen["modes"]
    runtime_results=json.loads(rc.RESULTS_PATH.read_text())
    evaluation=json.loads(Path(runtime_results["source"]["path"]).read_text())
    import hashlib
    assert runtime_results['source']['sha256'] == hashlib.sha256(Path(runtime_results['source']['path']).read_bytes()).hexdigest()
    rc.measured_coverage.cache_clear()
    for mode in ("baseline", "multiwindow"):
        for eta in (300,301,900,901,1800):
            result=rc.apply(dict(enough_data=True,eta_critical_sec=eta,confidence='HIGH',window_seconds=600),mode,{})
            factor=factor_for(frozen['modes'][mode],eta,'HIGH',600)
            assert result["eta_critical_low_sec"] == factor["q05"]*eta
            assert result["range_coverage"] == evaluation["pooled_coverage"][mode]
            assert f'{evaluation["pooled_coverage"][mode]*100:.1f}%' in result['range_tooltip']
        beyond=rc.apply(dict(enough_data=True,eta_critical_sec=1801),mode,{})
        assert beyond['eta_low_sec'] is beyond['eta_high_sec'] is None
        assert beyond['range_status'] == 'beyond_30_minutes'
        assert _critical_range(beyond) == 'more than 30 min'
    assert rc.apply(dict(enough_data=True,eta_critical_sec=-2),"baseline",{})["eta_low_sec"] is None


def test_live_history_group_uses_selected_window_and_keeps_points():
    from core import range_calibration as rc
    early=dict(enough_data=True,eta_critical_sec=177,confidence='HIGH',window_seconds=299)
    established=dict(early,window_seconds=300)
    a=rc.apply(dict(early),'multiwindow',{})
    b=rc.apply(dict(established),'multiwindow',{})
    assert a['eta_critical_high_sec'] > b['eta_critical_high_sec']
    assert {k:a[k] for k in early} == early
    assert {k:b[k] for k in established} == established


@pytest.mark.parametrize('method',['baseline','multiwindow'])
def test_beyond_30_minutes_api_cli_and_metrics_omit_range(monkeypatch,method):
    from core import range_calibration as rc
    from interfaces import web
    from fastapi.testclient import TestClient
    key=('dev','g','orders')
    f=rc.apply(dict(enough_data=True,cluster_name='dev',group_id='g',topic='orders',
                    eta_critical_sec=1801,eta_critical_low_sec=1700,eta_critical_high_sec=1900),method,{})
    monkeypatch.setattr(web,'cached_forecast_display',lambda:[f])
    monkeypatch.setattr(web,'_forecast_advice_snapshot',({key:f},[]))
    monkeypatch.setattr(web,'_status_rows',lambda:[dict(cluster_name='dev',group_id='g',topic='orders',status='OK',total_lag=10)])
    monkeypatch.setattr(web,'_cluster_statuses',{})
    client=TestClient(web.app)
    value=client.get('/api/forecast').json()['forecasts'][0]
    assert value['eta_critical_sec'] == 1801
    assert value['eta_critical_low_sec'] is value['eta_critical_high_sec'] is None
    assert value['eta_critical_formula_low_sec'] == 1700
    assert _critical_range(value) == 'more than 30 min'
    output=client.get('/metrics').text
    assert 'khm_forecast_critical_eta_seconds{cluster="dev",group="g",topic="orders"} 1801' in output
    assert 'khm_forecast_critical_eta_low_seconds{' not in output
    assert 'khm_forecast_critical_eta_high_seconds{' not in output
