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
        assert (result[f'eta_{name}_low_sec'],result[f'eta_{name}_high_sec'])==prediction_interval(selected,fc.CONFIG['alerts'][name+'_threshold'])
    assert result['eta_low_sec']==result['eta_critical_low_sec']
    assert result['eta_high_sec']==result['eta_critical_high_sec']


def test_range_display_retains_open_ended_and_missing_distinction():
    assert _critical_range(dict(eta_critical_low_sec=240,eta_critical_high_sec=420))=='4.0 to 7.0 min'
    assert _critical_range(dict(eta_critical_low_sec=240,eta_critical_high_sec=None))=='4.0 min or later'
    assert _critical_range({})=='Range unavailable'
