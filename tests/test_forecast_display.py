"""Snapshot isolation for the web forecast, without changing model outputs."""
from datetime import datetime, timedelta, timezone

import pytest
from core import db, forecasting as fc
import numpy as np


@pytest.mark.parametrize('method', ['baseline', 'multiwindow'])
def test_display_snapshot_tracks_fit_not_newer_history(tmp_path, monkeypatch, method):
    monkeypatch.setattr(db, 'DB_PATH', tmp_path / 'display.db')
    monkeypatch.setattr(fc, '_cache', None)
    monkeypatch.setitem(fc.CONFIG, 'forecast', dict(fc.CONFIG['forecast'], method=method, persist_every_cycle=False))
    monkeypatch.setitem(fc.CONFIG, 'monitor', dict(fc.CONFIG['monitor'], refresh_interval=3600))
    db.init_db()
    now = datetime.now(timezone.utc)
    with db._get_connection() as conn:
        conn.executemany(
            'INSERT INTO lag_history(cluster_name,group_id,topic,total_lag,status,recorded_at) VALUES (?,?,?,?,?,?)',
            [('demo', 'g', 't', 100+i*200, 'OK', (now-timedelta(seconds=60-i*10)).isoformat()) for i in range(6)])
    original = fc.cached_forecast_all()
    first = fc.cached_forecast_display()[0]
    assert {k:v for k,v in first.items() if k != 'display'} == original[0]
    assert first['display']['origin'] == (now-timedelta(seconds=10)).isoformat()
    assert first['display']['history'][-1]['total_lag'] == first['current_lag']
    db.save_lag('demo', 'g', 't', 9000, 'WARNING')
    assert fc.cached_forecast_display()[0] == first
    first['display']['history'].clear()
    assert len(fc.cached_forecast_display()[0]['display']['history']) == 6
    fc.compute_cycle_forecasts('demo')  # scoped computations must not replace it
    assert len(fc.cached_forecast_display()[0]['display']['history']) == 6
    fc.compute_cycle_forecasts()
    updated = fc.cached_forecast_display()[0]
    assert updated['display']['origin'] != first['display']['origin']
    assert updated['display']['history'][-1]['total_lag'] == updated['current_lag'] == 9000
    assert 'display' not in fc.cached_forecast_all()[0]


@pytest.mark.parametrize('method', ['baseline', 'multiwindow'])
@pytest.mark.parametrize('values', [[100,250,400,550,700,850], [100,100,3100,3100,3100,3100], [2000,1700,1400,1100,800,500]])
def test_curve_uses_existing_fit_and_table_rounding(monkeypatch, method, values):
    monkeypatch.setitem(fc.CONFIG, 'forecast', dict(fc.CONFIG['forecast'], method=method, short_window_minutes=.6))
    origin = datetime(2026, 10, 7, tzinfo=timezone.utc)
    rows = [dict(recorded_at=(origin+timedelta(seconds=s)).isoformat(), total_lag=v)
            for s,v in zip([-89,-87,-60,-17,-5,0], values)]
    normal = fc.fit_configured_forecast(rows, 'demo', 'g', 't')
    display = {}
    result = fc.fit_configured_forecast(rows, 'demo', 'g', 't', display=display)
    assert result == normal
    selected = rows[-3:] if result.get('window_used') == 'short' else rows
    ts = np.array([fc._parse_timestamp(r['recorded_at']) for r in selected])
    slope, intercept = np.polyfit(ts-ts[0], [r['total_lag'] for r in selected], 1)
    curve = display['curve']
    assert curve[0]['seconds'] == 0 and curve[-1]['seconds'] == 900
    assert max(b['seconds']-a['seconds'] for a,b in zip(curve,curve[1:])) <= 50
    for p in curve:
        assert p['lag'] == max(0,int(slope*(ts[-1]-ts[0]+p['seconds'])+intercept))
    assert next(p['lag'] for p in curve if p['seconds']==300) == result['predicted_lag_5min']
    assert curve[-1]['lag'] == result['predicted_lag_15min']


def test_curve_follows_selected_short_window(monkeypatch):
    monkeypatch.setitem(fc.CONFIG, 'forecast', dict(fc.CONFIG['forecast'], method='multiwindow', short_window_minutes=1))
    origin = datetime(2026, 10, 7, tzinfo=timezone.utc)
    rows = [dict(recorded_at=(origin+timedelta(seconds=i*10)).isoformat(), total_lag=100 if i<15 else 100+(i-15)*200)
            for i in range(22)]
    display = {}
    result = fc.fit_configured_forecast(rows, 'demo', 'g', 't', display=display)
    assert result['window_used'] == 'short'
    assert next(p['lag'] for p in display['curve'] if p['seconds']==300) == result['predicted_lag_5min']
    assert display['curve'][-1]['lag'] == result['predicted_lag_15min']
