"""Live/offline equivalence, interval persistence and operational rule checks."""
from datetime import datetime, timezone
import sqlite3

import pytest

from core import db, forecasting, recommender
from bench.variants import V3
from bench.splits import select_repetitions
from test_variants import records, config
from test_golden_output import golden_database, FIXTURES, canonical


@pytest.mark.parametrize('values', [
    [10*i+(-1)**i*20 for i in range(400)],
    [min(i*100,6000) for i in range(500)],
    [i*2 if i<350 else 700+(i-350)*100 for i in range(400)],
    [100+(i%2)*10 for i in range(400)],
    [1,2,3],
])
def test_live_matches_frozen_offline_v3(monkeypatch, values):
    cfg=config()
    cfg['forecast'].update(method='multiwindow',short_window_minutes=15,agreement_tolerance=1.0,interval_level=.9)
    monkeypatch.setattr(forecasting,'CONFIG',cfg)
    rows=records(values)
    expected=V3(cfg).fit(rows,datetime.now(timezone.utc),cfg['alerts'])
    actual=forecasting.fit_configured_forecast(rows,'c','g','t')
    assert {k:actual[k] for k in expected}==expected
    if len(values)==400 and values[-1]>5000:
        assert actual['window_used']=='short' and actual['regime_change']
    for p in actual.get('prediction_band',[]):
        assert p['lower'] <= p['mean'] <= p['upper']
    cfg['forecast']['method']='baseline'
    assert forecasting.fit_configured_forecast(rows,'c','g','t')==forecasting.fit_forecast(rows,'c','g','t')


@pytest.mark.parametrize('state',['EMPTY','DEAD'])
def test_stranded_suppresses_scale(monkeypatch,state):
    monkeypatch.setattr(recommender,'CONFIG',config())
    f=dict(confidence='HIGH',slope_per_min=60,r_squared=.99,eta_critical_min=10,eta_critical_sec=600)
    result=recommender.analyze_row('c','g','t',2000,state,'INCREASING',f,consumer_count=0,partition_count=3)
    assert [r['id'].rsplit('-',1)[-1] for r in result]==['stranded']


@pytest.mark.parametrize('high,text',[(None,'range 2.0 min or later'),(240,'range 2.0–4.0 min')])
def test_interval_advice_and_short_trend_gate(monkeypatch,high,text):
    cfg=config(); cfg['forecast']['show_range']=True
    monkeypatch.setattr(recommender,'CONFIG',cfg)
    f=dict(method='multiwindow',confidence='HIGH',slope_per_min=60,r_squared=.99,
           eta_critical_min=3,eta_critical_sec=180,eta_low_sec=120,eta_high_sec=high)
    def rules(consumers):
        return recommender.analyze_row('c','g','t',2000,'STABLE','INCREASING',f,consumer_count=consumers,partition_count=3)
    assert text in rules(1)[0]['advice']
    f['advice_eligible']=False
    assert not rules(1) and not rules(3)


def test_nullable_migration_preserves_old_rows_and_persists_interval(golden_database):
    baseline=next(f for f in forecasting.forecast_all() if f.get('enough_data'))
    db.save_forecast(baseline)
    new_columns=('method','window_used','slope_short','slope_long','regime_change','eta_low_sec','eta_high_sec')
    with sqlite3.connect(golden_database) as conn:
        legacy_id=conn.execute('SELECT max(id) FROM forecast_log').fetchone()[0]
        for name in new_columns:
            conn.execute(f'ALTER TABLE forecast_log DROP COLUMN {name}')
    db.init_db()
    db.init_db()
    with sqlite3.connect(golden_database) as conn:
        columns={r[1] for r in conn.execute('PRAGMA table_info(forecast_log)')}
        assert {'method','window_used','slope_short','slope_long','regime_change','eta_low_sec','eta_high_sec'} <= columns
        assert conn.execute('SELECT '+','.join(new_columns)+' FROM forecast_log WHERE id=?',(legacy_id,)).fetchone()==(None,)*7
    forecasting.CONFIG['forecast'].update(method='multiwindow',short_window_minutes=15,agreement_tolerance=1.0)
    f=next(f for f in forecasting.forecast_all() if f.get('enough_data'))
    f['eta_high_sec']=None
    db.save_forecast(f,input_ids=[1,2,3])
    with sqlite3.connect(golden_database) as conn:
        row=conn.execute('SELECT method,window_used,slope_short,slope_long,regime_change,eta_low_sec,eta_high_sec,input_ids_json FROM forecast_log ORDER BY id DESC LIMIT 1').fetchone()
    assert row==tuple(f.get(k) for k in ('method','window_used','slope_short','slope_long','regime_change','eta_low_sec','eta_high_sec'))+('[1, 2, 3]',)


@pytest.mark.parametrize('name,call', [('forecasts',forecasting.forecast_all),('recommendations',recommender.get_recommendations)])
def test_multiwindow_golden(golden_database,name,call):
    for module in (db,forecasting,recommender):
        module.CONFIG['forecast'].update(method='multiwindow',short_window_minutes=15,agreement_tolerance=1.0,interval_level=.9,show_range=True)
    expected=(FIXTURES/'multiwindow_expected'/f'{name}.json').read_text(encoding='utf-8')
    assert canonical(call())==expected


def test_smoke_guard_cannot_bypass_heldout_boundary():
    manifest=dict(profile='baseline',config={'forecast':{'method':'multiwindow'}},repetitions=[{'wave':1}])
    with pytest.raises(ValueError,match='smoke recording'):
        select_repetitions(manifest,'smoke',True)
    manifest['profile']='smoke'
    assert select_repetitions(manifest,'smoke',True)[0]==manifest['repetitions']
