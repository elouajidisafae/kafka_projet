"""Offline variants and selection boundary checks."""
from datetime import datetime, timezone
from copy import deepcopy
import pytest
from bench.replay import Baseline
from bench.variants import Variant,prediction_interval
from bench.splits import frozen_parameters,select_repetitions


def records(values,step=5):
    return [dict(id=i,cluster_name='c',group_id='g',topic='t',total_lag=v,
                 recorded_at=datetime.fromtimestamp(i*step,timezone.utc).isoformat()) for i,v in enumerate(values)]


def config():
    return dict(alerts=dict(warning_threshold=1000,critical_threshold=10000),forecast=dict(trend_deadband_msgs_per_sec=.05))


def test_v1_preserves_baseline_and_linear_interval():
    rows=records([10*i for i in range(100)])
    cfg=config(); now=datetime.now(timezone.utc)
    baseline=Baseline(cfg).fit(rows,now,cfg['alerts'])
    v1=Variant(cfg,'V1').fit(rows,now,cfg['alerts'])
    assert all(v1[k]==v for k,v in baseline.items())
    assert v1['eta_low_sec']==pytest.approx((10000-990)/2,abs=1e-3)
    assert v1['eta_high_sec']==pytest.approx(v1['eta_low_sec'],abs=1e-3)


def test_multiwindow_stops_advice_after_fill():
    rows=records([min(i*100,6000) for i in range(180)],step=5)
    cfg=config(); f=Variant(cfg,'V3',5,.25).fit(rows,datetime.now(timezone.utc),cfg['alerts'])
    assert f['window_used']=='short'
    assert f['regime_change'] and not f['advice_eligible']
    assert f['trend']=='STABLE'
    assert f['eta_high_sec'] is None


def test_prediction_band_open_end_and_fixed_level():
    rows=records([100+(i%2)*10 for i in range(100)])
    low,high=prediction_interval(rows,10000)
    assert high is None
    with pytest.raises(ValueError,match='fixed'): prediction_interval(rows,10000,.95)


def test_heldout_refuses_missing_or_uncommitted_parameters(tmp_path):
    with pytest.raises(ValueError,match='committed'): frozen_parameters(tmp_path)
    (tmp_path/'bench').mkdir()
    (tmp_path/'bench/forecaster_params.yml').write_text('interval_level: 0.9')
    with pytest.raises(ValueError,match='committed'): frozen_parameters(tmp_path)
    manifest=dict(repetitions=[dict(group_id='r00',wave=0),dict(group_id='r10',wave=1)])
    with pytest.raises(ValueError,match='explicit'): select_repetitions(manifest,None,True)
    assert select_repetitions(manifest,'selection',True)[0]==[manifest['repetitions'][0]]


def test_selection_constraints_and_objective_are_ordered():
    from bench.select_forecaster import rank_candidate
    def summary(error, flat, burst):
        return dict(patterns={p:{'M1_abs_error_'+h:dict(median=error) for h in ['0-5','5-15','15-30','30+']} for p in ['creeping','sawtooth']},
                    secondary=dict(patterns={'flat_high':{'S1_after_fill_advice_fraction':dict(median=flat)},
                                             'burst':{'S5_after_M1_0-15':dict(median=burst)}}))
    base=summary(10,.2,100)
    good=rank_candidate(base,summary(11,.1,200),5,.25)
    bad=rank_candidate(base,summary(12,0,0),5,.25)
    assert good < bad and not good[0] and bad[0]
    assert rank_candidate(base,summary(10,.05,999),15,1) < good


def test_noisy_prediction_roots_satisfy_band_equation():
    import numpy as np
    from scipy.stats import t
    rows=records([10*i+(-1)**i*20 for i in range(100)])
    low, high=prediction_interval(rows,1500)
    x=np.arange(100)*5; y=np.array([r['total_lag'] for r in rows])
    slope,intercept=np.polyfit(x,y,1)
    sigma=np.sqrt(np.sum((y-(slope*x+intercept))**2)/98)
    for eta,sign in [(low,1),(high,-1)]:
        future=x[-1]+eta
        band=slope*future+intercept+sign*t.ppf(.95,98)*sigma*np.sqrt(1+1/100+(future-x.mean())**2/np.sum((x-x.mean())**2))
        assert band==pytest.approx(1500)
    assert low < high
