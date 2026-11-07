"""Offline model and comparison safeguards; no archive or broker required."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import numpy as np
import pytest
from bench.comparison_models import regular_series, first_crossing, OfflineModel, holt_projection
from bench.replay import load_forecaster

CONFIG={"monitor":{"refresh_interval":5},"forecast":{"window_hours":1},"alerts":{"warning_threshold":45,"critical_threshold":50}}


def records(times=(0,5,10,15,20), values=(0,10,20,30,40)):
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    return [dict(id=i,cluster_name="c",group_id="g",topic="t",recorded_at=(start+timedelta(seconds=t)).isoformat(),total_lag=y) for i,(t,y) in enumerate(zip(times,values))]


def test_regular_series_forward_fills_without_future_leakage():
    series,filled=regular_series(records((0,5,15),(0,10,30)))
    assert series.tolist()==[0,10,10,30] and filled==1
    series,filled=regular_series(records())
    assert filled==0 and series.tolist()==[0,10,20,30,40]


def test_regular_series_rejects_invalid_input():
    with pytest.raises(ValueError): regular_series([])
    with pytest.raises(ValueError): regular_series(list(reversed(records())))


def test_holt_reproduces_a_line_including_beta_above_alpha():
    prediction=holt_projection(np.array([0,10,20,30,40]),.1,.5,3)
    assert np.allclose(prediction,[50,60,70])
    assert first_crossing(prediction,55,5)==10
    assert first_crossing(prediction,1000,5)==-2
    model=load_forecaster("bench.variants:Holt",dict(CONFIG,model_comparison={"Holt":dict(alpha=.1,beta=.5)}))
    result=model.fit(records(),None,CONFIG["alerts"])
    assert result["enough_data"] and result["eta_critical_sec"]==5
    assert result["filled_steps"]==0


def test_arima_drift_and_failure_as_missing():
    pytest.importorskip("statsmodels")
    model=OfflineModel(CONFIG,"ARIMA",p=0,q=0)
    data=records(tuple(range(0,300,5)),np.cumsum(np.random.default_rng(42).normal(2,.2,60)))
    result=model.fit(data,None,dict(warning_threshold=150,critical_threshold=200))
    assert result["enough_data"] and result["eta_critical_sec"] > 0
    with patch("statsmodels.tsa.arima.model.ARIMA",side_effect=RuntimeError("fit failed")):
        failed=model.fit(records(),None,CONFIG["alerts"])
    assert failed["fit_failed"] and failed["eta_critical_sec"] is None
    assert "fit failed" in failed["reason"]


def test_arima_nonconvergence_is_missing():
    pytest.importorskip("statsmodels")
    with patch("statsmodels.tsa.arima.model.ARIMA") as factory:
        factory.return_value.fit.return_value.mle_retvals={"converged":False}
        result=OfflineModel(CONFIG,"ARIMA",p=1,q=1).fit(records(),None,CONFIG["alerts"])
    assert result["fit_failed"] and result["eta_critical_sec"] is None


