import pytest
from fastapi.testclient import TestClient
from core.prometheus import RULES
from interfaces import web
from core import forecasting


@pytest.mark.parametrize('suffix,rule', list(RULES.items()))
def test_metrics_export_active_rule_and_forecast_values(monkeypatch, suffix, rule):
    key=('dev','g','orders')
    row=dict(cluster_name=key[0],group_id=key[1],topic=key[2],status='OK',total_lag=100)
    monkeypatch.setattr(web,'_status_rows',lambda:[row])
    monkeypatch.setattr(web,'_cluster_statuses',{})
    rec=dict(id='dev-g-orders-'+suffix,cluster_name='dev',group_id='g',topic='orders',priority='HIGH')
    f=dict(enough_data=True,eta_critical_sec=300,eta_critical_low_sec=240,eta_critical_high_sec=420)
    monkeypatch.setattr(web,'_forecast_advice_snapshot',({key:f},[rec]))
    output=TestClient(web.app).get('/metrics').text
    assert f'khm_recommendation_active{{cluster="dev",group="g",topic="orders",rule="{rule}",priority="HIGH"}} 1' in output
    for suffix,value in [('',300),('_low',240),('_high',420)]:
        assert f'khm_forecast_critical_eta{suffix}_seconds{{cluster="dev",group="g",topic="orders"}} {value}' in output
    monkeypatch.setattr(web,'_forecast_advice_snapshot',({key:f},[]))
    f.update(eta_critical_sec=-2,eta_critical_low_sec=0,eta_critical_high_sec=None)
    output=TestClient(web.app).get('/metrics').text
    samples=[line for line in output.splitlines() if not line.startswith('#')]
    assert not any(line.startswith('khm_recommendation_active{') for line in samples)
    assert not any(line.startswith('khm_forecast_critical_eta_seconds{') for line in samples)
    assert not any(line.startswith('khm_forecast_critical_eta_high_seconds{') for line in samples)
    assert 'khm_forecast_critical_eta_low_seconds{cluster="dev",group="g",topic="orders"} 0' in samples
    row['stale']=True
    output=TestClient(web.app).get('/metrics').text
    assert not any(line.startswith('khm_forecast_critical_eta') for line in output.splitlines())


def test_metric_label_escaping():
    from core.prometheus import forecast_advice_lines
    import json
    key=('dev','g"\\\n','orders')
    lines=forecast_advice_lines([],{key:dict(enough_data=True,eta_critical_sec=5)},{key})
    sample=next(line for line in lines if line.startswith('khm_forecast_critical_eta_seconds{'))
    assert f'group={json.dumps(key[1])},' in sample
    assert sample.endswith(' 5')
