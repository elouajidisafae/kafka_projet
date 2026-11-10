from copy import deepcopy
from core import recommender
from core.config_loader import DEFAULTS


def test_advice_uses_calibrated_not_formula_range(monkeypatch):
    cfg=deepcopy(DEFAULTS)
    monkeypatch.setattr(recommender,'CONFIG',cfg)
    f=dict(confidence='HIGH',eta_critical_sec=180,eta_critical_min=3,slope_per_min=100,r_squared=.99,
           eta_critical_low_sec=60,eta_critical_high_sec=600,
           eta_critical_formula_low_sec=179,eta_critical_formula_high_sec=181)
    result=recommender.analyze_row('c','g','t',2000,'STABLE','INCREASING',f,consumer_count=1,partition_count=3)
    assert 'range 1.0 to 10.0 min' in result[0]['advice']
    assert '3.0 to 3.0' not in result[0]['advice']
