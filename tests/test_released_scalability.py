from types import SimpleNamespace

import yaml

from bench import paper_scalability as scale


def test_released_scalability_keeps_product_settings(tmp_path, monkeypatch):
    from pathlib import Path
    original=yaml.safe_load(Path('config.yml').read_text(encoding='utf-8'))
    (tmp_path/'config.yml').write_text(yaml.safe_dump(original),encoding='utf-8')
    monkeypatch.setattr(scale,'ROOT',tmp_path)
    import confluent_kafka.admin
    monkeypatch.setattr(confluent_kafka.admin,'AdminClient',lambda config:SimpleNamespace(
        list_topics=lambda timeout:SimpleNamespace(topics={'chosen':None,'other':None})))
    actual=scale.released_config(['group'],['chosen'])
    for key in ['forecast','recommendations','monitor','retention','alerts']:
        assert actual[key]==original[key]
    assert actual['forecast']['method']=='multiwindow'
    assert actual['recommendations']['forecast_persistence_cycles']==12
    assert actual['exclude_topics']==['other']


def test_released_run_rejects_wrong_coverage_hash_before_start(tmp_path, monkeypatch):
    import json
    import pytest
    root=tmp_path/'bench'/'results';root.mkdir(parents=True)
    (root/'summary.json').write_text('{}',encoding='utf-8')
    (root/'final-confirmation-latest.json').write_text(json.dumps(
        dict(summary_path='bench/results/summary.json',sha256='wrong')),encoding='utf-8')
    monkeypatch.setattr(scale,'begin',lambda *args:pytest.fail('Must reject before creating a run'))
    with pytest.raises(ValueError,match='hash mismatch'):
        scale.run('frozen',released_default=True,results_root=root)

