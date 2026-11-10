"""Responsive defaults, range opt-in and exploratory commit/split boundary."""
from copy import deepcopy
import subprocess

import pytest

from core import forecasting, recommender
from core.config_loader import DEFAULTS
from bench import responsive, splits
from bench.variants import V2
from test_variants import records


def test_responsive_defaults_match_v2_and_advice_matches_horizon_limit(monkeypatch):
    cfg=deepcopy(DEFAULTS)
    assert cfg['forecast']['method']=='multiwindow'
    assert cfg['forecast']['show_range'] is False
    cfg['forecast']['method']='multiwindow'
    monkeypatch.setattr(forecasting,'CONFIG',cfg)
    monkeypatch.setattr(recommender,'CONFIG',cfg)
    rows=records([i*10 for i in range(100)])
    f=forecasting.fit_configured_forecast(rows,'c','g','t')
    expected=V2(cfg).fit(rows,None,cfg['alerts'])
    assert all(f[k]==v for k,v in expected.items())
    def advice():
        return recommender.analyze_row('c','g','t',990,'STABLE',f['trend'],f,
                                      consumer_count=1,partition_count=3)[0]['advice']
    before=advice()
    assert 'more than 30 min' in before
    cfg['forecast']['show_range']=True
    assert advice()==before


def test_exploratory_refuses_uncommitted_file_before_reading_dataset(tmp_path,monkeypatch):
    import shutil
    path=tmp_path/responsive.PARAMETERS
    path.parent.mkdir()
    shutil.copyfile(responsive.ROOT/responsive.PARAMETERS,path)
    def uncommitted(*args,**kwargs):
        raise subprocess.CalledProcessError(1,args[0])
    monkeypatch.setattr(subprocess,'check_output',uncommitted)
    monkeypatch.setattr(responsive,'verify',lambda _:pytest.fail('Dataset read before commit gate'))
    with pytest.raises(ValueError,match='committed'):
        responsive.lock_exploratory(tmp_path/'nonexistent',root=tmp_path)


def test_exploratory_split_is_only_wave_two(monkeypatch):
    monkeypatch.setattr(responsive,'responsive_parameters',lambda root:({}, {'params_commit':'abc'}))
    reps=[dict(group_id='selection',wave=0),dict(group_id='evaluation',wave=1)]
    actual,provenance=splits.select_repetitions({'repetitions':reps},'heldout-exploratory',True)
    assert actual==[reps[1]]
    assert provenance['params_commit']=='abc'


def test_config_loads_responsive_and_baseline_remains_selectable(monkeypatch, tmp_path):
    from core import config_loader
    import yaml
    shipped=yaml.safe_load((config_loader.CONFIG_PATH).read_text())
    assert shipped['forecast']['method']=='multiwindow'
    monkeypatch.setattr(config_loader,'CONFIG_PATH',tmp_path/'config.yml')
    cfg=config_loader.load_config()
    assert cfg['forecast']['method']=='multiwindow'
    assert cfg['forecast']['short_window_minutes']==5
    assert cfg['forecast']['agreement_tolerance']==.5
    config_loader.CONFIG_PATH.write_text('forecast:\n  method: baseline\n')
    cfg=config_loader.load_config()
    monkeypatch.setattr(forecasting,'CONFIG',cfg)
    rows=records([i*10 for i in range(100)])
    result=forecasting.fit_configured_forecast(rows,'c','g','t')
    original=forecasting.fit_forecast(rows,'c','g','t')
    assert {k:result[k] for k in original}==original
