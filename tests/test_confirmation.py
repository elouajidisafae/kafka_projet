"""Freeze gates, exact metric denominators, and the complete one-shot output path."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor

import pytest
import yaml
from bench import confirmation as protocol
from bench import confirmation_eval as ev
from bench.common import write_csv


@pytest.fixture
def frozen_repo(tmp_path):
    root = tmp_path/'repo'; root.mkdir(); (root/'bench').mkdir()
    (root/'source.py').write_text('fixed = 1\n')
    plan = dict(tag=protocol.TAG,no_tuning_after_recording=True,
                file_sha256={'source.py':hashlib.sha256(b'fixed = 1\n').hexdigest()})
    (root/protocol.PLAN).write_text(yaml.safe_dump(plan))
    def git(*args):
        subprocess.run(['git',*args],cwd=root,check=True,capture_output=True)
    git('init','-q');git('add','.')
    git('-c','user.name=Test','-c','user.email=test@example.invalid','-c','commit.gpgsign=false','commit','-qm','freeze')
    git('tag',protocol.TAG)
    return root,git


def test_guard_rejects_dirty_tree_different_head_and_hidden_setting_change(frozen_repo):
    root,git=frozen_repo
    _,evidence=protocol.guard(root)
    assert evidence['plan_sha256']==hashlib.sha256(protocol.git('show',protocol.TAG+':'+protocol.PLAN,root=root)).hexdigest()
    (root/'source.py').write_text('fixed = 2\n')
    with pytest.raises(ValueError,match='Working tree'):
        protocol.guard(root)
    git('update-index','--assume-unchanged','source.py')
    with pytest.raises(ValueError,match='hash mismatch'):
        protocol.guard(root)
    git('update-index','--no-assume-unchanged','source.py');git('add','.')
    git('-c','user.name=Test','-c','user.email=test@example.invalid','-c','commit.gpgsign=false','commit','-qm','changed')
    with pytest.raises(ValueError,match='Working tree'):
        protocol.guard(root)


def test_evaluation_gate_precedes_data_read(monkeypatch,tmp_path):
    def fail():
        raise ValueError('frozen tag mismatch')
    monkeypatch.setattr(protocol,'guard',fail)
    monkeypatch.setattr(ev,'verify',lambda _:pytest.fail('read data before gate'))
    with pytest.raises(ValueError,match='frozen tag'):
        ev.evaluate(tmp_path,{}, {},workers=1)
    monkeypatch.setattr(protocol,'guard',lambda:({'models':'frozen'}, {'commit':'abc'}))
    with pytest.raises(ValueError,match='Wrong freeze'):
        ev.evaluate(tmp_path,{'models':'changed'}, {'commit':'abc'},workers=1)


def test_recording_gate_precedes_creating_any_dataset(monkeypatch,tmp_path):
    from bench import run_forecast_validation as recorder
    def fail():
        raise ValueError('frozen tag mismatch')
    monkeypatch.setattr(protocol,'guard',fail)
    monkeypatch.setattr(recorder,'ROOT',tmp_path)
    with pytest.raises(ValueError,match='frozen tag'):
        recorder.run(arm='confirmation',waves=1,receipt_dir=tmp_path/'receipts')
    assert not (tmp_path/'bench/data').exists()


def test_all_frozen_adapters_receive_registered_parameters(monkeypatch):
    from bench.comparison_models import OfflineModel
    from bench.replay import Baseline
    from test_variants import records
    root=Path(__file__).resolve().parents[1]
    plan=yaml.safe_load((root/'bench/confirmation_plan.yml').read_text())
    config=plan['recording']['config']
    history=records([i*10 for i in range(20)])
    for r in history:
        r.update(group_state='STABLE',consumer_count=1,partition_count=3)
    ev.initialise(config,{r['id']:r for r in history},plan['models'])
    saved=dict(id='100',group_id='g',topic='t',recorded_at=history[-1]['recorded_at'],
               input_ids_json=json.dumps([r['id'] for r in history]))
    before=Baseline(config).fit(history,None,config['alerts']);saved.update(before)
    settings=[]
    def offline_fit(self,records,now,thresholds):
        settings.append((self.name,self.settings))
        return dict(before,fit_failed=False,filled_steps=0)
    monkeypatch.setattr(OfflineModel,'fit',offline_fit)
    try:
        rows={name:ev.fit_task((name,saved)) for name in plan['models']}
    finally:
        ev._limits.restore_original_limits()
    assert rows['OLS']['ols_differences']=={}
    assert rows['Responsive']['eta_critical_sec']==before['eta_critical_sec']
    assert settings==[('Holt',{'alpha':.2,'beta':.2}),('ARIMA',{'p':2,'q':2}),('ARIMA',{'p':2,'q':1})]


def test_parallel_confirmation_matches_sequential_on_synthetic_inputs():
    from bench.replay import Baseline
    from test_variants import records
    root=Path(__file__).resolve().parents[1]
    plan=yaml.safe_load((root/'bench/confirmation_plan.yml').read_text())
    config=plan['recording']['config']
    history=records([i*10+(i%3) for i in range(16)])
    for r in history:
        r.update(group_state='STABLE',consumer_count=1,partition_count=3)
    saved=dict(id='100',group_id='g',topic='t',recorded_at=history[-1]['recorded_at'],
               input_ids_json=json.dumps([r['id'] for r in history]))
    saved.update(Baseline(config).fit(history,None,config['alerts']))
    shared={r['id']:r for r in history}
    tasks=[(name,saved) for name in plan['models']]
    ev.initialise(config,shared,plan['models'])
    try:
        sequential=list(map(ev.fit_task,tasks))
    finally:
        ev._limits.restore_original_limits()
    with ProcessPoolExecutor(max_workers=2,initializer=ev.initialise,initargs=(config,shared,plan['models'])) as pool:
        parallel=list(pool.map(ev.fit_task,tasks))
    for result in sequential+parallel:
        result.pop('duration_seconds')
    assert sequential==parallel


def test_positive_coverage_counts_missing_forecasts_and_burst_end():
    iso=lambda s:datetime.fromtimestamp(s,timezone.utc).isoformat()
    inputs={'g':dict(rep=dict(pattern='burst'),history=[dict(recorded_at=iso(0),total_lag=0),dict(recorded_at=iso(20),total_lag=2000)],
                     metadata=dict(started_at=iso(0),rates=dict(burst_at=2,burst_seconds=4)))}
    f=lambda t,eta,available:dict(group_id='g',recorded_at=iso(t),eta_critical_sec=eta,enough_data=available)
    result=ev.positive_coverage(inputs,[f(5,10,True),f(6,4,True),f(8,None,False)],1000)
    assert result['table'][0]['origins']==2  # excludes t=5, which is during burst
    assert result['table'][0]['positive']==1
    assert result['table'][0]['pooled_fraction']==.5


def test_validate_design_rejects_old_seed_and_different_plan():
    evidence=dict(tag=protocol.TAG,commit='abc',plan_sha256='fixed')
    config={'forecast':{'method':'baseline'}}
    patterns={p:{'produce':1,'consume':1} for p in ('burst','creeping','sawtooth','flat_high')}
    reps=[dict(pattern=p,rates=r,wave=0,seed=i*10+j) for i,(p,r) in enumerate(patterns.items()) for j in range(10)]
    plan=dict(recording=dict(config=config,workloads=patterns,excluded_seeds=[100]))
    manifest=dict(confirmation=evidence,status='complete',arm='confirmation',dirty=False,commit='abc',config=config,
                  repetitions=reps,requested_duration_seconds=3600)
    ev.validate_design(manifest,plan,evidence)
    wrong=deepcopy(manifest);wrong['repetitions'][0]['seed']=100
    with pytest.raises(ValueError,match='Seeds'):
        ev.validate_design(wrong,plan,evidence)
    wrong=deepcopy(manifest);wrong['confirmation']['plan_sha256']='changed'
    with pytest.raises(ValueError,match='plan hash'):
        ev.validate_design(wrong,plan,evidence)


def test_complete_pipeline_writes_every_model_and_metric_once(tmp_path,monkeypatch):
    # Fits are stubbed here; existing adapter tests cover the numerical models.
    # Exercise real aggregation, tables, provenance, stable controls and nonoverwrite.
    source=Path(__file__).resolve().parents[1]
    for name in ('range_calibration.yml','range_calibration_v2.yml'):
        target=tmp_path/'bench'/name;target.parent.mkdir(exist_ok=True)
        shutil.copyfile(source/'bench'/name,target)
    dataset=tmp_path/'recording';dataset.mkdir();(dataset/'SHA256SUMS').write_text('sealed test input')
    iso=lambda s:datetime.fromtimestamp(s,timezone.utc).isoformat()
    patterns={p:{'produce':1,'consume':1,'burst_at':0,'burst_seconds':1,'fill_seconds':1} for p in ('burst','creeping','sawtooth','flat_high')}
    config=dict(alerts=dict(warning_threshold=100,critical_threshold=1000),forecast=dict(method='baseline'),monitor=dict(refresh_interval=5))
    evidence=dict(tag=protocol.TAG,commit='abc',plan_sha256='fixed')
    models={k:{} for k in ('OLS','Responsive','Holt','ARIMA_212','ARIMA_211_exploratory')}
    inputs={};logged=[];reps=[]
    for i,(p,rates) in enumerate(patterns.items()):
        for j in range(10):
            index=i*10+j;group=f'g{index}';rep=dict(pattern=p,rates=rates,wave=0,seed=index,group_id=group,topic=group);reps.append(rep)
            history=[dict(recorded_at=iso(t),total_lag=v) for t,v in ((0,0),(20,500 if p=='flat_high' else 2000))]
            generator=[dict(timestamp=iso(t),lag=v,produced=v,consumed=0,expected_produced=v,expected_consumed=0,max_commit_gap_seconds=.5) for t,v in ((0,0),(20,500 if p=='flat_high' else 2000))]
            inputs[group]=dict(rep=rep,history=history,generator=generator,metadata=dict(started_at=iso(0),rates=rates),start=0,end=20)
            logged.append(dict(id=str(index),group_id=group,topic=group,cluster_name='bench',recorded_at=iso(5),input_ids_json=json.dumps([index])))
    manifest=dict(confirmation=evidence,status='complete',arm='confirmation',dirty=False,commit='abc',config=config,
                  repetitions=reps,requested_duration_seconds=3600,run_id='confirmation-synthetic')
    plan=dict(models=models,recording=dict(config=config,workloads=patterns,excluded_seeds=[100]))
    write_csv(dataset/'forecast_log.csv',logged)
    write_csv(dataset/'recommendation_log.csv',[],['cluster_name','group_id','topic','rule','priority','recorded_at'])
    monkeypatch.setattr(protocol,'guard',lambda:(plan,evidence))
    monkeypatch.setattr(ev,'ROOT',tmp_path);monkeypatch.setattr(ev,'verify',lambda _:manifest)
    monkeypatch.setattr(ev,'load_inputs',lambda *_:({},inputs,logged))
    def fit(task):
        model,f=task
        result = dict(id=f['id'],group_id=f['group_id'],topic=f['topic'],recorded_at=f['recorded_at'],
                    enough_data=True,eta_critical_sec=5,trend='INCREASING',confidence='HIGH',window_seconds=600,
                    fit_failed=False,advice=[],ols_differences={},duration_seconds=0)
        if model not in {'OLS','Responsive'}:
            result.pop('trend')
            result['confidence']='NOT_APPLICABLE'
        return result
    monkeypatch.setattr(ev,'fit_task',fit)
    ev.evaluate(dataset,plan,evidence,workers=1)
    output=tmp_path/'bench/results/confirmation-synthetic/confirmation'
    result=json.loads((output/'summary.json').read_text())
    assert set(result['models'])==set(models)
    assert result['origins']==40
    assert set(result['ranges'])=={'OLS','Responsive'}
    assert all((output/(name+'.jsonl')).is_file() for name in models)
    assert result['models']['OLS']['positive_crossing_coverage']['table'][0]['pooled_fraction']==1
    assert result['ranges']['OLS']['stable_backlog'][-1]['n']==10
    assert (output/'table5.csv').is_file() and (output/'ranges.csv').is_file()
    assert 'Part 5' in (tmp_path/'internal/revision-r1-step6-report.md').read_text()
    with pytest.raises(FileExistsError):
        ev.evaluate(dataset,plan,evidence,workers=1)
