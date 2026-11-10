"""Run the real final CLI, including guards, real fits and publication, in isolation."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import yaml
from bench.common import write_csv, write_json, seal
from bench.replay import Baseline, configured
from core import recommender


def test_whole_final_command_on_small_sealed_sample(tmp_path):
    source=Path(__file__).resolve().parents[1]
    root=tmp_path/'repo';root.mkdir()
    for folder in ('bench','core','interfaces'):
        target=root/folder;target.mkdir()
        for path in (source/folder).iterdir():
            if path.is_file() and path.suffix in {'.py','.yml','.json'}:
                shutil.copyfile(path,target/path.name)
    shutil.copyfile(source/'.gitignore',root/'.gitignore')
    shutil.copyfile(source/'config.yml',root/'config.yml')
    original=yaml.safe_load((source/'bench/confirmation_plan.yml').read_text())
    config=original['recording']['config'];config['recommendations']['forecast_persistence_cycles']=1
    plan=dict(version=2,tag='revision-r1-final-frozen',no_tuning_after_recording=True,file_sha256={},
        models={k:original['models'][k] for k in ('OLS','Responsive')},
        reuse_receipts=dict(directory='bench/results/test-receipts'),advice_persistence=dict(chosen_N=12),recording=dict(original['recording'],config=config))
    (root/'bench/confirmation_plan_2.yml').write_text(yaml.safe_dump(plan,sort_keys=False))
    def git(*args):
        return subprocess.check_output(['git',*args],cwd=root,stderr=subprocess.DEVNULL).decode().strip()
    git('init','-q');git('add','.')
    git('-c','user.name=Test','-c','user.email=test@example.invalid','-c','commit.gpgsign=false','commit','-qm','synthetic final freeze')
    git('tag',plan['tag']);commit=git('rev-parse','HEAD')
    evidence=dict(tag=plan['tag'],commit=commit,plan_sha256=hashlib.sha256(subprocess.check_output(['git','show',plan['tag']+':bench/confirmation_plan_2.yml'],cwd=root)).hexdigest())
    data=root/'bench/data/confirmation-synthetic-final';(data/'generator').mkdir(parents=True)
    iso=lambda s:datetime.fromtimestamp(s,timezone.utc).isoformat()
    history=[];forecasts=[];advice=[];reps=[]
    for pattern,rates in plan['recording']['workloads'].items():
        for j in range(10):
            index=len(reps);group=f'fixture-{pattern}-{j}'
            reps.append(dict(pattern=pattern,rates=rates,wave=0,seed=100000+index,group_id=group,topic=group))
            pair=dict(cluster_name='bench',group_id=group,topic=group,group_state='STABLE',consumer_count=1,partition_count=3)
            history.append(dict(pair,id=len(history)+1,recorded_at=iso(0),total_lag=0))
            records=[]
            for k in range(5):
                row=dict(pair,id=len(history)+1,recorded_at=iso(980+k*5),total_lag=k*500)
                records.append(row);history.append(row)
            history.append(dict(pair,id=len(history)+1,recorded_at=iso(1100),total_lag=6000 if pattern=='flat_high' else 12000))
            f=Baseline(config).fit(records,None,config['alerts'])
            forecasts.append(dict(f,id=str(index+1),recorded_at=iso(1000),input_ids_json=json.dumps([r['id'] for r in records])))
            with configured(recommender,config):
                rules=recommender.analyze_row('bench',group,group,2000,'STABLE',f['trend'],f,consumer_count=1,partition_count=3,apply_cap=False)
            advice.extend(dict(cluster_name='bench',group_id=group,topic=group,recorded_at=iso(1000),rule=r['id'].rsplit('-',1)[-1],priority=r['priority']) for r in rules)
            samples=[dict(kind='metadata',started_at=iso(0),rates=rates)]
            for t,v in [(0,0),(1000,2000),(1100,6000 if pattern=='flat_high' else 12000)]:
                samples.append(dict(timestamp=iso(t),lag=v,produced=v,consumed=0,expected_produced=v,expected_consumed=0,max_commit_gap_seconds=.5))
            (data/'generator'/(group+'.jsonl')).write_text('\n'.join(json.dumps(r) for r in samples)+'\n')
    write_csv(data/'lag_history.csv',history);write_csv(data/'forecast_log.csv',forecasts);write_csv(data/'recommendation_log.csv',advice)
    write_json(data/'manifest.json',dict(run_id=data.name,confirmation=evidence,status='complete',arm='confirmation',dirty=False,
        commit=commit,config=config,repetitions=reps,requested_duration_seconds=3600))
    seal(data)
    command=[sys.executable,'bench/final_confirmation.py','evaluate','--dataset',str(data),'--workers','2']
    done=subprocess.run(command,cwd=root,capture_output=True,text=True,timeout=90)
    assert done.returncode==0,done.stdout+'\n'+done.stderr
    result=root/'bench/results'/data.name/'confirmation'
    summary=json.loads((result/'summary.json').read_text())
    assert set(summary['models'])=={'OLS','OLS_N1','Responsive','Responsive_N1'}
    assert summary['models']['Responsive']['table5']==summary['models']['Responsive_N1']['table5']
    after_fill=lambda model:next(r['median'] for r in summary['models'][model]['secondary']
        if r['pattern']=='flat_high' and r['metric']=='S1_after_fill_advice_fraction')
    assert after_fill('Responsive')==0 and after_fill('Responsive_N1')==1
    assert (root/'bench/results/final-confirmation-latest.json').is_file()
    assert (result/'completion.json').is_file()
    assert (root/'internal/revision-r1-step7-report.md').is_file()
    assert git('status','--porcelain')==''
    repeated=subprocess.run(command,cwd=root,capture_output=True,text=True,timeout=15)
    assert repeated.returncode!=0


def test_last_recording_ledger_refuses_a_second_attempt(tmp_path,monkeypatch):
    import pytest
    from bench import final_confirmation as final
    from bench import run_forecast_validation as recorder
    (tmp_path/'bench/results').mkdir(parents=True)
    evidence=dict(tag=final.TAG,commit='fixed',plan_sha256='fixed')
    plan=dict(reuse_receipts=dict(directory='bench/results/old-receipts'))
    monkeypatch.setattr(final,'ROOT',tmp_path)
    monkeypatch.setattr(final,'guard',lambda:(plan,evidence))
    monkeypatch.setattr(final.protocol,'validate_receipts',lambda *_:None)
    calls=[]
    monkeypatch.setattr(recorder,'run',lambda **kwargs:calls.append(kwargs))
    monkeypatch.setattr(sys,'argv',['final_confirmation.py','record'])
    final.main()
    with pytest.raises(FileExistsError):final.main()
    assert len(calls)==1
