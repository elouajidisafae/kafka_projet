"""Controlled recording, scheduling and offline scoring acceptance checks."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from unittest.mock import Mock
import pytest

from core import db, forecasting, recommender
from core.scheduling import run_cycles
from bench.common import seal, verify, write_json
from bench.forecast_eval import breach, horizon_bin, quantiles, score_rep


def test_serial_overrun_has_no_extra_delay():
    clock=[0.0]
    starts=[]
    overruns=[]
    def cycle():
        starts.append(clock[0])
        clock[0]+=7
    run_cycles(cycle,5,stopped=lambda:len(starts)==3,on_overrun=overruns.append,
               clock=lambda:clock[0],wait=lambda n:clock.__setitem__(0,clock[0]+n))
    assert starts==[0,7,14] and overruns==[7,7,7]


def test_short_cycle_waits_only_remaining_period():
    clock=[0.0]
    starts=[]
    def cycle():
        starts.append(clock[0]); clock[0]+=2
    run_cycles(cycle,5,stopped=lambda:len(starts)==3,clock=lambda:clock[0],wait=lambda n:clock.__setitem__(0,clock[0]+n))
    assert starts==[0,5,10]


def test_pre_cap_logging(tmp_path,monkeypatch):
    monkeypatch.setattr(db,"DB_PATH",tmp_path/"history.db")
    monkeypatch.setitem(db.CONFIG,"run_id","test-recording")
    db.init_db(); db.init_db()
    for i in range(15):
        db.save_lag("bench",f"g{i}","orders",2000,"WARNING","STABLE",consumer_count=3,partition_count=3)
    forecasts={("bench",f"g{i}","orders"):dict(trend="INCREASING",confidence="MEDIUM") for i in range(15)}
    monkeypatch.setattr(recommender,"forecasts_for_request",lambda:forecasts)
    shown=recommender.get_recommendations(persist=True)
    assert len(shown)==10
    with db._get_connection() as conn:
        rows=conn.execute("SELECT * FROM recommendation_log").fetchall()
    assert len(rows)==15 and sum(r["displayed"] for r in rows)==10
    assert all(r["run_id"]=="test-recording" for r in rows)
    assert json.loads(rows[0]["metrics_json"])==shown[0]["metrics"]
    recommender.get_recommendations()
    with db._get_connection() as conn:
        assert conn.execute("SELECT count(*) FROM recommendation_log").fetchone()[0]==15


def test_client_pool_reuses_by_alias_and_configuration():
    from core.client_pool import client
    factory=Mock(side_effect=lambda config:object())
    a=client(factory,{"bootstrap.servers":"a"},"first")
    assert client(factory,{"bootstrap.servers":"a"},"first") is a
    assert client(factory,{"bootstrap.servers":"a"},"second") is not a
    assert factory.call_count==2


def test_interpolation_and_error_sign():
    assert breach([(0,0),(10,200)],100)==5
    assert breach([(0,0),(10,20)],100) is None
    rep=dict(group_id="g",pattern="creeping")
    iso=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
    history=[dict(recorded_at=iso(0),total_lag=0),dict(recorded_at=iso(100),total_lag=200)]
    f=dict(id=1,recorded_at=iso(10),eta_critical_sec=60,trend="INCREASING",confidence="HIGH")
    metrics,pairs,truth,_=score_rep(rep,history,[f],[],[],100)
    assert truth==50 and pairs[0]["error_seconds"]==20 and pairs[0]["horizon_seconds"]==40
    assert metrics["M1_abs_error_0-5"]==20 and metrics["M2_signed_error_0-5"]==20
    assert metrics["M3_missed_warning_rate"]==0
    assert metrics["M4_advice_lead_seconds"] is None


def test_negative_control_and_quantiles():
    rep=dict(group_id="g",pattern="flat_high")
    iso=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
    history=[dict(recorded_at=iso(0),total_lag=60),dict(recorded_at=iso(100),total_lag=60)]
    f=dict(id=1,recorded_at=iso(10),eta_critical_sec=-2,trend="STABLE",confidence="LOW")
    metrics,_,truth,_=score_rep(rep,history,[f],[],[],100)
    assert truth is None and metrics["M6_any_false_alarm"]==0
    assert quantiles([1,3,100])["median"]==3
    assert [horizon_bin(t) for t in (300,301,900,901,1800,1801)]==["0-5","5-15","5-15","15-30","15-30","30+"]


def test_seal_detects_tampering(tmp_path):
    write_json(tmp_path/"manifest.json",{"run_id":"x"})
    seal(tmp_path)
    assert verify(tmp_path)["run_id"]=="x"
    with pytest.raises(ValueError): seal(tmp_path)
    (tmp_path/"extra").write_text("unexpected")
    with pytest.raises(ValueError): verify(tmp_path)


def test_export_replay_and_deterministic_summary(tmp_path,monkeypatch):
    import runpy
    from bench import replay as replay_module, forecast_eval as evaluation
    from bench.export_run import export
    harness=runpy.run_path(str(Path(__file__).parent/"test_golden_output.py"))
    config=deepcopy(harness["SETTINGS"])
    config["run_id"]="synthetic"
    config["forecast"].update(persist_every_cycle=True,record_inputs=True)
    database=tmp_path/"history.db"
    shutil.copyfile(Path(__file__).parent/"fixtures/golden.db",database)
    monkeypatch.setattr(db,"DB_PATH",database)
    monkeypatch.setattr(db,"datetime",harness["FixedDatetime"])
    for module in (db,forecasting,recommender): monkeypatch.setattr(module,"CONFIG",config)
    monkeypatch.setattr(forecasting,"_cache",None)
    db.init_db()
    with db._get_connection() as conn: conn.execute("UPDATE lag_history SET run_id='synthetic'")
    forecasting.forecast_all()
    recommender.get_recommendations(persist=True)
    dataset=tmp_path/"data"
    dataset.mkdir()
    repetitions=[]
    with db._get_connection() as conn:
        history=[dict(r) for r in conn.execute("SELECT * FROM lag_history ORDER BY recorded_at")]
    (dataset/"generator").mkdir()
    for group in sorted({r["group_id"] for r in history}):
        repetitions.append(dict(group_id=group,topic="orders",pattern="creeping"))
        rows=[dict(timestamp=r["recorded_at"],produced=r["total_lag"],consumed=0,lag=r["total_lag"],elapsed=i,expected_produced=max(1,r["total_lag"]),expected_consumed=1,max_commit_gap_seconds=.5) for i,r in enumerate(history) if r["group_id"]==group]
        (dataset/"generator"/(group+".jsonl")).write_text("\n".join(json.dumps(r) for r in rows))
    write_json(dataset/"manifest.json",dict(run_id="synthetic",config=config,repetitions=repetitions,profile="smoke",status="complete"))
    export("synthetic",database,dataset)
    monkeypatch.setattr(replay_module,"ROOT",tmp_path)
    monkeypatch.setattr(evaluation,"ROOT",tmp_path)
    assert replay_module.replay(dataset)["passed"]
    evaluation.evaluate(dataset,plot=False)
    summary=tmp_path/"bench/results/synthetic/baseline/summary.json"
    first=summary.read_bytes()
    evaluation.evaluate(dataset,plot=False)
    assert summary.read_bytes()==first


def test_overrun_counter_exposition():
    from core import timing
    timing.record_overrun("test-cluster")
    output="\n".join(timing.prometheus_lines())
    assert "# TYPE khm_collection_overruns_total counter" in output
    assert 'khm_collection_overruns_total{cluster="test-cluster"}' in output


def test_recommendation_retention(tmp_path,monkeypatch):
    monkeypatch.setattr(db,"DB_PATH",tmp_path/"history.db")
    db.init_db()
    with db._get_connection() as conn:
        conn.execute("INSERT INTO recommendation_log (cluster_name,group_id,topic,rule,type,priority,displayed,metrics_json,recorded_at) VALUES ('c','g','t','scale','PERFORMANCE','HIGH',1,'{}','2000-01-01')")
    db.purge_old_records()
    with db._get_connection() as conn:
        assert conn.execute("SELECT count(*) FROM recommendation_log").fetchone()[0]==0


def test_operator_interrupt_marks_aborted(tmp_path,monkeypatch):
    from bench import run_forecast_validation as runner
    root=Path(__file__).resolve().parents[1]
    (tmp_path/"bench").mkdir()
    for name in ("config.bench.yml","config.bench.smoke.yml"):
        shutil.copyfile(root/name,tmp_path/name)
    shutil.copyfile(root/"bench/patterns.yml",tmp_path/"bench/patterns.yml")
    shutil.copyfile(root/"bench/workload.py",tmp_path/"bench/workload.py")
    monkeypatch.setattr(runner,"ROOT",tmp_path)
    monkeypatch.setattr(runner,"capture",lambda args:"test")
    def interrupt(*args,**kwargs):
        raise KeyboardInterrupt
    monkeypatch.setattr(runner,"command",interrupt)
    with pytest.raises(RuntimeError,match="aborted"):
        runner.run(arm="smoke",profile="smoke",reps=1,waves=1,duration_min=5)
    manifest=json.loads(next((tmp_path/"bench/data").glob("*/manifest.json")).read_text())
    assert manifest["status"]=="aborted" and manifest["ended_at"]


def test_run_allowlist_excludes_historical_snapshots(tmp_path,monkeypatch):
    monkeypatch.setattr(db,"DB_PATH",tmp_path/"history.db")
    db.init_db()
    db.save_lag("bench","old-run","topic",2000,"WARNING","STABLE")
    db.save_lag("bench","new-run","topic",100,"OK","STABLE")
    monkeypatch.setitem(db.CONFIG,"include_groups",["new-run"])
    assert [r["group_id"] for r in db.get_latest_per_group()]==["new-run"]
    assert [r["group_id"] for r in db.get_latest_per_group("bench")]==["new-run"]


def test_missed_warning_and_precap_lead():
    iso=lambda t:datetime.fromtimestamp(t,timezone.utc).isoformat()
    history=[dict(recorded_at=iso(0),total_lag=0),dict(recorded_at=iso(100),total_lag=200)]
    forecasts=[dict(id=1,recorded_at=iso(10),eta_critical_sec=-2,trend="STABLE",confidence="LOW"),
               dict(id=2,recorded_at=iso(20),eta_critical_sec=20,trend="INCREASING",confidence="HIGH"),
               dict(id=3,recorded_at=iso(60),eta_critical_sec=-1,trend="INCREASING",confidence="HIGH")]
    recs=[dict(rule="scale",priority="MEDIUM",recorded_at=iso(15),displayed=0)]
    metrics,pairs,_,_=score_rep(dict(group_id="g",pattern="creeping"),history,forecasts,recs,[],100)
    assert metrics["M3_missed_warning_rate"]==.5
    assert metrics["M4_advice_lead_seconds"]==35
    assert len(pairs)==1 and pairs[0]["error_seconds"]==-10


def test_sealed_export_releases_windows_file_handle(tmp_path,monkeypatch):
    from bench.export_run import export
    database=tmp_path/"history.db"
    monkeypatch.setattr(db,"DB_PATH",database)
    monkeypatch.setitem(db.CONFIG,"run_id","r")
    db.init_db()
    db.save_lag("c","g","t",1,"OK")
    f=dict(enough_data=True,cluster_name="c",group_id="g",topic="t",slope=.1,intercept=1,r_squared=1,confidence="HIGH",trend="INCREASING",current_lag=1,n_points=5,window_seconds=20,warning_threshold=1000,critical_threshold=10000,eta_warning_sec=2**70,eta_critical_sec=2**71)
    db.save_forecast(f,input_ids=[1])
    dataset=tmp_path/"data"
    write_json(dataset/"manifest.json",dict(run_id="r"))
    export("r",database,dataset)
    moved=tmp_path/"closed.db"
    database.rename(moved)
    assert str(2**71) in (dataset/"forecast_log.csv").read_text()


def test_processed_offsets_commit_immediately_and_idle_is_bounded():
    from bench.workload import CommitTracker
    from confluent_kafka import TopicPartition
    clock=[0.0]
    consumer=Mock()
    consumer.commit.side_effect=lambda **kwargs: kwargs["offsets"]
    offsets={0:0,1:0,2:0}
    tracker=CommitTracker(consumer,"topic",offsets,clock=lambda:clock[0])
    clock[0]=.05
    offsets[0]=1
    tracker.flush(offsets)
    assert consumer.commit.call_count==1
    assert tracker.offsets==offsets
    offsets[0]=2
    assert tracker.offsets[0]==1  # confirmed snapshot is not an alias
    clock[0]=.1
    tracker.flush(offsets)
    assert consumer.commit.call_count==2
    clock[0]=.2
    tracker.flush(offsets)
    assert consumer.commit.call_count==2
    clock[0]=.61
    tracker.flush(offsets)
    assert consumer.commit.call_count==3
    assert tracker.max_gap==pytest.approx(.51)


def test_failed_commit_does_not_advance_confirmed_offsets():
    from bench.workload import CommitTracker
    from types import SimpleNamespace
    consumer=Mock()
    consumer.commit.return_value=[SimpleNamespace(error="commit failed")]
    tracker=CommitTracker(consumer,"topic",{0:0})
    with pytest.raises(RuntimeError,match="rejected"):
        tracker.flush({0:1})
    assert tracker.offsets=={0:0}


def test_publication_requires_all_acceptance_evidence():
    from bench.forecast_eval import publication_eligible
    manifest=dict(arm='baseline',profile='baseline',status='complete',dirty=False)
    check=dict(rate_pass=True,commit_pass=True,starts_at_zero=True,calibration_failure=False)
    fidelity=dict(passed=True,exact_forecast_fraction=1,exact_recommendation_fraction=1,
                  forecast_mismatches=[],recommendation_mismatches=[])
    assert publication_eligible(manifest,[check],1,fidelity)
    for fraction in (None,0,.94):
        assert not publication_eligible(manifest,[check],fraction,fidelity)
    assert not publication_eligible(manifest,[],1,fidelity)
    assert not publication_eligible(manifest,[check],1,{})
    for field,value in [('arm','calibration'),('profile','smoke'),('status','aborted'),('dirty',True)]:
        assert not publication_eligible(dict(manifest,**{field:value}),[check],1,fidelity)
    for field,value in [('rate_pass',False),('commit_pass',False),('starts_at_zero',False),('calibration_failure',True)]:
        assert not publication_eligible(manifest,[dict(check,**{field:value})],1,fidelity)
    for field,value in [('passed',False),('exact_forecast_fraction',.98),('exact_recommendation_fraction',.98),
                        ('forecast_mismatches',[{'cause':'unexplained'}]),('recommendation_mismatches',[{'cause':'unexplained'}])]:
        assert not publication_eligible(manifest,[check],1,dict(fidelity,**{field:value}))
