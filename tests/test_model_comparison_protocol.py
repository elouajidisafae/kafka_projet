from bench.model_comparison import HOLT, ARIMA, metrics


def test_all_grid_points_present_and_deterministic():
    assert len(HOLT)==30 and len(ARIMA)==9
    assert {r["p"] for r in ARIMA}=={0,1,2}
    assert {r["q"] for r in ARIMA}=={0,1,2}


def test_scoring_aggregates_repetitions_before_quartiles():
    def observation(rep,error):
        return dict(rep=rep,pattern="creeping",horizon="0-5",absolute_error_seconds=error,
                    duration_seconds=.01,filled_steps=1,fit_failed=False,available=True,ols_differences={})
    result=metrics([observation("a",0),observation("a",100),observation("b",10)])
    assert result["horizon_errors"][0]["median"]==30
    assert result["horizon_errors"][0]["n"]==2
    assert result["filled_steps"]==3



def test_output_never_overwrites_an_existing_phase(tmp_path, monkeypatch):
    import pytest
    from bench import model_comparison as comparison
    monkeypatch.setattr(comparison,"ROOT",tmp_path)
    output=comparison.new_phase(tmp_path/"dataset","selection")
    (output/"kept.txt").write_text("keep")
    with pytest.raises(FileExistsError): comparison.new_phase(tmp_path/"dataset","selection")
    assert (output/"kept.txt").read_text()=="keep"


def test_selection_uses_only_wave_one_input_provenance(tmp_path, monkeypatch):
    import json
    from bench import model_comparison as comparison
    group="selection"
    rep=dict(group_id=group,topic="t",pattern="creeping",wave=0)
    manifest={"repetitions":[rep,dict(group_id="heldout",topic="t",pattern="creeping",wave=1)],
              "config":{"alerts":{"critical_threshold":50}}}
    monkeypatch.setattr(comparison,"verify",lambda path:manifest)
    (tmp_path/"generator").mkdir()
    (tmp_path/"generator/selection.jsonl").write_text(json.dumps(dict(produced=10,consumed=0,timestamp="2026-01-01T00:00:00+00:00"))+"\n")
    history=[dict(id="1",cluster_name="c",group_id=group,topic="t",total_lag="10",recorded_at="2026-01-01T00:00:00+00:00")]
    forecasts=[dict(id="1",cluster_name="c",group_id=group,topic="t",recorded_at="2026-01-01T00:00:00+00:00",input_ids_json="[1]"),
               dict(id="2",group_id="heldout",input_ids_json="invalid heldout inputs must never be parsed")]
    monkeypatch.setattr(comparison,"csv_rows",lambda path:iter(history if path.name=="lag_history.csv" else forecasts))
    _,reps,observations=comparison.context(tmp_path,"selection")
    assert reps==[rep] and len(observations)==1
    assert observations[0]["records"][0]["id"]==1



def test_minute_selection_keeps_earliest_origin_per_repetition_and_interval():
    from datetime import datetime, timezone
    from bench.model_comparison import sample_selection_origins
    def origin(group, seconds, identifier):
        return dict(rep={"group_id":group},repetition_start=0,
                    logged={"recorded_at":datetime.fromtimestamp(seconds,timezone.utc).isoformat(),"id":str(identifier)})
    rows=[origin("a",61,4),origin("a",5,1),origin("a",59,2),origin("a",60,3),
          origin("a",120,6),origin("a",120,5),origin("b",7,7)]
    selected=sample_selection_origins(rows)
    assert [r["logged"]["id"] for r in selected]==["1","3","5","7"]


def test_parallel_and_sequential_forecasts_are_exactly_equal():
    from datetime import datetime, timedelta, timezone
    import json
    import numpy as np
    from bench.model_comparison import FitRunner, run_model
    from bench.replay import Baseline
    config={"monitor":{"refresh_interval":5},"forecast":{"window_hours":1},
            "alerts":{"warning_threshold":150,"critical_threshold":200}}
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    values=np.cumsum(np.random.default_rng(42).normal(2,.2,60))
    history=[dict(id=i,cluster_name="c",group_id="g",topic="t",total_lag=float(value),
                  recorded_at=(start+timedelta(seconds=5*i)).isoformat()) for i,value in enumerate(values)]
    observations=[]
    for index,length in enumerate([20,30,45,60]):
        records=history[:length]
        now=datetime.fromisoformat(records[-1]["recorded_at"])
        expected=Baseline(config).fit(records,now,config["alerts"])
        logged=dict(id=str(index),recorded_at=now.isoformat(),input_ids_json=json.dumps([r["id"] for r in records]),
                    **{key:str(expected[key]) for key in ("slope","intercept","r_squared","eta_critical_sec")})
        observations.append(dict(rep={"group_id":"g","pattern":"creeping"},logged=logged,records=records,
                                 truth=(start+timedelta(seconds=500)).timestamp()))
    with FitRunner(observations,config,1) as sequential, FitRunner(observations,config,2) as parallel:
        for name,settings in [("OLS",{}),("Holt",{"alpha":.3,"beta":.2}),
                              ("ARIMA",{"p":0,"q":0}),("ARIMA",{"p":1,"q":1})]:
            before=run_model(observations,config,name,settings,runner=sequential)
            after=run_model(observations,config,name,settings,runner=parallel)
            # Wall-clock duration is measurement telemetry, never a forecast result.
            strip=lambda rows:[{key:value for key,value in row.items() if key!="duration_seconds"} for row in rows]
            assert strip(before)==strip(after), name
            assert [r["forecast_id"] for r in after]==["0","1","2","3"]
            if name in {"OLS","Holt"} or settings=={"p":0,"q":0}:
                assert any(r["available"] for r in after)


def test_heldout_keeps_every_origin_while_selection_samples_minutes(tmp_path,monkeypatch):
    import json
    from bench import model_comparison as comparison
    group="g"
    manifest={"repetitions":[dict(group_id=group,topic="t",pattern="creeping",wave=0)],
              "config":{"alerts":{"critical_threshold":50}}}
    monkeypatch.setattr(comparison,"verify",lambda path:manifest)
    (tmp_path/"generator").mkdir()
    (tmp_path/"generator/g.jsonl").write_text("\n".join(json.dumps(dict(produced=10,consumed=0,timestamp=time)) for time in
        ["2026-01-01T00:00:00+00:00","2026-01-01T00:01:00+00:00"]))
    history=[dict(id=str(i),cluster_name="c",group_id=group,topic="t",total_lag="10",recorded_at=f"2026-01-01T00:00:{second:02d}+00:00")
             for i,second in [(1,5),(2,10)]]
    forecasts=[dict(id=r["id"],cluster_name="c",group_id=group,topic="t",recorded_at=r["recorded_at"],input_ids_json=json.dumps([int(r["id"])])) for r in history]
    monkeypatch.setattr(comparison,"csv_rows",lambda path:iter(history if path.name=="lag_history.csv" else forecasts))
    assert len(comparison.context(tmp_path,"selection")[2])==1
    manifest["repetitions"][0]["wave"]=1
    assert len(comparison.context(tmp_path,"heldout")[2])==2



def test_pending_sampling_protocol_cannot_unlock_heldout(tmp_path,monkeypatch):
    import pytest
    import yaml
    from bench import model_comparison as comparison
    raw=yaml.safe_dump(dict(status="pending_selection",selection_sampling=comparison.SELECTION_SAMPLING)).encode()
    params=tmp_path/"params.yml"
    params.write_bytes(raw)
    monkeypatch.setattr(comparison,"PARAMS",params)
    monkeypatch.setattr(comparison.subprocess,"check_output",lambda *a,**kw:raw)
    with pytest.raises(ValueError,match="Selection must finish"):
        comparison.committed_parameters(tmp_path/"dataset")
