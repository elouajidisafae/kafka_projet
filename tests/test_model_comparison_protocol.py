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
