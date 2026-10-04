"""Frozen held-out evaluation: intervals, paired results and immutable provenance."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import pytest
from bench.heldout import interval_metrics,paired_metrics,lock_evaluation
from bench.splits import frozen_parameters


def iso(t):
    return datetime.fromtimestamp(t,timezone.utc).isoformat()


def test_interval_coverage_open_end_and_missing_lower():
    forecasts=[dict(recorded_at=iso(0),eta_low_sec=50,eta_high_sec=150),
               dict(recorded_at=iso(0),eta_low_sec=110,eta_high_sec=None),
               dict(recorded_at=iso(0),eta_low_sec=20,eta_high_sec=None),
               dict(recorded_at=iso(0),eta_low_sec=None,eta_high_sec=None)]
    result=interval_metrics(forecasts,100)
    assert result['M8_coverage']==.5
    assert result['M9_relative_width']==1
    assert result['M9_open_ended_count']==2
    assert result['M8_missing_interval_count']==1
    assert interval_metrics(forecasts,None)['M8_coverage'] is None


def test_paired_differences_preserve_missing_and_error_sign():
    rows=[]
    for variant,values in [('V0',[10,-20,None]),('V3',[8,-5,.8])]:
        for metric,value in zip(['M1_abs_error_0-5','M2_signed_error_0-5','M8_coverage'],values):
            rows.append(dict(variant=variant,rep='r10',pattern='burst',metric=metric,value=value))
    result=paired_metrics(rows)['V3']['burst']
    assert result['M1_abs_error_0-5']['difference']['median']==-2
    assert result['M2_signed_error_0-5']['difference']['median']==15
    assert result['M2_signed_error_0-5']['improved']==1
    assert result['M8_coverage']['comparable']==0
    assert result['M8_coverage']['difference']['median'] is None


def test_parameter_guard_allows_only_checkout_line_endings(tmp_path,monkeypatch):
    blob=b'interval_level: 0.9\nshort_window_minutes: 15\n'
    (tmp_path/'bench').mkdir();path=tmp_path/'bench/forecaster_params.yml'
    path.write_bytes(blob.replace(b'\n',b'\r\n'))
    monkeypatch.setattr(subprocess,'check_output',lambda args,**kw:'abc\n' if kw.get('text') else blob)
    params,evidence=frozen_parameters(tmp_path)
    assert params['short_window_minutes']==15
    assert evidence['params_sha256']==hashlib.sha256(blob).hexdigest()
    path.write_bytes(blob.replace(b'15',b'10'))
    with pytest.raises(ValueError,match='differ'): frozen_parameters(tmp_path)


def test_freeze_refuses_changed_source_and_parameters(tmp_path,monkeypatch):
    from bench import heldout
    from bench.common import seal,write_json
    dataset=tmp_path/'dataset';dataset.mkdir()
    write_json(dataset/'manifest.json',dict(run_id='test'))
    seal(dataset)
    report=tmp_path/'bench/results/test/selection/report.md';report.parent.mkdir(parents=True);report.write_text('selection')
    params=dict(dataset='test',selection_report_sha256=hashlib.sha256(report.read_bytes()).hexdigest())
    provenance=dict(params_sha256='original',params_commit='abc')
    monkeypatch.setattr(heldout,'frozen_parameters',lambda root:(params,dict(provenance)))
    for name in ['bench/heldout.py','bench/variants.py','bench/replay.py','bench/forecast_eval.py','bench/secondary.py','bench/splits.py','core/forecasting.py','core/recommender.py']:
        p=tmp_path/name;p.parent.mkdir(exist_ok=True);p.write_text('source')
    first=lock_evaluation(dataset,tmp_path)[1]
    assert lock_evaluation(dataset,tmp_path)[1]==first
    provenance['params_sha256']='changed'
    with pytest.raises(ValueError,match='frozen'): lock_evaluation(dataset,tmp_path)
    provenance['params_sha256']='original'
    (tmp_path/'bench/variants.py').write_text('new source')
    with pytest.raises(ValueError,match='frozen'): lock_evaluation(dataset,tmp_path)


@pytest.mark.parametrize("exploratory", [False, True])
def test_comparison_repeat_is_byte_identical_without_refitting(tmp_path,monkeypatch,exploratory):
    from bench import heldout,replay as replay_module,forecast_eval as evaluation
    from bench.common import write_csv,write_json
    dataset=tmp_path/'data';dataset.mkdir();(dataset/'generator').mkdir()
    rep=dict(group_id='g-r10-test',topic='t',pattern='creeping',wave=1)
    manifest=dict(run_id='synthetic',repetitions=[rep])
    output=tmp_path/('heldout-exploratory' if exploratory else 'heldout');output.mkdir()
    original=tmp_path/'original-heldout.json';original.write_bytes(b'unchanged')
    provenance=dict(params_sha256='frozen',params_commit='committed')
    monkeypatch.setattr(heldout,'lock_evaluation',lambda data:(manifest,provenance,output))
    from bench import responsive
    monkeypatch.setattr(responsive,'lock_exploratory',lambda data:(manifest,provenance,output))
    monkeypatch.setattr(heldout,'select_repetitions',lambda *args:([rep],provenance))
    write_csv(dataset/'forecast_log.csv',[dict(id='1',recorded_at=iso(10),cluster_name='c',group_id=rep['group_id'],topic='t',input_ids_json='[1,2]')])
    (dataset/'generator'/(rep['group_id']+'.jsonl')).write_text('\n'.join(json.dumps(dict(timestamp=iso(t),produced=1)) for t in [0,200]))
    calls=[]
    def fake_replay(data,variant,split):
        calls.append(variant)
        d=output/('baseline' if variant=='V0' else variant);d.mkdir()
        f=dict(id='1',recorded_at=iso(10),group_id=rep['group_id'])
        if variant in ['V1','V3']: f.update(eta_low_sec=50,eta_high_sec=150)
        write_json(d/'replay.json',[f]);write_json(d/'fidelity.json',dict(passed=True))
        write_csv(d/'replayed_recommendations.csv',[],['forecast_id'])
    def fake_evaluate(data,variant,**kwargs):
        d=output/('baseline' if variant=='V0' else variant)
        write_csv(d/'per_rep.csv',[dict(rep=rep['group_id'],pattern='creeping',metric='M1_abs_error_0-5',value=10)])
        write_csv(d/'secondary_per_rep.csv',[dict(rep=rep['group_id'],pattern='creeping',metric='S3_first_advice_eta_error_seconds',value=-5)])
        return dict(patterns={'creeping':{}},secondary=dict(analysis_class='secondary'),repetitions=[dict(rep=rep['group_id'],truth=100)])
    monkeypatch.setattr(replay_module,'replay',fake_replay)
    monkeypatch.setattr(evaluation,'evaluate',fake_evaluate)
    heldout.compare(dataset, exploratory=exploratory)
    before=(output/'summary.json').read_bytes()
    heldout.compare(dataset, exploratory=exploratory)
    assert before==(output/'summary.json').read_bytes()
    assert calls==(['V0','V2'] if exploratory else ['V0','V1','V2','V3'])
    assert original.read_bytes()==b'unchanged'
    if exploratory:
        assert json.loads(before)['analysis_class']=='exploratory'
    assert json.loads(before)['repetition_ids']==[rep['group_id']]
    (output/('V2' if exploratory else 'V3')/'replay.json').write_text('[]')
    with pytest.raises(ValueError,match='receipt'): heldout.compare(dataset, exploratory=exploratory)
