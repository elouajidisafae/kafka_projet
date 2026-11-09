"""Single frozen-model evaluation; no selection, retries, or parameter writes."""
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
from time import perf_counter

import numpy as np
import yaml
from bench.common import ROOT, read_csv, timestamp, verify, write_csv, write_json
from bench.forecast_eval import breach, horizon_bin, quantiles, score_rep
from bench.model_comparison_alignment import aligned_model
from bench.replay import Baseline, configured, history_row, unique_forecast_rows, recommendation_observations
from bench.secondary import secondary_metrics
from core.range_groups import factor_for

_config = _history = _models = _limits = None


def initialise(config, history, models):
    global _config, _history, _models, _limits
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        from statsmodels.tsa.arima.model import ARIMA  # warm imports before limiting BLAS
    from threadpoolctl import threadpool_limits
    _limits = threadpool_limits(limits=1)
    _config, _history, _models = config, history, models


def fit_task(task):
    name, logged = task
    from bench.variants import Variant
    from bench.comparison_models import OfflineModel
    from core import recommender
    records = [_history[i] for i in json.loads(logged['input_ids_json'])]
    spec = _models[name]
    if name == 'OLS':
        model = Baseline(_config)
    elif name == 'Responsive':
        model = Variant(_config, 'V2', spec['short_window_minutes'], spec['agreement_tolerance'])
    elif spec['family'] == 'Holt':
        model = OfflineModel(_config, 'Holt', alpha=spec['alpha'], beta=spec['beta'])
    else:
        p, d, q = spec['order']
        if d != 1:
            raise ValueError('Only frozen d=1 adapters are supported')
        model = OfflineModel(_config, 'ARIMA', p=p, q=q)
    started = perf_counter()
    f = model.fit(records, datetime.fromisoformat(logged['recorded_at']), _config['alerts'])
    duration = perf_counter()-started
    advice = []
    if name in {'OLS', 'Responsive'}:
        row = records[-1]
        with configured(recommender, _config):
            rules = recommender.analyze_row(row['cluster_name'], row['group_id'], row['topic'],
                row['total_lag'], row['group_state'], f.get('trend'), f,
                consumer_count=row.get('consumer_count'), partition_count=row.get('partition_count'),
                apply_cap=False)
        advice = [dict(rule=r['id'].rsplit('-',1)[-1], priority=r['priority'],
                       recorded_at=logged['recorded_at'], group_id=row['group_id'], topic=row['topic'])
                  for r in rules if r['id'].rsplit('-',1)[-1] in {'scale','topology'}]
    differences = {}
    if name == 'OLS':
        for field in ('slope','intercept','r_squared','eta_critical_sec'):
            value = int(logged[field]) if field == 'eta_critical_sec' else float(logged[field])
            if f.get(field) != value:
                differences[field] = dict(recorded=value, replayed=f.get(field))
    return dict(id=str(logged['id']), recorded_at=logged['recorded_at'], group_id=logged['group_id'],
                topic=logged['topic'], input_ids=json.loads(logged['input_ids_json']), model=name,
                **{k:v for k,v in f.items() if k not in {'group_id','topic'}},
                duration_seconds=duration, advice=advice, ols_differences=differences)


def load_inputs(dataset, manifest):
    history = {int(r['id']):history_row(r) for r in read_csv(dataset/'lag_history.csv')}
    logged = unique_forecast_rows(read_csv(dataset/'forecast_log.csv'))
    inputs, canonical = {}, []
    for rep in manifest['repetitions']:
        raw = [json.loads(line) for line in (dataset/'generator'/(rep['group_id']+'.jsonl')).read_text().splitlines()]
        metadata = next(r for r in raw if r.get('kind') == 'metadata')
        generator = [r for r in raw if 'produced' in r]
        start, end = timestamp(generator[0]['timestamp']), timestamp(generator[-1]['timestamp'])
        rows = [r for r in history.values() if r['group_id'] == rep['group_id'] and r['topic'] == rep['topic']
                and start <= timestamp(r['recorded_at']) <= end]
        inputs[rep['group_id']] = dict(rep=rep, metadata=metadata, generator=generator,
                                      history=rows, start=start, end=end)
        for f in logged:
            if f['group_id'] != rep['group_id'] or f['topic'] != rep['topic'] or not start <= timestamp(f['recorded_at']) <= end:
                continue
            ids = json.loads(f['input_ids_json'])
            records = [history[i] for i in ids]
            if not records or any((r['cluster_name'],r['group_id'],r['topic']) !=
                                  (f['cluster_name'],f['group_id'],f['topic']) for r in records):
                raise ValueError('Invalid exact-input provenance')
            canonical.append(f)
    return history, inputs, sorted(canonical, key=lambda f:(f['recorded_at'],int(f['id'])))


def positive_coverage(inputs, forecasts, critical):
    """Availability of a positive crossing, separate from conditional ETA error."""
    grouped = defaultdict(list)
    for f in forecasts:
        grouped[f['group_id']].append(f)
    reps = []
    for group, entry in inputs.items():
        pattern = entry['rep']['pattern']
        if pattern == 'flat_high':
            continue
        truth = breach([(timestamp(r['recorded_at']),r['total_lag']) for r in entry['history']],critical)
        for label in (('after-burst 0-15',) if pattern == 'burst' else ('0-5','5-15','15-30')):
            eligible = []
            for f in grouped[group]:
                time = timestamp(f['recorded_at'])
                if truth is None or time >= truth:
                    continue
                if pattern == 'burst':
                    rates = entry['metadata']['rates']
                    cutoff = timestamp(entry['metadata']['started_at'])+rates['burst_at']+rates['burst_seconds']
                    if time < cutoff or truth-time > 900:
                        continue
                elif horizon_bin(truth-time) != label:
                    continue
                eligible.append(f)
            positive = sum(f.get('enough_data',False) and f.get('eta_critical_sec') is not None and f['eta_critical_sec'] > 0 for f in eligible)
            available = sum(f.get('enough_data',False) and f.get('eta_critical_sec') is not None for f in eligible)
            reps.append(dict(rep=group, pattern=pattern, horizon=label, n=len(eligible), available=available,
                             positive=positive, fraction=positive/len(eligible) if eligible else None))
    cells = []
    for pattern, label in sorted({(r['pattern'],r['horizon']) for r in reps}):
        values = [r for r in reps if (r['pattern'],r['horizon']) == (pattern,label)]
        n, positive = sum(r['n'] for r in values), sum(r['positive'] for r in values)
        cells.append(dict(pattern=pattern,horizon=label,origins=n,positive=positive,
                          pooled_fraction=positive/n if n else None, **quantiles([r['fraction'] for r in values])))
    return dict(per_rep=reps, table=cells)


def range_metrics(inputs, forecasts, mode, v1, v2, critical):
    crossing, stable = [], []
    for group, entry in inputs.items():
        truth = breach([(timestamp(r['recorded_at']),r['total_lag']) for r in entry['history']],critical)
        for f in forecasts:
            if f['group_id'] != group:
                continue
            eta = f.get('eta_critical_sec')
            if entry['rep']['pattern'] == 'flat_high':
                stable.append(f)
            if truth is None or timestamp(f['recorded_at']) >= truth or eta is None or not 0 < eta <= 1800:
                continue
            crossing.append(dict(pattern=entry['rep']['pattern'], rep=group,
                remaining=truth-timestamp(f['recorded_at']), predicted=eta,
                confidence=f.get('confidence'), window_seconds=f.get('window_seconds')))
    def bounds(f, version):
        eta = f.get('predicted',f.get('eta_critical_sec'))
        if eta is None or eta <= 0:
            return None
        if version == 'v1':
            factor = v1['modes'][mode][horizon_bin(eta)]
        else:
            factor = factor_for(v2['modes'][mode],eta,f.get('confidence'),f.get('window_seconds'))
        return (factor['q05']*eta, factor['q95']*eta) if factor and factor['n'] else None
    table = []
    for pattern in ('burst','creeping','sawtooth','pooled'):
        for label in ('0-5','5-15','15-30','0-30'):
            selected = [f for f in crossing if (pattern == 'pooled' or f['pattern'] == pattern)
                        and (label == '0-30' or horizon_bin(f['predicted']) == label)]
            for version in ('v1','v2'):
                intervals = [(f,bounds(f,version)) for f in selected]
                shown = [(f,b) for f,b in intervals if b is not None]
                count = sum(b[0] <= f['remaining'] <= b[1] for f,b in shown)
                table.append(dict(pattern=pattern,bin=label,version=version,n=len(selected),range_n=len(shown),covered=count,
                    coverage=count/len(selected) if selected else None,
                    median_width_sec=float(np.median([b[1]-b[0] for _,b in shown])) if shown else None))
    flat = []
    for label in ('0-30','30+','all'):
        selected = [f for f in stable if label == 'all' or label == '0-30' and f.get('eta_critical_sec') is not None and 0 < f['eta_critical_sec'] <= 1800
                    or label == '30+' and f.get('eta_critical_sec') is not None and f['eta_critical_sec'] > 1800]
        for version in ('v1','v2'):
            count = sum(bounds(f,version) is not None for f in selected)
            flat.append(dict(bin=label,version=version,n=len(selected),range_n=count,share=count/len(selected) if selected else None))
    return dict(table=table, stable_backlog=flat)


def summarize(inputs, rows, thresholds, advice_applicable, recorded_advice=None):
    aligned = aligned_model(inputs,rows,thresholds)
    positive = positive_coverage(inputs,rows,thresholds['critical_threshold'])
    per_rep, secondary = [], []
    for group, entry in inputs.items():
        selected = [f for f in rows if f['group_id'] == group]
        advice = [r for f in selected for r in f['advice']]
        if recorded_advice is not None:
            advice = [r for r in recorded_advice if r['group_id']==group and r['topic']==entry['rep']['topic']
                      and entry['start'] <= timestamp(r['recorded_at']) <= entry['end']]
        # Missing model predictions remain in availability denominators; never turn
        # a failed ARIMA fit into a fabricated no-crossing sentinel.
        estimable = [f for f in selected if f.get('eta_critical_sec') is not None]
        metrics, _, truth, independent = score_rep(entry['rep'], entry['history'], estimable, advice,
                                                entry['generator'], thresholds['critical_threshold'])
        metrics = {k:v for k,v in metrics.items() if k.startswith(('M1','M2','M4','M6','M7'))}
        extra = secondary_metrics(entry['rep'],entry['history'],estimable,advice,entry['metadata'],
                                  thresholds['warning_threshold'],thresholds['critical_threshold'])
        for key,value in extra.items():
            secondary.append(dict(rep=group,pattern=entry['rep']['pattern'],metric=key,value=value))
        per_rep.append(dict(rep=group,pattern=entry['rep']['pattern'],forecasts=len(selected),
            available=sum(f.get('enough_data',False) and f.get('eta_critical_sec') is not None for f in selected),
            fit_failures=sum(f.get('fit_failed',False) for f in selected),
            advice_applicable=advice_applicable,metrics=metrics if advice_applicable else
            {k:v for k,v in metrics.items() if not k.startswith(('M4','M6'))}))
    cells = []
    for pattern in ('burst','creeping','sawtooth','flat_high'):
        values = [r for r in per_rep if r['pattern'] == pattern]
        keys = sorted({k for r in values for k in r['metrics']})
        for key in keys:
            cells.append(dict(pattern=pattern,metric=key,**quantiles([r['metrics'].get(key) for r in values])))
    secondary_cells = []
    for pattern,key in sorted({(r['pattern'],r['metric']) for r in secondary}):
        if not advice_applicable and (key.startswith(('S1','S2','S3'))):
            continue
        secondary_cells.append(dict(pattern=pattern,metric=key,**quantiles([r['value'] for r in secondary if r['pattern']==pattern and r['metric']==key])))
    return dict(table5=aligned,positive_crossing_coverage=positive,per_rep=per_rep,
                metrics=cells,secondary_per_rep=secondary,secondary=secondary_cells,
                fit_failures=sum(r.get('fit_failed',False) for r in rows),
                missing=sum(not r.get('enough_data',False) or r.get('eta_critical_sec') is None for r in rows),
                ols_field_mismatches=sum(bool(r['ols_differences']) for r in rows),
                computation_seconds=quantiles([r['duration_seconds'] for r in rows]),
                gated_advice_applicable=advice_applicable)


def validate_design(manifest, plan, evidence):
    recorded = manifest.get('confirmation',{})
    if any(recorded.get(k) != v for k,v in evidence.items()):
        raise ValueError('Dataset plan hash/tag does not match the frozen confirmation')
    if manifest.get('status') != 'complete' or manifest.get('arm') != 'confirmation' or manifest.get('dirty') or manifest.get('commit') != evidence['commit']:
        raise ValueError('Not a completed clean confirmation recording')
    config = deepcopy(manifest['config']); config.pop('include_groups',None)
    if config != plan['recording']['config']:
        raise ValueError('Recording configuration differs from the frozen plan')
    reps = manifest['repetitions']
    if len(reps) != 40 or manifest['requested_duration_seconds'] != 3600:
        raise ValueError('Expected 40 repetitions in one 60-minute wave')
    seeds = [r['seed'] for r in reps]
    if len(set(seeds)) != 40 or set(seeds) & set(plan['recording']['excluded_seeds']):
        raise ValueError('Seeds are duplicated or overlap the archived baseline')
    for pattern,rates in plan['recording']['workloads'].items():
        group = [r for r in reps if r['pattern'] == pattern]
        if len(group) != 10 or any(r['wave'] != 0 or r['rates'] != rates for r in group):
            raise ValueError('Wrong workload design')


def report_tables(summary):
    text = '# Frozen confirmation results\n\n'
    text += 'Table 5 errors: median [Q1,Q3] of per-repetition medians; nulls excluded and counted.\n\n'
    text += '| Model | Workload | Horizon | Median error (s) | Q1 | Q3 | Repetitions |\n|---|---|---|---:|---:|---:|---:|\n'
    show = lambda v: 'N/A' if v is None else str(round(v,6)) if isinstance(v,float) else str(v)
    for model,values in summary['models'].items():
        for r in values['table5']['table']:
            text += '| '+' | '.join(show(x) for x in (model,r['pattern'],r['horizon'],r['median'],r['q1'],r['q3'],r['n']))+' |\n'
    for title,key in [('Positive crossing coverage','positive_crossing_coverage'),('Stable false crossings within 30 minutes','table5')]:
        text += '\n## '+title+'\n\n'
        if key == 'positive_crossing_coverage':
            text += '| Model | Workload | Horizon | Origins | Positive | Pooled fraction | Median per-rep fraction | Q1 | Q3 |\n|---|---|---|---:|---:|---:|---:|---:|---:|\n'
            for model,v in summary['models'].items():
                for r in v[key]['table']:
                    text += '| '+' | '.join(show(x) for x in (model,r['pattern'],r['horizon'],r['origins'],r['positive'],r['pooled_fraction'],r['median'],r['q1'],r['q3']))+' |\n'
        else:
            text += '| Model | Median per-rep fraction | Q1 | Q3 | Repetitions |\n|---|---:|---:|---:|---:|\n'
            for model,v in summary['models'].items():
                r=v[key]['flat_fraction'];text+='| '+' | '.join(show(x) for x in (model,r['median'],r['q1'],r['q3'],r['n']))+' |\n'
    text += '\n## Gated advice and lead time\n\nHolt/ARIMA advice: N/A (no archived R2 gate for these models).\n\n| Model | Workload | Measure | Median | Q1 | Q3 | Repetitions |\n|---|---|---|---:|---:|---:|---:|\n'
    for model,v in {**summary['models'],'OLS recorded advice':summary['recorded_OLS_advice']}.items():
        if not v['gated_advice_applicable']:
            continue
        for r in v['metrics']+v['secondary']:
            if r.get('metric','') in {'M6_false_alarm_cycle_fraction','M4_advice_lead_seconds','S1_after_fill_advice_fraction','S1_fill_advice_fraction','S2_critical_minus_advice'}:
                text+='| '+' | '.join(show(x) for x in (model,r['pattern'],r['metric'],r['median'],r['q1'],r['q3'],r['n']))+' |\n'
    text += '\n## Calibrated ranges (predicted horizons up to 30 minutes)\n\n| Model | Workload | Bin | Version | n | Ranges | Covered | Coverage | Median width (s) |\n|---|---|---|---|---:|---:|---:|---:|---:|\n'
    for model,v in summary['ranges'].items():
        for r in v['table']:
            text+='| '+' | '.join(show(x) for x in (model,r['pattern'],r['bin'],r['version'],r['n'],r['range_n'],r['covered'],r['coverage'],r['median_width_sec']))+' |\n'
    text += '\n## Stable backlog ranges\n\n| Model | Predicted bin | Version | n | Range n | Share |\n|---|---|---|---:|---:|---:|\n'
    for model,v in summary['ranges'].items():
        for r in v['stable_backlog']:
            text+='| '+' | '.join(show(x) for x in (model,r['bin'],r['version'],r['n'],r['range_n'],r['share']))+' |\n'
    text += '\n## Availability / failures\n\n| Model | Missing forecasts | Fit failures | OLS field mismatches |\n|---|---:|---:|---:|\n'
    for model,v in summary['models'].items():
        text+=f"| {model} | {v['missing']} | {v['fit_failures']} | {v['ols_field_mismatches']} |\n"
    text += '\nRecording quality checks: '+json.dumps(summary['quality'],sort_keys=True)+'\n'
    return text


def evaluate(dataset, plan, evidence, workers=8):
    from bench.confirmation import guard
    frozen_plan, current = guard()  # Also protect direct Python callers, before dataset access.
    if plan != frozen_plan or current != evidence or not 1 <= workers <= 64:
        raise ValueError('Wrong freeze evidence or invalid worker count')
    manifest = verify(dataset)
    validate_design(manifest,plan,evidence)
    output = ROOT/'bench/results'/manifest['run_id']/'confirmation'
    output.mkdir(parents=True,exist_ok=False)  # one attempt; never overwrite/reselect
    write_json(output/'freeze.json',dict(**evidence,dataset_sha256=hashlib.sha256((dataset/'SHA256SUMS').read_bytes()).hexdigest(),workers=workers,status='running',
                                       python=platform.python_version(),numpy=np.__version__))
    history, inputs, logged = load_inputs(dataset,manifest)
    config = deepcopy(manifest['config'])
    v1 = yaml.safe_load((ROOT/'bench/range_calibration.yml').read_text())
    v2 = yaml.safe_load((ROOT/'bench/range_calibration_v2.yml').read_text())
    results = {}
    names = ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS')
    previous = {k:os.environ.get(k) for k in names}
    try:
        for key in names:
            os.environ[key] = '1'
        with ExitStack() as stack:
            if workers > 1:
                pool = stack.enter_context(ProcessPoolExecutor(max_workers=workers,initializer=initialise,initargs=(config,history,plan['models'])))
            else:
                initialise(config,history,plan['models'])
                stack.callback(_limits.restore_original_limits)
            for model in plan['models']:
                print('Replaying frozen model:',model,flush=True)
                tasks = ((model,f) for f in logged)
                fitted = pool.map(fit_task,tasks,chunksize=8) if workers > 1 else map(fit_task,tasks)
                rows = []
                with (output/(model+'.jsonl')).open('x',encoding='utf-8') as handle:
                    for index,row in enumerate(fitted,1):
                        rows.append(row);handle.write(json.dumps(row,allow_nan=False)+'\n')
                        if index % 2000 == 0:
                            print(model,index,'/',len(logged),flush=True)
                results[model] = rows
    finally:
        for key,value in previous.items():
            if value is None:
                os.environ.pop(key,None)
            else:
                os.environ[key] = value
    guard()  # Never publish a complete evaluation under modified source/settings.
    summary = dict(**evidence,run_id=manifest['run_id'],origins=len(logged),models={},ranges={})
    for model,rows in results.items():
        summary['models'][model] = summarize(inputs,rows,config['alerts'],model in {'OLS','Responsive'})
        if model in {'OLS','Responsive'}:
            summary['ranges'][model] = range_metrics(inputs,rows,'baseline' if model=='OLS' else 'multiwindow',v1,v2,config['alerts']['critical_threshold'])
    raw_forecasts = read_csv(dataset/'forecast_log.csv')
    raw_advice = read_csv(dataset/'recommendation_log.csv')
    identities, observed = recommendation_observations(raw_forecasts,raw_advice)
    advice_mismatches = []
    for row in results['OLS']:
        expected = {(r['rule'],r['priority']) for r in row['advice']}
        if expected != observed[identities[row['id']]]:
            advice_mismatches.append(dict(id=row['id'],expected=sorted(expected),recorded=sorted(observed[identities[row['id']]])))
    summary['recorded_OLS_advice'] = summarize(inputs,results['OLS'],config['alerts'],True,recorded_advice=raw_advice)
    summary['advice_mismatches'] = advice_mismatches
    checks = []
    for group,entry in inputs.items():
        _,_,truth,independent = score_rep(entry['rep'],entry['history'],[],[],entry['generator'],config['alerts']['critical_threshold'])
        final = entry['generator'][-1]
        drift = {k:abs(final.get(k,0)-final.get('expected_'+k,0))/max(final.get('expected_'+k,0),1) for k in ('produced','consumed')}
        checks.append(dict(rep=group,pattern=entry['rep']['pattern'],truth=truth,generator_truth=independent,
            truth_match=truth is not None and independent is not None and abs(truth-independent)<=5,
            flat_crossed=entry['rep']['pattern']=='flat_high' and truth is not None,
            starts_at_zero=entry['generator'][0]['lag']==0,relative_rate_drift=drift,
            commit_pass=final.get('max_commit_gap_seconds',float('inf'))<=1,
            rate_pass=all(x<=.05 for x in drift.values())))
    crossing = [r for r in checks if r['pattern']!='flat_high']
    truth_fraction = sum(r['truth_match'] for r in crossing)/len(crossing)
    ols_mismatches = summary['models']['OLS']['ols_field_mismatches']
    summary['quality'] = dict(truth_match_fraction=truth_fraction,ols_field_mismatches=ols_mismatches,advice_mismatches=len(advice_mismatches),
        passed=truth_fraction>=.95 and not ols_mismatches and not advice_mismatches and all(not r['flat_crossed'] and r['starts_at_zero'] and r['rate_pass'] and r['commit_pass'] for r in checks))
    summary['recording_checks'] = checks
    write_json(output/'summary.json',summary)
    write_csv(output/'table5.csv',[dict(model=m,**r) for m,v in summary['models'].items() for r in v['table5']['table']])
    write_csv(output/'positive-crossing-coverage.csv',[dict(model=m,**r) for m,v in summary['models'].items() for r in v['positive_crossing_coverage']['table']])
    write_csv(output/'ranges.csv',[dict(model=m,**r) for m,v in summary['ranges'].items() for r in v['table']])
    write_csv(output/'stable-ranges.csv',[dict(model=m,**r) for m,v in summary['ranges'].items() for r in v['stable_backlog']])
    report = report_tables(summary)
    (output/'report.md').write_text(report,encoding='utf-8')
    internal = ROOT/'internal/revision-r1-step6-report.md';internal.parent.mkdir(exist_ok=True)
    with internal.open('a',encoding='utf-8') as handle:
        handle.write('\n\n## Part 5: fresh confirmation results\n\n'+report)
    lock = json.loads((output/'freeze.json').read_text());lock['status']='complete'
    write_json(output/'freeze.json',lock)
    print('Confirmation complete:',output,'quality passed:',summary['quality']['passed'],flush=True)
