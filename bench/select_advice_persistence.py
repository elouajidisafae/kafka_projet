"""Choose persistence on the archived baseline only; separate secondary check."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, verify, read_csv, write_json, timestamp
from bench.confirmation_eval import load_inputs
from bench.forecast_eval import breach, quantiles
from bench.secondary import secondary_metrics
from bench.replay import configured
from core import recommender
from core.advice_persistence import persist_stream

CANDIDATES=(1,2,3,6,12,24)
ARCHIVE='baseline-20261001T111043363413Z'
OUTPUT=ROOT/'bench/results'/ARCHIVE/'advice-persistence-selection'


def archived_rows(manifest,logged):
    base=ROOT/'bench/results'/ARCHIVE
    sources=[base/'selection/candidates/w5-t0.5/replay.json',base/'heldout-exploratory/V2/replay.json']
    saved={str(f['id']):f for path in sources for f in json.loads(path.read_text())}
    history={r['id']:r for r in read_csv(ROOT/'bench/data'/ARCHIVE/'lag_history.csv')}
    rows=[]
    for f in logged:
        raw=saved[str(f['id'])];last=history[str(json.loads(f['input_ids_json'])[-1])]
        with configured(recommender,manifest['config']):
            rules=recommender.analyze_row(f['cluster_name'],f['group_id'],f['topic'],int(last['total_lag']),last['group_state'],raw.get('trend'),raw,
                consumer_count=int(last['consumer_count']) if last['consumer_count'] else None,
                partition_count=int(last['partition_count']) if last['partition_count'] else None,apply_cap=False)
        advice=[dict(rule=r['id'].rsplit('-',1)[-1],priority=r['priority'],recorded_at=f['recorded_at'],group_id=f['group_id'],topic=f['topic'])
                for r in rules if r['id'].rsplit('-',1)[-1] in {'scale','topology'}]
        rows.append(dict(raw,id=str(f['id']),recorded_at=f['recorded_at'],group_id=f['group_id'],topic=f['topic'],advice=advice))
    return rows,{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}


def candidate_table(inputs,rows,thresholds):
    table=[]
    for n in CANDIDATES:
        grouped=defaultdict(list)
        for f in persist_stream(rows,n):grouped[f['group_id']].append(f)
        stable=[];leads=defaultdict(list);details=[]
        for group,entry in inputs.items():
            selected=grouped[group];advice=[a for f in selected for a in f['advice']]
            metrics=secondary_metrics(entry['rep'],entry['history'],selected,advice,entry['metadata'],thresholds['warning_threshold'],thresholds['critical_threshold'])
            truth=breach([( timestamp(r['recorded_at']),r['total_lag']) for r in entry['history']],thresholds['critical_threshold'])
            early=[timestamp(a['recorded_at']) for a in advice if a['priority'] in {'HIGH','MEDIUM'} and truth is not None and timestamp(a['recorded_at'])<truth]
            lead=truth-min(early) if early else None
            if entry['rep']['pattern']=='flat_high':stable.append(metrics['S1_after_fill_advice_fraction'])
            else:leads[entry['rep']['pattern']].append(lead)
            details.append(dict(rep=group,pattern=entry['rep']['pattern'],after_fill_share=metrics['S1_after_fill_advice_fraction'],lead_sec=lead))
        table.append(dict(N=n,stable_after_fill=quantiles(stable),leads={p:quantiles(v) for p,v in leads.items()},
                          pooled_lead=quantiles([x for v in leads.values() for x in v]),per_rep=details))
    baseline=table[0]
    for r in table:
        r['lead_loss_sec']={p:None if v['median'] is None else baseline['leads'][p]['median']-v['median'] for p,v in r['leads'].items()}
        r['qualifies']=all(x is not None and x<=120 for x in r['lead_loss_sec'].values()) and all(r['leads'][p]['n']==baseline['leads'][p]['n'] for p in r['leads'])
    return table


def select():
    data=ROOT/'bench/data'/ARCHIVE;manifest=verify(data)
    _,inputs,logged=load_inputs(data,manifest)
    rows,sources=archived_rows(manifest,logged)
    table=candidate_table(inputs,rows,manifest['config']['alerts'])
    chosen=min((r for r in table if r['qualifies']),key=lambda r:(r['stable_after_fill']['median'],r['N']))['N']
    OUTPUT.mkdir(exist_ok=False)
    result=dict(chosen_N=chosen,candidates=table,selection_dataset=ARCHIVE,repetitions=len(inputs),sources=sources,
                rule='minimise median per-repetition S1 after-fill advice share, requiring per-workload median M4 lead loss <=120 sec versus N=1; no lost estimable repetitions; ties smaller N',
                statement='The chosen N is adopted as the default whatever the confirmation result. No further recording or tuning follows.')
    write_json(OUTPUT/'selection.json',result)
    print('Chosen N:',chosen)
    for r in table:print(r['N'],r['stable_after_fill']['median'],r['lead_loss_sec'],r['qualifies'])


def secondary(dataset):
    frozen=json.loads((OUTPUT/'selection.json').read_text())
    manifest=verify(dataset);_,inputs,logged=load_inputs(dataset,manifest)
    source=ROOT/'bench/results'/manifest['run_id']/'confirmation/Responsive.jsonl'
    rows=[json.loads(line) for line in source.open()]
    table=candidate_table(inputs,rows,manifest['config']['alerts'])
    target=ROOT/'bench/results'/manifest['run_id']/'advice-persistence-secondary'
    target.mkdir(exist_ok=False)
    write_json(target/'secondary.json',dict(chosen_N=frozen['chosen_N'],used_for_selection=False,candidates=table,
               source_sha256=hashlib.sha256(source.read_bytes()).hexdigest()))
    for r in table:print('secondary',r['N'],r['stable_after_fill']['median'],r['lead_loss_sec'])


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('stage',choices=['select','secondary']);p.add_argument('--dataset',type=Path)
    a=p.parse_args();select() if a.stage=='select' else secondary(a.dataset)
