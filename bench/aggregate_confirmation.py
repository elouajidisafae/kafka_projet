"""Reproduce a sealed confirmation from saved streams only, without fitting."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, verify, write_json
from bench import confirmation_eval as ev


def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
    return h.hexdigest()


class Streams:
    def __init__(self, folder, models, logged):
        self.folder,self.models,self.logged=folder,models,{str(f['id']):f for f in logged}
        self.hashes={}
    def items(self):
        for model in self.models:
            yield model,self[model]
    def __getitem__(self, model):
        if model not in self.models: raise KeyError(model)
        path=self.folder/(model+'.jsonl'); rows=[];seen=set()
        for line in path.open(encoding='utf-8'):
            r=json.loads(line); f=self.logged[r['id']]
            assert r['id'] not in seen and r['model']==model
            assert all(r[k]==f[k] for k in ('recorded_at','group_id','topic'))
            assert r['input_ids']==json.loads(f['input_ids_json'])
            seen.add(r['id']);rows.append(r)
        assert seen==set(self.logged)
        self.hashes[path.name]=sha(path)
        return rows


def aggregate(dataset, streams, output, reference=None):
    manifest=verify(dataset)
    evidence={k:manifest['confirmation'][k] for k in ('tag','commit','plan_sha256')}
    raw=subprocess.check_output(['git','show',evidence['tag']+':bench/confirmation_plan.yml'],cwd=ROOT)
    assert hashlib.sha256(raw).hexdigest()==evidence['plan_sha256']
    plan=yaml.safe_load(raw);ev.validate_design(manifest,plan,evidence)
    for relative in ('bench/range_calibration.yml','bench/range_calibration_v2.yml'):
        assert hashlib.sha256((ROOT/relative).read_bytes().replace(b'\r\n',b'\n')).hexdigest()==plan['file_sha256'][relative]
    freeze=json.loads((streams/'freeze.json').read_text())
    assert all(freeze[k]==v for k,v in evidence.items())
    assert freeze['dataset_sha256']==sha(dataset/'SHA256SUMS')
    _,inputs,logged=ev.load_inputs(dataset,manifest)
    output.mkdir(parents=True,exist_ok=False)
    saved=Streams(streams,plan['models'],logged)
    summary=ev.publish_aggregation(dataset,manifest,inputs,logged,saved,evidence,output)
    comparisons={}
    if reference:
        old=json.loads((reference/'summary.json').read_text())
        assert summary==old, 'Summary differs; inspect without replacing existing outputs'
        for name in ('table5.csv','positive-crossing-coverage.csv','ranges.csv','stable-ranges.csv'):
            # Compare cells, preserving numeric text exactly but allowing CRLF checkout.
            import csv
            with (reference/name).open(newline='') as a,(output/name).open(newline='') as b:
                old_rows=list(csv.DictReader(a)); new_rows=list(csv.DictReader(b))
                key=lambda row:json.dumps(row,sort_keys=True)
                assert sorted(old_rows,key=key)==sorted(new_rows,key=key), name
            comparisons[name]='every cell identical'
    for name,digest in saved.hashes.items(): assert sha(streams/name)==digest
    write_json(output/'identity.json',dict(summary_identical=bool(reference),tables=comparisons,
        streams_sha256=saved.hashes,source_freeze_sha256=sha(streams/'freeze.json'),
        reference=str(reference),fitting_performed=False))
    print('Tracked aggregation complete; summary and all CSV table cells identical:',bool(reference))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dataset','streams','output'): p.add_argument('--'+key,type=Path,required=True)
    p.add_argument('--reference',type=Path)
    a=p.parse_args();aggregate(a.dataset,a.streams,a.output,a.reference)
