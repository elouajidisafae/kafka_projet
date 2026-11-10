"""Last preregistered recording and complete two-fitter persistence comparison."""
import argparse
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, verify, write_json
from bench import confirmation as protocol
from bench import confirmation_eval as ev
from core.advice_persistence import persist_stream

TAG='revision-r1-final-frozen'
PLAN='bench/confirmation_plan_2.yml'


def guard():
    protocol.TAG=TAG;protocol.PLAN=PLAN
    return protocol.guard()


def evaluate(dataset, workers=8):
    plan,evidence=guard()
    if not 1<=workers<=64:raise ValueError('Invalid worker count')
    manifest=verify(dataset);ev.validate_design(manifest,plan,evidence)
    output=ROOT/'bench/results'/manifest['run_id']/'confirmation'
    output.mkdir(parents=True,exist_ok=False)
    write_json(output/'freeze.json',dict(**evidence,status='running',workers=workers,
        dataset_sha256=hashlib.sha256((dataset/'SHA256SUMS').read_bytes()).hexdigest()))
    history,inputs,logged=ev.load_inputs(dataset,manifest)
    raw={};names=('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS')
    previous={k:os.environ.get(k) for k in names}
    try:
        for k in names:os.environ[k]='1'
        with ExitStack() as stack:
            if workers>1:
                pool=stack.enter_context(ProcessPoolExecutor(max_workers=workers,initializer=ev.initialise,
                    initargs=(manifest['config'],history,plan['models'])))
            else:
                ev.initialise(manifest['config'],history,plan['models']);stack.callback(ev._limits.restore_original_limits)
            for model in plan['models']:
                print('Replaying final frozen fitter:',model,flush=True)
                tasks=((model,f) for f in logged)
                rows=pool.map(ev.fit_task,tasks,chunksize=8) if workers>1 else map(ev.fit_task,tasks)
                raw[model]=[]
                with (output/(model+'-raw.jsonl')).open('x',encoding='utf-8') as handle:
                    for i,row in enumerate(rows,1):
                        raw[model].append(row);handle.write(json.dumps(row,allow_nan=False)+'\n')
                        if i%2000==0:print(model,i,'/',len(logged),flush=True)
    finally:
        for k,v in previous.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v
    chosen=plan['advice_persistence']['chosen_N'];results={};modes={}
    for model,rows in raw.items():
        for label,n in ((model,chosen),(model+'_N1',1)):
            results[label]=[dict(f,model=label,forecast_persistence_cycles=n) for f in persist_stream(rows,n)]
            modes[label]='baseline' if model=='OLS' else 'multiwindow'
            with (output/(label+'.jsonl')).open('x',encoding='utf-8') as handle:
                for row in results[label]:handle.write(json.dumps(row,allow_nan=False)+'\n')
    guard()
    summary=ev.publish_aggregation(dataset,manifest,inputs,logged,results,evidence,output,
        model_modes=modes,recorded_model='OLS_N1',internal_report=ROOT/'internal/revision-r1-step7-report.md')
    # Only completed final results can be the source of displayed coverage.
    pointer=ROOT/'bench/results/final-confirmation-latest.json'
    if pointer.exists():raise ValueError('Final results pointer already exists; no repeat confirmation is permitted')
    write_json(output/'completion.json',dict(**evidence,status='complete',chosen_N=chosen,
        adopted_regardless_of_result=True,no_further_recording_or_tuning=True))
    write_json(pointer,dict(summary_path=(output/'summary.json').relative_to(ROOT).as_posix(),
        sha256=hashlib.sha256((output/'summary.json').read_bytes()).hexdigest(),
        model_modes={'baseline':'OLS','multiwindow':'Responsive'},**evidence))
    lock=json.loads((output/'freeze.json').read_text());lock['status']='complete';write_json(output/'freeze.json',lock)
    guard()
    print('FINAL confirmation complete:',output,'quality passed:',summary['quality']['passed'],flush=True)
    print('Chosen N remains',chosen,'regardless of these results. No further recording or tuning.',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',choices=['check','record','evaluate','smoke','calibrate'])
    parser.add_argument('--dataset',type=Path);parser.add_argument('--checks-dir',type=Path)
    parser.add_argument('--workers',type=int,default=8)
    args=parser.parse_args();plan,evidence=guard()
    directory=args.checks_dir or ROOT/plan['reuse_receipts']['directory']
    if args.stage=='check':
        protocol.validate_receipts(plan,evidence,directory)
        print(json.dumps(dict(**evidence,chosen_N=plan['advice_persistence']['chosen_N'],receipts='valid/reusable'),indent=2))
    elif args.stage=='record':
        protocol.validate_receipts(plan,evidence,directory)
        # One attempt, even if interrupted: never silently acquire another dataset.
        ledger=ROOT/'bench/results/final-confirmation-recording-attempt.json'
        with ledger.open('x',encoding='utf-8') as handle:json.dump(dict(**evidence,last_recording=True),handle,indent=2)
        from bench.run_forecast_validation import run
        run(arm='confirmation',profile='baseline',reps=10,waves=1,duration_min=60,receipt_dir=directory)
    elif args.stage=='evaluate':
        if args.dataset is None:parser.error('--dataset required')
        evaluate(args.dataset.resolve(),args.workers)
    else:
        # Optional fresh checks only if approved reused receipts are unavailable.
        if args.checks_dir is None:parser.error('--checks-dir required for fresh checks')
        stage='smoke' if args.stage=='smoke' else 'calibration'
        directory=protocol.check_receipt_stage(directory,stage,evidence)
        if args.stage=='smoke':
            from bench.run_forecast_validation import run
            run(arm='final-smoke',profile='smoke',reps=1,waves=1,duration_min=5,receipt_dir=directory)
        else:
            from bench.calibrate import main as calibrate
            calibrate(receipt_dir=directory)
        guard()


if __name__=='__main__':main()
