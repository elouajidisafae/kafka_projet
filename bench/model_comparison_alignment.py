"""Read-only Table 5 alignment and failure counts from saved forecasts; no fits."""
import argparse
from collections import defaultdict
import csv
import json
from pathlib import Path
import statistics
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, timestamp, verify
from bench.forecast_eval import breach, horizon_bin, quantiles
from bench.replay import unique_forecast_rows, history_row
from bench.secondary import secondary_metrics


def csv_rows(path):
    with path.open(encoding="utf-8",newline="") as handle:
        yield from csv.DictReader(handle)


def archive_inputs(dataset):
    manifest=verify(dataset)
    reps=[r for r in manifest["repetitions"] if r["wave"]==1]
    groups={r["group_id"] for r in reps}
    logged=unique_forecast_rows([r for r in csv_rows(dataset/"forecast_log.csv") if r["group_id"] in groups])
    history=defaultdict(list)
    for row in csv_rows(dataset/"lag_history.csv"):
        if row["group_id"] in groups: history[row["group_id"]].append(history_row(row))
    inputs={}
    canonical={}
    for rep in reps:
        logs=[json.loads(line) for line in (dataset/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
        metadata=next(r for r in logs if r.get("kind")=="metadata")
        samples=[r for r in logs if "produced" in r]
        start,end=timestamp(samples[0]["timestamp"]),timestamp(samples[-1]["timestamp"])
        rows=[r for r in history[rep["group_id"]] if start<=timestamp(r["recorded_at"])<=end]
        inputs[rep["group_id"]]=dict(rep=rep,metadata=metadata,history=rows,start=start,end=end)
        for forecast in logged:
            if forecast["group_id"]==rep["group_id"] and start<=timestamp(forecast["recorded_at"])<=end:
                canonical[str(forecast["id"])]=forecast
    return manifest,inputs,canonical


def saved_forecasts(path,canonical,comparison=True):
    if comparison:
        with path.open(encoding="utf-8") as handle:
            raw=[json.loads(line) for line in handle]
        rows=[dict(id=str(r["forecast_id"]),recorded_at=r["forecast_time"],
                   eta_critical_sec=r["eta_critical_sec"],enough_data=r["available"],
                   confidence="NOT_APPLICABLE") for r in raw]
    else:
        rows=json.loads(path.read_text())
    selected={}
    for row in rows:
        identifier=str(row["id"])
        if identifier not in canonical: continue
        if identifier in selected: raise ValueError("Duplicate saved forecast ID")
        archived=canonical[identifier]
        if row["recorded_at"]!=archived["recorded_at"]: raise ValueError("Origin timestamp changed")
        selected[identifier]=dict(row,group_id=archived["group_id"],topic=archived["topic"])
    if set(selected)!=set(canonical): raise ValueError("Saved model does not cover every canonical heldout origin")
    return list(selected.values())


def false_crossing(rows):
    available=[r for r in rows if r.get("enough_data",True) and r.get("eta_critical_sec") is not None]
    count=sum(0<float(r["eta_critical_sec"])<=1800 for r in available)
    return dict(available=len(available),total=len(rows),missing=len(rows)-len(available),
                predicted=count,fraction=count/len(available) if available else None)


def aligned_model(inputs, forecasts, thresholds):
    grouped=defaultdict(list)
    for row in forecasts: grouped[row["group_id"]].append(row)
    cells=defaultdict(list)
    per_rep=[]
    flat=[]
    for group,entry in inputs.items():
        rep,history,metadata=entry["rep"],entry["history"],entry["metadata"]
        rows=grouped[group]
        valid=[r for r in rows if r.get("enough_data",True) and r.get("eta_critical_sec") is not None]
        truth=breach([(timestamp(r["recorded_at"]),r["total_lag"]) for r in history],thresholds["critical_threshold"])
        values={}
        if rep["pattern"] in {"creeping","sawtooth"}:
            bins=defaultdict(list)
            for row in valid:
                origin=timestamp(row["recorded_at"]); eta=float(row["eta_critical_sec"])
                if truth is not None and origin<truth and eta>0:
                    bins[horizon_bin(truth-origin)].append(abs(origin+eta-truth))
            for horizon in ("0-5","5-15","15-30"):
                values[horizon]=statistics.median(bins[horizon]) if bins[horizon] else None
        elif rep["pattern"]=="burst":
            # Reuse the archived S5 phase rule, including its end-of-burst cutoff.
            secondary=secondary_metrics(rep,history,valid,[],metadata,thresholds["warning_threshold"],thresholds["critical_threshold"])
            values["after-burst 0-15"]=secondary["S5_after_M1_0-15"]
        else:
            flat.append(dict(rep=group,**false_crossing(rows)))
        for horizon,value in values.items():
            per_rep.append(dict(rep=group,pattern=rep["pattern"],horizon=horizon,value=value))
            cells[(rep["pattern"],horizon)].append(value)
    return dict(table=[dict(pattern=p,horizon=h,**quantiles(v)) for (p,h),v in sorted(cells.items())],
                per_rep=per_rep,flat_high=flat,flat_fraction=quantiles([r["fraction"] for r in flat]))


def failure_counts(path):
    counts=defaultdict(lambda:dict(origins=0,fit_failures=0,missing=0))
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row=json.loads(line); count=counts[row["pattern"]]
            count["origins"]+=1; count["fit_failures"]+=int(row["fit_failed"])
            count["missing"]+=int(not row["available"])
    return {key:dict(value,failure_rate=value["fit_failures"]/value["origins"]) for key,value in counts.items()}


def analyse(dataset):
    manifest,inputs,canonical=archive_inputs(dataset)
    folder=ROOT/"bench/results"/dataset.name
    models={}
    for name in ("OLS","Holt","ARIMA"):
        rows=saved_forecasts(folder/"model-comparison/heldout"/(name+".jsonl"),canonical)
        models[name]=aligned_model(inputs,rows,manifest["config"]["alerts"])
    rows=saved_forecasts(folder/"heldout-exploratory/V2/replay.json",canonical,False)
    models["V2 exploratory"]=aligned_model(inputs,rows,manifest["config"]["alerts"])
    selection=json.loads((folder/"model-comparison/selection/summary.json").read_text())
    first=[]
    for index,candidate in enumerate(r for r in selection["candidates"] if r["model"]=="ARIMA"):
        first.append(dict(order=[candidate["settings"]["p"],1,candidate["settings"]["q"]],
                          workloads=failure_counts(folder/"model-comparison/selection"/f"ARIMA-{index:02d}.jsonl")))
    return dict(dataset=dataset.name,models=models,first_half_arima=first,
                second_half_arima=dict(order=[2,1,2],workloads=failure_counts(folder/"model-comparison/heldout/ARIMA.jsonl")),
                heldout_origins=len(canonical),burst_rule="metadata.started_at + rates.burst_at + rates.burst_seconds; issue time >= this cutoff",
                burst_boundaries=[dict(rep=group,onset=timestamp(entry["metadata"]["started_at"])+entry["metadata"]["rates"]["burst_at"],
                                      end=timestamp(entry["metadata"]["started_at"])+entry["metadata"]["rates"]["burst_at"]+entry["metadata"]["rates"]["burst_seconds"])
                                  for group,entry in inputs.items() if entry["rep"]["pattern"]=="burst"])


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset",type=Path,required=True)
    args=parser.parse_args()
    # Stdout only: never write into archived result folders.
    print(json.dumps(analyse(args.dataset.resolve()),indent=2,allow_nan=False))
