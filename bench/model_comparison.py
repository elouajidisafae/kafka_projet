"""Offline comparison using exact archived replay inputs; no live or old result writes."""
import argparse
from collections import defaultdict
import csv
from datetime import datetime
import hashlib
import itertools
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
from time import perf_counter
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, timestamp, verify, write_json
from bench.forecast_eval import breach, horizon_bin, quantiles
from bench.replay import Baseline, history_row, load_forecaster, unique_forecast_rows
from bench.splits import select_repetitions

def source_hashes():
    return {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ("bench/model_comparison.py", "bench/comparison_models.py", "bench/variants.py",
             "bench/replay.py", "bench/forecast_eval.py", "core/forecasting.py")}


PARAMS = ROOT / "bench/model_comparison_params.yml"
HOLT = [dict(alpha=a, beta=b) for a,b in itertools.product((.1,.2,.3,.5,.7,.9),(.05,.1,.2,.3,.5))]
ARIMA = [dict(p=p,q=q) for p,q in itertools.product(range(3),repeat=2)]


def csv_rows(path):
    with path.open(encoding="utf-8",newline="") as handle:
        yield from csv.DictReader(handle)


def context(dataset, split, sample=False):
    manifest = verify(dataset)
    # Selection isolation uses the established split helper. Heldout parameter guard is ours.
    if split == "selection":
        reps, _ = select_repetitions(manifest, "selection", True)
    else:
        reps = [r for r in manifest["repetitions"] if r["wave"] == 1]
    if sample:
        reps = [next(r for r in reps if r["pattern"] == pattern) for pattern in sorted({r["pattern"] for r in reps})]
    groups = {r["group_id"] for r in reps}
    history = [history_row(r) for r in csv_rows(dataset/"lag_history.csv") if r["group_id"] in groups]
    by_id = {r["id"]: r for r in history}
    logged = unique_forecast_rows([r for r in csv_rows(dataset/"forecast_log.csv") if r["group_id"] in groups])
    by_group = defaultdict(list)
    for f in logged:
        by_group[f["group_id"]].append(f)
    observations = []
    for rep in reps:
        raw = [json.loads(line) for line in (dataset/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
        generator = [r for r in raw if "produced" in r]
        start,end = timestamp(generator[0]["timestamp"]),timestamp(generator[-1]["timestamp"])
        rows = [r for r in history if r["group_id"] == rep["group_id"] and start<=timestamp(r["recorded_at"])<=end]
        truth = breach([(timestamp(r["recorded_at"]),r["total_lag"]) for r in rows],manifest["config"]["alerts"]["critical_threshold"])
        for f in by_group[rep["group_id"]]:
            if not start<=timestamp(f["recorded_at"])<=end:
                continue
            records = [by_id[i] for i in json.loads(f["input_ids_json"])]
            key = (f["cluster_name"],f["group_id"],f["topic"])
            if not records or any((r["cluster_name"],r["group_id"],r["topic"]) != key for r in records):
                raise ValueError("Invalid exact-input provenance")
            observations.append(dict(rep=rep, logged=f, records=records, truth=truth))
    return manifest, reps, observations


def make_model(config, name, settings):
    if name == "OLS": return Baseline(config)
    return load_forecaster("bench.variants:"+name, dict(config, model_comparison={name:settings}))


def run_model(observations, config, name, settings, path=None):
    model = make_model(config, name, settings)
    outputs=[]
    handle = path.open("x",encoding="utf-8") if path else None
    try:
        for obs in observations:
            f = obs["logged"]
            started = perf_counter()
            result = model.fit(obs["records"],datetime.fromisoformat(f["recorded_at"]),config["alerts"])
            seconds = perf_counter()-started
            eta = result.get("eta_critical_sec")
            time = timestamp(f["recorded_at"])
            truth = obs["truth"]
            error = abs(time+eta-truth) if truth is not None and time<truth and eta is not None and eta>0 else None
            horizon = horizon_bin(truth-time) if truth is not None and time<truth else None
            differences = {}
            if name == "OLS":
                for field in ("slope","intercept","r_squared","eta_critical_sec"):
                    old = int(f[field]) if field=="eta_critical_sec" else float(f[field])
                    if result.get(field) != old:
                        differences[field] = dict(archived=old,replayed=result.get(field))
            row=dict(rep=obs["rep"]["group_id"],pattern=obs["rep"]["pattern"],forecast_id=f["id"],
                     input_ids=json.loads(f["input_ids_json"]),model=name,forecast_time=f["recorded_at"],
                     horizon=horizon,absolute_error_seconds=error,eta_critical_sec=eta,
                     duration_seconds=seconds,filled_steps=result.get("filled_steps",0),
                     fit_failed=result.get("fit_failed",False),reason=result.get("reason"),
                     available=bool(result.get("enough_data")),ols_differences=differences)
            outputs.append(row)
            if handle:
                handle.write(json.dumps(row,allow_nan=False)+"\n")
    finally:
        if handle: handle.close()
    return outputs


def metrics(rows):
    per_rep=defaultdict(list)
    for r in rows:
        if r["absolute_error_seconds"] is not None:
            per_rep[(r["pattern"],r["rep"],r["horizon"])].append(r["absolute_error_seconds"])
    cells=defaultdict(list)
    rep_metrics=[]
    for (pattern,rep,horizon),values in per_rep.items():
        value=statistics.median(values)
        cells[(pattern,horizon)].append(value)
        rep_metrics.append(dict(pattern=pattern,rep=rep,horizon=horizon,median_absolute_error_seconds=value))
    flat=defaultdict(list)
    for r in rows:
        if r["pattern"]=="flat_high": flat[r["rep"]].append(r)
    false=[]
    for rep,values in flat.items():
        valid=[r for r in values if r["available"] and r["eta_critical_sec"] is not None]
        false.append(dict(rep=rep,forecasts=len(values),valid_forecasts=len(valid),missing_forecasts=len(values)-len(valid),
                          fraction=sum(0<r["eta_critical_sec"]<=1800 for r in valid)/len(valid) if valid else None))
    return dict(horizon_errors=[dict(pattern=p,horizon=h,**quantiles(v)) for (p,h),v in sorted(cells.items())],
                per_rep=rep_metrics,stable_backlog=false,
                stable_backlog_fraction=quantiles([r["fraction"] for r in false]),
                computation_seconds=quantiles([r["duration_seconds"] for r in rows]),
                filled_steps=sum(r["filled_steps"] for r in rows),fit_failures=sum(r["fit_failed"] for r in rows),
                missing_forecasts=sum(not r["available"] for r in rows),forecasts=len(rows),
                ols_field_mismatches=sum(bool(r["ols_differences"]) for r in rows))


def new_phase(dataset, phase):
    output=ROOT/"bench/results"/dataset.name/"model-comparison"/phase
    output.mkdir(parents=True,exist_ok=False)
    return output


def selection(dataset):
    if PARAMS.exists(): raise ValueError("Parameter file already exists; refusing reselection")
    output=new_phase(dataset,"selection")
    manifest,reps,observations=context(dataset,"selection")
    candidates=[]
    for name,grid in [("Holt",HOLT),("ARIMA",ARIMA)]:
        for index,settings in enumerate(grid):
            rows=run_model(observations,manifest["config"],name,settings,output/f"{name}-{index:02d}.jsonl")
            errors=[r["absolute_error_seconds"] for r in rows if r["pattern"]!="flat_high" and r["absolute_error_seconds"] is not None]
            summary=dict(model=name,settings=settings,pooled_crossing_error=quantiles(errors),**metrics(rows))
            candidates.append(summary)
            write_json(output/f"{name}-{index:02d}-summary.json",summary)
            print(name,settings,summary["pooled_crossing_error"],"failures",summary["fit_failures"],flush=True)
    chosen={}
    for name in ("Holt","ARIMA"):
        eligible=[r for r in candidates if r["model"]==name and r["pooled_crossing_error"]["n"]]
        if not eligible: raise ValueError(f"No estimable candidate for {name}")
        winner=min(eligible,key=lambda r:r["pooled_crossing_error"]["median"])
        chosen[name]=winner["settings"]
    report=dict(dataset=dataset.name,split="wave_1",source_hashes=source_hashes(),criterion="pooled median absolute crossing-time error over all pre-breach forecasts of creeping/burst/sawtooth; missing predictions excluded and counted",repetition_ids=[r["group_id"] for r in reps],chosen=chosen,candidates=candidates)
    write_json(output/"summary.json",report)
    chosen.update(dataset=dataset.name,selection_split="wave_1",source_hashes=source_hashes(),selection_report_sha256=hashlib.sha256((output/"summary.json").read_bytes()).hexdigest())
    PARAMS.write_text(yaml.safe_dump(chosen,sort_keys=True),encoding="utf-8")
    print("STOP: review and commit bench/model_comparison_params.yml before evaluation.")


def committed_parameters(dataset):
    relative="bench/model_comparison_params.yml"
    try:
        raw=subprocess.check_output(["git","show","HEAD:"+relative],cwd=ROOT,stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as exc:
        raise ValueError("Commit bench/model_comparison_params.yml before heldout evaluation") from exc
    if raw.replace(b"\r\n",b"\n")!=PARAMS.read_bytes().replace(b"\r\n",b"\n"):
        raise ValueError("Parameters must match committed HEAD")
    params=yaml.safe_load(raw)
    if params["dataset"]!=dataset.name or params["selection_split"]!="wave_1": raise ValueError("Wrong parameter provenance")
    if params["source_hashes"] != source_hashes(): raise ValueError("Model comparison sources changed since selection")
    source=ROOT/"bench/results"/dataset.name/"model-comparison/selection/summary.json"
    if hashlib.sha256(source.read_bytes()).hexdigest()!=params["selection_report_sha256"]: raise ValueError("Selection report changed")
    return params,hashlib.sha256(raw).hexdigest()


def evaluate(dataset):
    params,param_hash=committed_parameters(dataset)
    output=new_phase(dataset,"heldout")
    manifest,reps,observations=context(dataset,"heldout")
    summaries={}
    all_rows={}
    for name in ("OLS","Holt","ARIMA"):
        rows=run_model(observations,manifest["config"],name,params.get(name,{}),output/(name+".jsonl"))
        all_rows[name]=rows
        summaries[name]=metrics(rows)
    baseline={(r["rep"],r["horizon"]):r["median_absolute_error_seconds"] for r in summaries["OLS"]["per_rep"]}
    paired={}
    for name in ("Holt","ARIMA"):
        candidate={(r["rep"],r["horizon"]):r["median_absolute_error_seconds"] for r in summaries[name]["per_rep"]}
        paired[name]=[]
        for pattern in sorted({r["pattern"] for r in reps}):
            group_ids=[r["group_id"] for r in reps if r["pattern"]==pattern]
            for horizon in ("0-5","5-15","15-30","30+"):
                values=[candidate[(rep,horizon)]-baseline[(rep,horizon)] for rep in group_ids
                        if (rep,horizon) in candidate and (rep,horizon) in baseline]
                paired[name].append(dict(pattern=pattern,horizon=horizon,better=sum(v<0 for v in values),
                                         worse=sum(v>0 for v in values),ties=sum(v==0 for v in values),
                                         paired_repetitions=len(values),total_repetitions=len(group_ids),
                                         differences_seconds=values))
    # Read the archived results only during heldout evaluation, never during selection/pilot.
    archived=[r for r in csv_rows(ROOT/"bench/results"/dataset.name/"baseline/per_rep.csv") if r["rep"] in {rep["group_id"] for rep in reps} and r["metric"].startswith("M1_abs_error_")]
    archive_differences=[]
    for r in archived:
        horizon=r["metric"].removeprefix("M1_abs_error_")
        actual=baseline.get((r["rep"],horizon))
        expected=float(r["value"]) if r["value"] else None
        if actual!=expected: archive_differences.append(dict(rep=r["rep"],horizon=horizon,archived=expected,replayed=actual))
    result=dict(dataset=dataset.name,split="wave_2",parameter_sha256=param_hash,repetition_ids=[r["group_id"] for r in reps],models=summaries,paired=paired,ols_field_mismatches=sum(bool(r["ols_differences"]) for r in all_rows["OLS"]),ols_archived_metric_differences=archive_differences,environment=dict(python=sys.version,platform=platform.platform()))
    write_json(output/"summary.json",result)
    table=["# Held-out model comparison", "", "Errors are seconds: median [Q1, Q3] across per-repetition median absolute errors.", "",
           "| Workload | Horizon (min) | Model | Median [Q1, Q3] | Repetitions |", "|---|---|---|---|---|"]
    for pattern in ("creeping","burst","sawtooth"):
        for horizon in ("0-5","5-15","15-30"):
            for name,summary in summaries.items():
                cell=next((r for r in summary["horizon_errors"] if r["pattern"]==pattern and r["horizon"]==horizon),None)
                value=f"{cell['median']:.3f} [{cell['q1']:.3f}, {cell['q3']:.3f}]" if cell else "missing"
                table.append(f"| {pattern} | {horizon} | {name} | {value} | {cell['n'] if cell else 0} |")
    table += ["", "## Paired comparisons", "", "Model minus OLS; lower is better. Missing repetitions are excluded and the paired count is reported.", "",
              "| Workload | Horizon | Model | Better | Worse | Tied | Paired / total |", "|---|---|---|---|---|---|---|"]
    for name,rows in paired.items():
        for r in rows:
            if r["horizon"]!="30+":
                table.append(f"| {r['pattern']} | {r['horizon']} | {name} | {r['better']} | {r['worse']} | {r['ties']} | {r['paired_repetitions']} / {r['total_repetitions']} |")
    table += ["", "## Runtime, gaps and failures", "", "| Model | Median seconds / forecast | Filled steps | Fit failures | Missing forecasts | Flat backlog fraction: median [Q1, Q3] |", "|---|---|---|---|---|---|"]
    for name,s in summaries.items():
        flat=s["stable_backlog_fraction"]
        table.append(f"| {name} | {s['computation_seconds']['median']} | {s['filled_steps']} | {s['fit_failures']} | {s['missing_forecasts']} | {flat['median']} [{flat['q1']}, {flat['q3']}] |")
    table += ["", f"OLS forecast-field mismatches: {result['ols_field_mismatches']}.",
              f"OLS archived per-repetition metric differences: {len(archive_differences)}. See summary.json for every difference.",
              "", "Flat backlog denominator: available forecasts, including successful non-crossing forecasts; failed/insufficient fits are missing and separately counted.",
              "Filled steps are summed per fit, including repeated use of gaps in overlapping windows."]
    (output/"tables.md").write_text("\n".join(table)+"\n",encoding="utf-8")
    print("Heldout complete:",output/"summary.json")


def pilot(dataset, points=3):
    output=new_phase(dataset,"timing")
    manifest,reps,observations=context(dataset,"selection",sample=True)
    sampled=[]
    for rep in reps:
        values=[r for r in observations if r["rep"]["group_id"]==rep["group_id"]]
        indices=sorted({int((len(values)-1)*fraction) for fraction in ([.05,.5,.95] if points==3 else [.5])})
        sampled.extend(values[i] for i in indices)
    timings=[]
    started=perf_counter()
    for name,grid in [("OLS",[{}]),("Holt",HOLT),("ARIMA",ARIMA)]:
        for settings in grid:
            rows=run_model(sampled,manifest["config"],name,settings)
            timings.append(dict(model=name,settings=settings,**metrics(rows)))
            print("pilot",name,settings,"seconds",sum(r["duration_seconds"] for r in rows),flush=True)
            if perf_counter()-started>180:
                raise RuntimeError("Pilot budget reached; preserve partial console output; no full run started")
    # Exact full counts use raw inputs of wave 1 only; no wave-2 outcome/results reads.
    _,all_reps,full=context(dataset,"selection")
    means={name:sum(t["computation_seconds"]["median"] for t in timings if t["model"]==name)/sum(t["model"]==name for t in timings) for name in ("OLS","Holt","ARIMA")}
    selection_seconds=len(full)*(30*means["Holt"]+9*means["ARIMA"])
    heldout_seconds=len(full)*sum(means.values())
    write_json(output/"summary.json",dict(repetition_ids=[r["group_id"] for r in reps],sampled_forecasts=len(sampled),full_selection_forecasts=len(full),timings=timings,selection_estimate_seconds=selection_seconds,heldout_estimate_seconds=heldout_seconds,estimate_note="Representative pilot medians; heldout assumes equal wave sizes. Allow 0.5x-3x for optimisation/failures and I/O. No second-wave results read."))
    print("Estimated selection hours:",selection_seconds/3600,"heldout hours:",heldout_seconds/3600)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase",choices=["timing","select","evaluate"])
    parser.add_argument("--dataset",type=Path,required=True)
    args=parser.parse_args()
    {"timing":pilot,"select":selection,"evaluate":evaluate}[args.phase](args.dataset.resolve())
