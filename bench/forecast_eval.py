"""Per-repetition forecast scoring; never pool autocorrelated forecasts as replicates."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, read_csv, timestamp, verify, write_csv, write_json


def breach(samples, threshold):
    """First crossing, interpolated between bracketing (seconds, lag) points."""
    samples=sorted(samples)
    for index,(time,lag) in enumerate(samples):
        if lag>=threshold:
            if not index:
                return time
            before,value=samples[index-1]
            return before+(time-before)*(threshold-value)/(lag-value)
    return None


def quantiles(values):
    values=[float(v) for v in values if v is not None and math.isfinite(float(v))]
    if not values:
        return dict(median=None,q1=None,q3=None,n=0)
    q=statistics.quantiles(values,n=4,method="inclusive") if len(values)>1 else [values[0]]*3
    return dict(median=statistics.median(values),q1=q[0],q3=q[2],n=len(values))


def horizon_bin(seconds):
    return "0-5" if seconds<=300 else "5-15" if seconds<=900 else "15-30" if seconds<=1800 else "30+"


def score_rep(rep, history, forecasts, recommendations, generator, critical):
    truth=breach([(timestamp(r["recorded_at"]),int(r["total_lag"])) for r in history],critical)
    independent=breach([(timestamp(r["timestamp"]),r["produced"]-r["consumed"]) for r in generator],critical)
    metrics={}
    pairs=[]
    errors=defaultdict(list)
    by_confidence=defaultdict(list)
    for f in forecasts:
        time=timestamp(f["recorded_at"])
        eta=float(f["eta_critical_sec"])
        if truth is not None and time<truth and eta>0:
            error=time+eta-truth
            horizon=truth-time
            errors[horizon_bin(horizon)].append(error)
            by_confidence[f["confidence"]].append(abs(error))
            pairs.append(dict(rep=rep["group_id"],pattern=rep["pattern"],forecast_id=f["id"],forecast_time=time,truth=truth,error_seconds=error,horizon_seconds=horizon,confidence=f["confidence"]))
    for label in ("0-5","5-15","15-30","30+"):
        values=errors[label]
        metrics[f"M1_abs_error_{label}"]=statistics.median([abs(x) for x in values]) if values else None
        metrics[f"M2_signed_error_{label}"]=statistics.median(values) if values else None
    final=[f for f in forecasts if truth is not None and truth-900<=timestamp(f["recorded_at"])<truth]
    metrics["M3_missed_warning_rate"]=sum(float(f["eta_critical_sec"])==-2 or f["trend"]!="INCREASING" for f in final)/len(final) if final else None
    advice=[r for r in recommendations if r["rule"] in {"scale","topology"} and r["priority"] in {"HIGH","MEDIUM"}]
    early=[timestamp(r["recorded_at"]) for r in advice if truth is not None and timestamp(r["recorded_at"])<truth]
    metrics["M4_advice_lead_seconds"]=truth-min(early) if early else None
    for confidence in ("HIGH","MEDIUM","LOW"):
        values=by_confidence[confidence]
        metrics[f"M5_abs_error_{confidence}"]=statistics.median(values) if values else None
    metrics["M6_any_false_alarm"]=int(bool(advice)) if rep["pattern"]=="flat_high" else None
    # One logging pass per collection cycle; group/topic identify one repetition.
    affected=len({r["recorded_at"] for r in advice})
    metrics["M6_false_alarm_cycle_fraction"]=affected/len(forecasts) if rep["pattern"]=="flat_high" and forecasts else None
    metrics["M7_truth_difference_seconds"]=abs(truth-independent) if truth is not None and independent is not None else None
    return metrics,pairs,truth,independent


def publication_eligible(manifest, checks, truth_fraction, fidelity):
    """Require acceptance evidence, not just a completed baseline capture."""
    return bool(
        manifest.get("arm") == "baseline" and manifest.get("profile") == "baseline"
        and manifest.get("status") == "complete" and manifest.get("dirty") is False
        and checks and truth_fraction is not None and truth_fraction >= .95
        and all(r["rate_pass"] and r["commit_pass"] and r["starts_at_zero"]
                and not r["calibration_failure"] for r in checks)
        and fidelity.get("passed")
        and fidelity.get("exact_forecast_fraction", 0) >= .99
        and fidelity.get("exact_recommendation_fraction", 0) >= .99
        and not fidelity.get("forecast_mismatches")
        and not fidelity.get("recommendation_mismatches"))


def evaluate(dataset,forecaster="baseline",source="live",plot=True,secondary=False,split=None,parameters=None):
    dataset=Path(dataset)
    manifest=verify(dataset)
    from copy import deepcopy
    from bench.splits import select_repetitions, frozen_parameters
    candidate = forecaster not in {"baseline", "V0"}
    provenance = {}
    if split or candidate:
        manifest["repetitions"], provenance = select_repetitions(manifest, split, candidate)
    config = deepcopy(manifest["config"])
    if split == "heldout":
        from bench.heldout import lock_evaluation
        lock_evaluation(dataset)
        parameters, _ = frozen_parameters()
    if split == "heldout-exploratory":
        from bench.responsive import lock_exploratory, responsive_parameters
        lock_exploratory(dataset)
        parameters, _ = responsive_parameters()
        if forecaster not in {"V0", "baseline", "V2"}:
            raise ValueError("Exploratory comparison is V2 versus V0 only")
    if parameters:
        config["forecast"].update(parameters)
    history=read_csv(dataset/"lag_history.csv")
    from bench.replay import load_forecaster, unique_forecast_rows
    model_name=load_forecaster(forecaster,config).name
    output=ROOT/"bench/results"/manifest["run_id"]
    if split:
        output=output/split
    output=output/model_name
    output.mkdir(parents=True,exist_ok=True)
    if source=="live":
        if forecaster not in {"baseline","V0"}:
            raise ValueError("Live logs describe baseline only")
        forecasts=read_csv(dataset/"forecast_log.csv")
        recommendations=read_csv(dataset/"recommendation_log.csv")
    else:
        forecasts=json.loads((output/"replay.json").read_text())
        recommendations=read_csv(output/"replayed_recommendations.csv")
    # Browser polls must not give repeated observations extra statistical weight.
    original_forecasts = read_csv(dataset/"forecast_log.csv")
    canonical_ids = {r["id"] for r in unique_forecast_rows(original_forecasts)}
    groups = {r["group_id"] for r in manifest["repetitions"]}
    forecasts = [f for f in forecasts if f["group_id"] in groups]
    original_count = len(forecasts)
    forecasts = [f for f in forecasts if str(f["id"]) in canonical_ids]
    if source != "live":
        recommendations = [r for r in recommendations if str(r["forecast_id"]) in canonical_ids]
    if secondary and not split:
        output = ROOT/"bench/results"/manifest["run_id"]/(model_name+"-secondary")
        output.mkdir(parents=True, exist_ok=True)
    secondary_rows = []
    secondary_grouped = defaultdict(lambda: defaultdict(list))
    per_rep=[]
    pairs=[]
    grouped=defaultdict(lambda:defaultdict(list))
    checks=[]
    interval=manifest["config"]["monitor"]["refresh_interval"]
    for rep in manifest["repetitions"]:
        select=lambda rows:[r for r in rows if r["group_id"]==rep["group_id"] and r["topic"]==rep["topic"]]
        logs=[json.loads(line) for line in (dataset/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
        metadata = next((r for r in logs if r.get("kind") == "metadata"), None)
        logs=[r for r in logs if "produced" in r]
        start, end = timestamp(logs[0]["timestamp"]), timestamp(logs[-1]["timestamp"])
        within = lambda rows: [r for r in select(rows) if start <= timestamp(r["recorded_at"]) <= end]
        metrics,rep_pairs,truth,independent=score_rep(rep,within(history),within(forecasts),within(recommendations),logs,manifest["config"]["alerts"]["critical_threshold"])
        if secondary:
            from bench.secondary import secondary_metrics
            if metadata is None:
                raise ValueError("Secondary phase analysis requires generator metadata")
            values = secondary_metrics(rep, within(history), within(forecasts), within(recommendations),
                                       metadata, manifest["config"]["alerts"]["warning_threshold"],
                                       manifest["config"]["alerts"]["critical_threshold"])
            for name, value in values.items():
                secondary_rows.append(dict(analysis_class="secondary", rep=rep["group_id"],
                                           pattern=rep["pattern"], metric=name, value=value))
                secondary_grouped[rep["pattern"]][name].append(value)
        pairs.extend(rep_pairs)
        for name,value in metrics.items():
            per_rep.append(dict(rep=rep["group_id"],pattern=rep["pattern"],metric=name,value=value))
            grouped[rep["pattern"]][name].append(value)
        duration=logs[-1]["elapsed"] if logs else 0
        final=logs[-1] if logs else {}
        drift={name: abs(final.get(name,0)-final.get("expected_"+name,0))/final.get("expected_"+name,1) for name in ("produced","consumed")}
        checks.append(dict(rep=rep["group_id"],pattern=rep["pattern"],truth=truth,generator_truth=independent,
            calibration_failure=rep["pattern"]=="flat_high" and truth is not None,
            truth_within_interval=truth is not None and independent is not None and abs(truth-independent)<=interval,
            duration_seconds=duration,relative_rate_drift=drift,rate_pass=all(x<=.05 for x in drift.values()),
            commit_gap_seconds=final.get("max_commit_gap_seconds"),commit_pass=final.get("max_commit_gap_seconds",float("inf"))<=1,
            starts_at_zero=bool(logs) and logs[0]["lag"]==0))
    breaching=[r for r in checks if r["truth"] is not None]
    truth_fraction=sum(r["truth_within_interval"] for r in breaching)/len(breaching) if breaching else None
    fidelity_path=ROOT/"bench/results"/manifest["run_id"]/"baseline"/"fidelity.json"
    fidelity=json.loads(fidelity_path.read_text()) if fidelity_path.exists() else {}
    summary=dict(run_id=manifest["run_id"],forecaster=forecaster,source=source,smoke=manifest["profile"]=="smoke",
        publishable=not candidate and split is None and publication_eligible(manifest,checks,truth_fraction,fidelity),
        forecast_sampling="earliest persisted row per pair and ordered input IDs",
        forecast_rows=original_count, unique_forecast_observations=len(forecasts),
        resolution_floor_seconds=interval,error_sign="positive means predicted later than reality",
        aggregation="per-repetition metrics, then median [Q1,Q3] across repetitions; null means not estimable",
        patterns={pattern:{name:quantiles(values) for name,values in metrics.items()} for pattern,metrics in grouped.items()},
        repetitions=checks,truth_match_fraction=truth_fraction,
        truth_outliers=[r["rep"] for r in breaching if not r["truth_within_interval"]],
        calibration_failures=[r["rep"] for r in checks if r["calibration_failure"]])
    summary.update(split=split, repetition_ids=sorted(groups), **provenance)
    if secondary:
        summary["analysis_class"] = "secondary"
        summary["secondary"] = dict(analysis_class="secondary",
            repetition_ids=[r["group_id"] for r in manifest["repetitions"]],
            patterns={pattern:{name:quantiles(values) for name,values in metrics.items()}
                      for pattern,metrics in secondary_grouped.items()})
        if split == "heldout-exploratory":
            for row in secondary_rows:
                row["analysis_class"] = "exploratory"
        write_csv(output/"secondary_per_rep.csv", secondary_rows,
                  ["analysis_class","rep","pattern","metric","value"])
    if split == "heldout-exploratory":
        summary["analysis_class"] = "exploratory"
        if secondary:
            summary["secondary"]["analysis_class"] = "exploratory"
    write_json(output/"summary.json",summary)
    write_csv(output/"per_rep.csv",per_rep,["rep","pattern","metric","value"])
    write_csv(output/"pairs.csv",pairs,["rep","pattern","forecast_id","forecast_time","truth","error_seconds","horizon_seconds","confidence"])
    if plot:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig,axes=plt.subplots(2,2,figsize=(10,7),sharex=True,sharey=True)
        for ax,pattern in zip(axes.flat,sorted(grouped)):
            rows=[r for r in pairs if r["pattern"]==pattern]
            ax.scatter([r["horizon_seconds"]/60 for r in rows],[abs(r["error_seconds"])/60 for r in rows],s=5,alpha=.35)
            ax.set_title(pattern)
            ax.set_xlabel("Horizon (minutes)")
            ax.set_ylabel("Absolute error (minutes)")
        fig.tight_layout()
        fig.savefig(output/"error_vs_horizon.png",dpi=180)
        plt.close(fig)
    return summary


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--dataset",type=Path,required=True)
    parser.add_argument("--forecaster",default="baseline")
    parser.add_argument("--source",choices=["live","replay"],default="live")
    parser.add_argument("--secondary", action="store_true", help="Write post-baseline analyses to a separate result directory")
    parser.add_argument("--split",choices=["selection","heldout"])
    args=parser.parse_args()
    if "," in args.forecaster:
        if args.split != "heldout" or args.forecaster.split(",") != ["V0","V1","V2","V3"]:
            parser.error("Combined comparison requires V0,V1,V2,V3 and --split heldout")
        from bench.heldout import compare
        result=compare(args.dataset)
    else:
        result=evaluate(args.dataset,args.forecaster,args.source,secondary=args.secondary,split=args.split)
    print(json.dumps(result,indent=2))
