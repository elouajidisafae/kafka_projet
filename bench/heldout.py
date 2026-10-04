"""Frozen, paired wave-two comparison; repeated execution verifies identical output."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path

from bench.common import ROOT, read_csv, timestamp, verify, write_csv, write_json
from bench.splits import frozen_parameters, select_repetitions


def lock_evaluation(dataset, root=ROOT):
    root = Path(root)
    manifest = verify(dataset)
    params, provenance = frozen_parameters(root)
    if params.get("dataset") != manifest["run_id"]:
        raise ValueError("Frozen parameters belong to a different dataset")
    report = root/"bench/results"/manifest["run_id"]/"selection/report.md"
    if hashlib.sha256(report.read_bytes()).hexdigest() != params["selection_report_sha256"]:
        raise ValueError("Selection report checksum differs from frozen parameters")
    output = root/"bench/results"/manifest["run_id"]/"heldout"
    output.mkdir(parents=True, exist_ok=True)
    sources = ["bench/heldout.py", "bench/variants.py", "bench/replay.py", "bench/forecast_eval.py",
               "bench/secondary.py", "bench/splits.py", "core/forecasting.py", "core/recommender.py"]
    evidence = dict(**provenance, selection_report_sha256=params["selection_report_sha256"],
        dataset_sha256=hashlib.sha256((Path(dataset)/"SHA256SUMS").read_bytes()).hexdigest(),
        source_sha256={name:hashlib.sha256((root/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest()
                       for name in sources})
    lock = output/"freeze.json"
    if lock.exists():
        if json.loads(lock.read_text()) != evidence:
            raise ValueError("Held-out evaluation is frozen; parameters, dataset or analysis source changed")
    else:
        with lock.open("x", encoding="utf-8") as handle:
            json.dump(evidence, handle, sort_keys=True, indent=2)
            handle.write("\n")
    return manifest, evidence, output


def interval_metrics(forecasts, truth):
    from bench.forecast_eval import horizon_bin, quantiles
    coverage, widths = defaultdict(list), defaultdict(list)
    opened, missing = defaultdict(int), defaultdict(int)
    for f in forecasts:
        time = timestamp(f["recorded_at"])
        if truth is None or time >= truth or "eta_low_sec" not in f:
            continue
        horizon = truth-time
        label = horizon_bin(horizon)
        low, high = f.get("eta_low_sec"), f.get("eta_high_sec")
        for key in ("all", label):
            if low is None:
                coverage[key].append(0)
                missing[key] += 1
                continue
            coverage[key].append(int(low <= horizon and (high is None or horizon <= high)))
            if high is None:
                opened[key] += 1
            else:
                widths[key].append((high-low)/horizon)
    result = {}
    for key in ("all", "0-5", "5-15", "15-30", "30+"):
        suffix = "" if key == "all" else "_"+key
        result["M8_coverage"+suffix] = sum(coverage[key])/len(coverage[key]) if coverage[key] else None
        result["M9_relative_width"+suffix] = quantiles(widths[key])["median"]
        result["M9_open_ended_count"+suffix] = opened[key]
        result["M8_missing_interval_count"+suffix] = missing[key]
    return result


def paired_metrics(rows):
    from bench.forecast_eval import quantiles
    baseline = {(r["rep"],r["metric"]):r["value"] for r in rows if r["variant"]=="V0"}
    grouped = defaultdict(list)
    for r in rows:
        if r["variant"] == "V0":
            continue
        base = baseline.get((r["rep"],r["metric"]))
        value = r["value"]
        diff = value-base if value is not None and base is not None else None
        name = r["metric"]
        # Signed errors improve towards zero; lead time and coverage improve upwards.
        direction = "closer_to_zero" if name.startswith(("M2_","S3_")) or "_M2_" in name else (
            "higher" if name.startswith(("M4_","M8_coverage")) or name in {
                "S2_critical_minus_warning","S2_critical_minus_advice","S2_warning_minus_advice"} else "lower")
        improved = None if diff is None else (abs(value)<abs(base) if direction=="closer_to_zero"
                    else diff>0 if direction=="higher" else diff<0)
        r.update(baseline_value=base, difference=diff, improved=improved)
        grouped[(r["variant"],r["pattern"],name,direction)].append(r)
    result = {}
    for (variant,pattern,metric,direction), values in sorted(grouped.items()):
        result.setdefault(variant,{}).setdefault(pattern,{})[metric] = dict(
            difference=quantiles([r["difference"] for r in values]),
            improved=sum(r["improved"] is True for r in values),
            comparable=sum(r["improved"] is not None for r in values),
            total=len(values), direction=direction)
    return result


def compare(dataset):
    from bench.replay import replay, unique_forecast_rows
    from bench.forecast_eval import evaluate, quantiles
    manifest, provenance, output = lock_evaluation(dataset)
    reps, _ = select_repetitions(manifest,"heldout",True)
    canonical = {r["id"] for r in unique_forecast_rows(read_csv(Path(dataset)/"forecast_log.csv"))}
    summaries, rows = {}, []
    for variant in ("V0","V1","V2","V3"):
        name = "baseline" if variant=="V0" else variant
        directory = output/name
        # A completed replay can be reused only under the unchanged freeze lock.
        if not (directory/"replay-complete.json").exists():
            print("Replaying "+variant+" on wave 2", flush=True)
            replay(dataset,variant,split="heldout")
            write_json(directory/"replay-complete.json", dict(provenance=provenance,
                sha256={name:hashlib.sha256((directory/name).read_bytes()).hexdigest()
                        for name in ("replay.json","replayed_recommendations.csv","fidelity.json")}))
        receipt=json.loads((directory/"replay-complete.json").read_text())
        if receipt["provenance"] != provenance or any(
            hashlib.sha256((directory/name).read_bytes()).hexdigest()!=digest
            for name,digest in receipt["sha256"].items()):
            raise ValueError("Completed replay differs from frozen evaluation receipt")
        summary = evaluate(dataset,variant,source="replay",secondary=True,split="heldout",plot=False)
        summaries[variant] = summary
        for filename in ("per_rep.csv","secondary_per_rep.csv"):
            for r in read_csv(directory/filename):
                rows.append(dict(variant=variant,rep=r["rep"],pattern=r["pattern"],metric=r["metric"],
                    value=float(r["value"]) if r["value"] else None,
                    analysis_class="secondary" if filename.startswith("secondary") else "primary"))
        forecasts = json.loads((directory/"replay.json").read_text())
        checks = {r["rep"]:r for r in summary["repetitions"]}
        intervals = defaultdict(lambda:defaultdict(list))
        for rep in reps:
            logs = [json.loads(line) for line in (Path(dataset)/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
            samples = [r for r in logs if "produced" in r]
            start,end = timestamp(samples[0]["timestamp"]),timestamp(samples[-1]["timestamp"])
            selected = [f for f in forecasts if str(f["id"]) in canonical and f["group_id"]==rep["group_id"]
                        and start<=timestamp(f["recorded_at"])<=end]
            metrics = interval_metrics(selected,checks[rep["group_id"]]["truth"])
            for metric,value in metrics.items():
                if variant in {"V0","V2"}:
                    value = None
                rows.append(dict(variant=variant,rep=rep["group_id"],pattern=rep["pattern"],metric=metric,
                                 value=value,analysis_class="primary"))
                intervals[rep["pattern"]][metric].append(value)
        for pattern,metrics in intervals.items():
            summary["patterns"][pattern].update({k:quantiles(v) for k,v in metrics.items()})
    result = dict(split="heldout",run_id=manifest["run_id"],**provenance,
        repetition_ids=sorted(r["group_id"] for r in reps), variants=summaries,
        paired=paired_metrics(rows),
        secondary=dict(analysis_class="secondary",variants={v:s["secondary"] for v,s in summaries.items()}),
        interval_policy="All pre-breach forecasts; missing lower bound counts as uncovered; null upper bound is open-ended. Width excludes open-ended intervals.",
        paired_policy="Variant minus V0 per repetition. Unavailable comparisons remain null (not zero). Narrower width alone is not evidence of better calibration.")
    encoded = json.dumps(result,sort_keys=True,indent=2,allow_nan=False)+"\n"
    final = output/"summary.json"
    if final.exists() and final.read_text(encoding="utf-8") != encoded:
        raise ValueError("Held-out summary changed on repeat execution; original retained")
    final.write_text(encoded,encoding="utf-8")
    write_csv(output/"per_rep.csv",rows,["variant","rep","pattern","metric","value","analysis_class","baseline_value","difference","improved"])
    print("Held-out comparison complete: "+str(final),flush=True)
    return result
