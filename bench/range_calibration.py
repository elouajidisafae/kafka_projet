"""Calibrate saved forecasts, then score the disjoint second wave. No point refitting."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import yaml
from bench.common import timestamp
from bench.forecast_eval import horizon_bin
ROOT = Path(__file__).resolve().parents[1]
RUN = "baseline-20261001T111043363413Z"
BASE = ROOT / "bench/results" / RUN
FACTORS = ROOT / "bench/range_calibration.yml"
BINS = ("0-5", "5-15", "15-30", "30+")

def sources(split):
    return {"baseline": BASE / split / "baseline/pairs.csv",
            "multiwindow": BASE / ("selection/candidates/w5-t0.5/pairs.csv" if split == "selection" else "heldout-exploratory/V2/pairs.csv")}

def pairs(path):
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
    for r in rows:
        r["remaining"] = float(r["truth"])-float(r["forecast_time"])
        r["predicted"] = r["remaining"]+float(r["error_seconds"])
    return [r for r in rows if r["remaining"] > 0 and r["predicted"] > 0]

def calibrate():
    if FACTORS.exists(): raise ValueError("Factors already exist")
    result = dict(dataset=RUN, split="wave 0 / selection; repetitions r00-r09 only",
        evaluation_split="wave 1 / heldout; repetitions r10-r19 only",
        horizon="predicted seconds: (0,300], (300,900], (900,1800], (1800,infinity)",
        quantiles="numpy percentile linear, forecast-weighted; pooled burst/creeping/sawtooth",
        responsive=dict(short_window_minutes=5, agreement_tolerance=.5), modes={}, provenance={})
    for mode,path in sources("selection").items():
        rows = pairs(path)
        result["provenance"][mode] = dict(path=path.relative_to(ROOT).as_posix(), sha256=hashlib.sha256(path.read_bytes()).hexdigest(), repetitions=sorted({r["rep"] for r in rows}))
        result["modes"][mode] = {}
        for b in BINS:
            ratios = [r["remaining"]/r["predicted"] for r in rows if horizon_bin(r["predicted"]) == b]
            q = np.percentile(ratios,[5,95]) if ratios else [None,None]
            result["modes"][mode][b] = dict(n=len(ratios), q05=float(q[0]) if ratios else None, q95=float(q[1]) if ratios else None)
    FACTORS.write_text(yaml.safe_dump(result, sort_keys=False))

def evaluate():
    committed = subprocess.check_output(["git","show","HEAD:bench/range_calibration.yml"],cwd=ROOT)
    if committed.replace(b"\r\n",b"\n") != FACTORS.read_bytes().replace(b"\r\n",b"\n"): raise ValueError("Commit factors before evaluation")
    factors = yaml.safe_load(committed)
    output = BASE / "range-calibration"
    output.mkdir(exist_ok=False)
    # Exact saved input IDs are used only to compute the legacy formula bounds.
    from core.multiwindow import prediction_interval
    from bench.replay import history_row, unique_forecast_rows
    data = ROOT / "bench/data" / RUN
    with (data/"lag_history.csv").open() as h: history={r["id"]:history_row(r) for r in csv.DictReader(h)}
    with (data/"forecast_log.csv").open() as h: logged={str(r["id"]):r for r in unique_forecast_rows(list(csv.DictReader(h)))}
    saved=json.loads((BASE/"heldout-exploratory/V2/replay.json").read_text())
    responsive={str(r["id"]):r for r in saved}
    formula={str(r["id"]):r for r in json.loads((BASE/"heldout/V1/replay.json").read_text())}
    results=[]; pooled={}; stable={}; provenance={}
    for mode,path in sources("heldout").items():
        rows=pairs(path)
        provenance[mode]=dict(path=path.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),repetitions=sorted({r["rep"] for r in rows}))
        for r in rows:
            b=horizon_bin(r["predicted"]); f=factors["modes"][mode][b]
            r["cal_low"]=f["q05"]*r["predicted"] if f["n"] else None
            r["cal_high"]=f["q95"]*r["predicted"] if f["n"] else None
            if mode=="baseline":
                old=formula[r["forecast_id"]]; lo,hi=old["eta_low_sec"],old["eta_high_sec"]
            else:
                old=responsive[r["forecast_id"]]
                records=[history[str(i)] for i in json.loads(logged[r["forecast_id"]]["input_ids_json"])]
                if old["window_used"]=="short":
                    end=timestamp(records[-1]["recorded_at"])
                    records=[v for v in records if timestamp(v["recorded_at"])>=end-300]
                lo,hi=prediction_interval(records,10000)
            r["formula_low"],r["formula_high"]=lo,hi
        for pattern in ("burst","creeping","sawtooth","pooled"):
            for b in (*BINS,"all"):
                subset=[r for r in rows if (pattern=="pooled" or r["pattern"]==pattern) and (b=="all" or horizon_bin(r["predicted"])==b)]
                for kind in ("cal","formula"):
                    covered=sum(r[kind+"_low"] is not None and r[kind+"_low"]<=r["remaining"] and (r[kind+"_high"] is None or r["remaining"]<=r[kind+"_high"]) for r in subset)
                    widths=[r[kind+"_high"]-r[kind+"_low"] for r in subset if r[kind+"_low"] is not None and r[kind+"_high"] is not None]
                    results.append(dict(mode=mode,workload=pattern,bin=b,range=kind,n=len(subset),covered=covered,coverage=covered/len(subset) if subset else None,median_width_sec=float(np.median(widths)) if widths else None,finite_width_n=len(widths),open_upper_n=sum(r[kind+"_low"] is not None and r[kind+"_high"] is None for r in subset)))
                    if pattern=="pooled" and b=="all" and kind=="cal": pooled[mode]=covered/len(subset)
        # All canonical stable-backlog forecasts, including negative/no-crossing sentinels.
        stable_rows=[r for r in (saved if mode=="multiwindow" else list(formula.values())) if "flat_high" in r["group_id"]]
        count=0
        for r in stable_rows:
            eta=r["eta_critical_sec"]
            f=factors["modes"][mode][horizon_bin(eta)] if eta>0 else None
            count+=bool(f and f["n"] and f["q05"]*eta<=1800)
        stable[mode]=dict(n=len(stable_rows),starts_within_30min=count,share=count/len(stable_rows))
    result=dict(factors_sha256=hashlib.sha256(committed).hexdigest(),provenance=provenance,pooled_coverage=pooled,stable_backlog=stable,rows=results,formula_policy="Missing lower uncovered; null upper open-ended; finite widths only. Responsive formula recomputed on saved selected inputs, point estimates never refit.")
    (output/"evaluation.json").write_text(json.dumps(result,indent=2))
    with (output/"evaluation.csv").open("w",newline="") as h:
        writer=csv.DictWriter(h,fieldnames=results[0]); writer.writeheader();writer.writerows(results)

if __name__ == "__main__":
    parser=argparse.ArgumentParser();parser.add_argument("part",choices=["calibrate","evaluate"])
    globals()[parser.parse_args().part]()
