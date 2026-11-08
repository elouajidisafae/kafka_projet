"""Read-only positive crossing coverage over the common held-out origins."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT,timestamp
from bench.forecast_eval import breach,quantiles
from bench.model_comparison_alignment import archive_inputs,saved_forecasts


def coverage(rows,truth):
    eligible=[r for r in rows if truth is not None and timestamp(r["recorded_at"])<truth]
    positive=sum(r.get("eta_critical_sec") is not None and float(r["eta_critical_sec"])>0 for r in eligible)
    return dict(eligible=len(eligible),positive=positive,fraction=positive/len(eligible) if eligible else None)


def run(dataset):
    manifest,inputs,canonical=archive_inputs(dataset)
    folder=ROOT/"bench/results"/dataset.name
    paths={name:(folder/"model-comparison/heldout"/(name+".jsonl"),True) for name in ("OLS","Holt","ARIMA")}
    paths["V2 responsive"]=(folder/"heldout-exploratory/V2/replay.json",False)
    paths["ARIMA exploratory"]=(folder/"model-comparison/arima-check-exploratory/ARIMA.jsonl",True)
    result={}
    for name,(path,comparison) in paths.items():
        rows=saved_forecasts(path,canonical,comparison)
        grouped=defaultdict(list)
        for row in rows: grouped[row["group_id"]].append(row)
        per_rep=[]
        for group,entry in inputs.items():
            truth=breach([(timestamp(r["recorded_at"]),r["total_lag"]) for r in entry["history"]],manifest["config"]["alerts"]["critical_threshold"])
            per_rep.append(dict(rep=group,pattern=entry["rep"]["pattern"],observed_crossing=truth,**coverage(grouped[group],truth)))
        summaries=[]
        for pattern in sorted({r["pattern"] for r in per_rep}):
            values=[r for r in per_rep if r["pattern"]==pattern]
            n=sum(r["eligible"] for r in values);positive=sum(r["positive"] for r in values)
            summaries.append(dict(pattern=pattern,eligible=n,positive=positive,pooled_fraction=positive/n if n else None,
                                  per_repetition_fraction=quantiles([r["fraction"] for r in values])))
        result[name]=dict(origins=len(rows),patterns=summaries,per_rep=per_rep)
    return dict(dataset=dataset.name,definition="positive ETA among ALL canonical origins issued before observed crossing; missing forecasts remain in denominator; no observed crossing means not applicable",models=result)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--dataset",type=Path,required=True)
    print(json.dumps(run(parser.parse_args().dataset.resolve()),indent=2,allow_nan=False))
