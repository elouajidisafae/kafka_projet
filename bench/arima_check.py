"""Post-heldout exploratory ARIMA robustness check; never a confirmatory result."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, write_json
from bench.model_comparison import context, FitRunner, run_model, metrics, new_phase, source_hashes, DEFAULT_WORKERS
from bench.model_comparison_alignment import archive_inputs, saved_forecasts, aligned_model

PARAMS=ROOT/"bench/model_comparison_params_arima_check.yml"
DISCLOSURE="The <=5% first-half fit-failure rule was added after the original held-out results were seen."


def check_sources():
    return dict(source_hashes(),**{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
               for name in ("bench/arima_check.py","bench/model_comparison_alignment.py","bench/secondary.py")})


def choose_from_summary(summary):
    if summary.get("split")!="wave_1": raise ValueError("Exploratory order choice must use first-half selection results only")
    eligible=[r for r in summary["candidates"] if r["model"]=="ARIMA" and r["forecasts"]>0
              and r["fit_failures"]/r["forecasts"]<=.05 and r["pooled_crossing_error"]["median"] is not None]
    if not eligible: raise ValueError("No order passes the first-half failure limit")
    return min(eligible,key=lambda r:r["pooled_crossing_error"]["median"])


def choose(dataset):
    if PARAMS.exists(): raise ValueError("Exploratory parameter file exists; refusing to replace it")
    # This function reads no second-half forecasts/results.
    path=ROOT/"bench/results"/dataset.name/"model-comparison/selection/summary.json"
    raw=path.read_bytes(); summary=json.loads(raw)
    if summary["dataset"]!=dataset.name: raise ValueError("Selection dataset mismatch")
    if summary["source_hashes"]!=source_hashes(): raise ValueError("Original selection sources changed")
    winner=choose_from_summary(summary)
    params=dict(analysis_class="exploratory",rule_added_after_heldout=DISCLOSURE,
                dataset=dataset.name,selection_split="wave_1",evaluation_split="wave_2",
                max_first_half_failure_rate=.05,failure_denominator="all sampled first-half forecast origins, including insufficient histories",
                first_half_origins=winner["forecasts"],first_half_fit_failures=winner["fit_failures"],
                first_half_failure_rate=winner["fit_failures"]/winner["forecasts"],
                first_half_crossing_error_median_seconds=winner["pooled_crossing_error"]["median"],
                criterion=summary["criterion"],selection_sampling=summary["selection_sampling"],
                heldout_sampling=summary["heldout_sampling"],ARIMA=winner["settings"],
                selection_report_sha256=hashlib.sha256(raw).hexdigest(),source_hashes=check_sources())
    PARAMS.write_text(yaml.safe_dump(params,sort_keys=True),encoding="utf-8")
    print(yaml.safe_dump(params,sort_keys=True))
    print("STOP: commit bench/model_comparison_params_arima_check.yml before evaluation.")


def frozen_check(dataset):
    relative="bench/model_comparison_params_arima_check.yml"
    try:
        raw=subprocess.check_output(["git","show","HEAD:"+relative],cwd=ROOT,stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as exc:
        raise ValueError("Commit exploratory settings before evaluation") from exc
    if raw.replace(b"\r\n",b"\n")!=PARAMS.read_bytes().replace(b"\r\n",b"\n"):
        raise ValueError("Exploratory parameters differ from committed settings")
    params=yaml.safe_load(raw)
    if (params.get("analysis_class")!="exploratory" or params.get("rule_added_after_heldout")!=DISCLOSURE
        or params.get("dataset")!=dataset.name or params.get("selection_split")!="wave_1"
        or params.get("evaluation_split")!="wave_2"):
        raise ValueError("Exploratory disclosure/dataset/split missing")
    path=ROOT/"bench/results"/dataset.name/"model-comparison/selection/summary.json"
    selection=path.read_bytes()
    winner=choose_from_summary(json.loads(selection))
    if (hashlib.sha256(selection).hexdigest()!=params["selection_report_sha256"]
        or params["ARIMA"]!=winner["settings"] or params["source_hashes"]!=check_sources()):
        raise ValueError("Exploratory rule, source or selection provenance changed")
    return params,hashlib.sha256(raw).hexdigest()


def evaluate(dataset,workers=DEFAULT_WORKERS):
    params,digest=frozen_check(dataset)  # Must pass before creating outputs or fitting.
    output=new_phase(dataset,"arima-check-exploratory")
    manifest,reps,observations=context(dataset,"heldout")
    with FitRunner(observations,manifest["config"],workers) as runner:
        rows=run_model(observations,manifest["config"],"ARIMA",params["ARIMA"],output/"ARIMA.jsonl",runner=runner)
    _,inputs,canonical=archive_inputs(dataset)
    aligned=aligned_model(inputs,saved_forecasts(output/"ARIMA.jsonl",canonical),manifest["config"]["alerts"])
    result=dict(analysis_class="exploratory",rule_added_after_heldout=DISCLOSURE,ARIMA=params["ARIMA"],
                dataset=dataset.name,workers=workers,parameter_sha256=digest,
                repetition_ids=[r["group_id"] for r in reps],metrics=metrics(rows),table5_aligned=aligned)
    write_json(output/"summary.json",result)
    print("Exploratory ARIMA check complete:",output/"summary.json")


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase",choices=["choose","evaluate"])
    parser.add_argument("--dataset",type=Path,required=True)
    parser.add_argument("--workers",type=int,default=DEFAULT_WORKERS)
    args=parser.parse_args()
    if args.phase=="choose": choose(args.dataset.resolve())
    else: evaluate(args.dataset.resolve(),args.workers)
