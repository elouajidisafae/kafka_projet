"""Operator calibration: allow 45 minutes to observe the latest 40-minute target."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.run_forecast_validation import run
from bench.common import ROOT
from bench.common import read_csv, timestamp, write_json


def main():
    dataset,summary=run(arm="calibration",reps=1,waves=1,duration_min=45)
    import json
    manifest=json.loads((dataset/"manifest.json").read_text())
    rows=[]
    for rep,check in zip(manifest["repetitions"],summary["repetitions"]):
        logs=[json.loads(line) for line in (dataset/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
        start=next(timestamp(r["timestamp"]) for r in logs if "produced" in r)
        minutes=(check["truth"]-start)/60 if check["truth"] is not None else None
        bounds=(rep["rates"].get("target_min"),rep["rates"].get("target_max"))
        passed=(minutes is None if rep["pattern"]=="flat_high" else minutes is not None and bounds[0]<=minutes<=bounds[1])
        row=dict(pattern=rep["pattern"],crossing_minutes=minutes,target_minutes=bounds,passed=passed and check["rate_pass"] and check["commit_pass"])
        rows.append(row)
        print(row)
    write_json(dataset.parents[1]/"results"/manifest["run_id"]/"calibration.json",rows)
    if not all(r["passed"] for r in rows) or summary["truth_match_fraction"] is None or summary["truth_match_fraction"]<.95:
        raise SystemExit(1)
    write_json(ROOT/"bench/results/calibration-passed.json",dict(passed=True,run_id=manifest["run_id"],fingerprint=manifest["configuration_fingerprint"]))


if __name__=="__main__":
    main()
