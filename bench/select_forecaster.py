"""Evaluate the predeclared grid on wave one, then write parameters for owner commit."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, verify, write_csv, write_json
from bench.forecast_eval import evaluate
from bench.replay import replay

RULE = '**Constraints** (a combination failing any is excluded):\n\n1. Creeping, every horizon bin: median \\|error\\| no worse than V0 by more than 10 %\n   **or** 5 s, whichever is larger.\n2. Sawtooth, every horizon bin: median \\|error\\| no worse than V0 by more than 10 %.\n\n**Objective** (among combinations passing both constraints):\n\n1. Minimise flat-high **after-fill** advice cycle fraction (S1).\n2. Tie-break: minimise burst **after-burst** median \\|error\\| over the 0–15 min horizons (S5).\n3. Tie-break: smaller `w_s`, then smaller `τ`.\n\nIf **no** combination passes the constraints: select the one with the smallest constraint\nviolation, and report this explicitly. Do **not** loosen the constraints.'


def rank_candidate(baseline, candidate, short, tolerance):
    violations = []
    for pattern in ("creeping", "sawtooth"):
        for horizon in ("0-5", "5-15", "15-30", "30+"):
            metric = "M1_abs_error_"+horizon
            original = baseline["patterns"][pattern][metric]["median"]
            new = candidate["patterns"][pattern][metric]["median"]
            if original is None:
                continue
            allowance = max(.1*original, 5) if pattern == "creeping" else .1*original
            violations.append(float("inf") if new is None else max(0, new-original-allowance))
    violation = sum(violations)
    secondary = candidate["secondary"]["patterns"]
    flat = secondary["flat_high"]["S1_after_fill_advice_fraction"]["median"]
    burst = secondary["burst"]["S5_after_M1_0-15"]["median"]
    return (violation > 0, violation, float("inf") if flat is None else flat,
            float("inf") if burst is None else burst, short, tolerance)


def select(dataset):
    manifest = verify(dataset)
    output = ROOT/"bench/results"/manifest["run_id"]/"selection"
    params_path = ROOT/"bench/forecaster_params.yml"
    if params_path.exists() or output.exists():
        raise ValueError("Selection output or parameters already exist; review them instead of overwriting")
    output.mkdir(parents=True)
    (output/"rule.md").write_text(RULE+"\nFallback violation is the sum of positive excess error in seconds across constrained bins. "
        "Missing candidate error in a populated baseline bin is infinite violation; empty baseline bins impose no constraint. "
        "Fallback ties use the objective order above.\n", encoding="utf-8")
    baseline = evaluate(dataset, "V0", secondary=True, split="selection", plot=False)
    rows, candidates = [], []
    for short in (5,10,15):
        for tolerance in (.25,.5,1.0):
            params = dict(short_window_minutes=short, agreement_tolerance=tolerance, interval_level=.90)
            replay(dataset, "V2", split="selection", parameters=params)
            summary = evaluate(dataset, "V2", source="replay", secondary=True,
                               split="selection", parameters=params, plot=False)
            rank = rank_candidate(baseline, summary, short, tolerance)
            candidates.append((rank, params))
            name = f"w{short}-t{tolerance}"
            target = output/"candidates"/name
            target.parent.mkdir(exist_ok=True)
            shutil.move(str(output/"V2"), str(target))
            for section in (summary["patterns"], summary["secondary"]["patterns"]):
                for pattern, metrics in section.items():
                    for metric, values in metrics.items():
                        rows.append(dict(candidate=name, short_window_minutes=short,
                            agreement_tolerance=tolerance, pattern=pattern, metric=metric,
                            **values, constraints_pass=not rank[0],
                            repetition_ids=json.dumps(summary["repetition_ids"])))
            write_csv(output/"grid.csv", rows)
            print(name, "constraints pass:", not rank[0], flush=True)
    rank, winner = min(candidates, key=lambda item:item[0])
    report = "# Forecaster selection (wave 1 only)\n\n"+(output/"rule.md").read_text()+"\n\n"
    report += f"Selected short window: {winner['short_window_minutes']} minutes; tolerance: {winner['agreement_tolerance']}.\n"
    report += f"All constraints passed: {not rank[0]}. Constraint violation: {rank[1]} seconds.\n"
    report += f"After-fill advice fraction: {rank[2]}; after-burst 0-15 minute error: {rank[3]} seconds.\n"
    report += "\nRepetition IDs:\n"+"\n".join(baseline["repetition_ids"])+"\n"
    (output/"report.md").write_text(report, encoding="utf-8")
    winner.update(selection_report_sha256=hashlib.sha256((output/"report.md").read_bytes()).hexdigest(),
                  dataset=manifest["run_id"], selection_split="wave_1")
    params_path.write_text(yaml.safe_dump(winner, sort_keys=True), encoding="utf-8")
    print("Selection complete. Owner must review and commit bench/forecaster_params.yml before held-out evaluation.", flush=True)


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    select(parser.parse_args().dataset)
