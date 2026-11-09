"""Select within wave zero, freeze, then evaluate once on wave one. No refitting."""
import argparse
import csv
import hashlib
import itertools
import json
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np
import yaml
from bench.range_calibration import BASE, ROOT, RUN, pairs
from core.range_groups import HORIZONS, HISTORIES, QUALITIES, factor_for, group_key, horizon

OUTPUT = BASE / "range-calibration-v2"
FACTORS = ROOT / "bench/range_calibration_v2.yml"
PATTERNS = ("burst", "creeping", "sawtooth")
CANDIDATES = ("C0", "C1", "C2", "C3")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def repetition_number(group):
    return int(re.search(r"-r(\d+)-", group)[1])


def paths(split):
    return {
        "baseline": BASE / split / "baseline/pairs.csv",
        "multiwindow": BASE / ("selection/candidates/w5-t0.5/pairs.csv" if split == "selection"
                                 else "heldout-exploratory/V2/pairs.csv"),
    }


def metadata(split):
    """Filter baseline raw records before use; never open heldout results in selection."""
    from bench.replay import unique_forecast_rows
    data = ROOT / "bench/data" / RUN / "forecast_log.csv"
    wave = range(10) if split == "selection" else range(10, 20)
    with data.open() as handle:
        baseline = unique_forecast_rows([r for r in csv.DictReader(handle)
                                        if repetition_number(r["group_id"]) in wave])
    saved_path = BASE / ("selection/candidates/w5-t0.5/replay.json" if split == "selection"
                         else "heldout-exploratory/V2/replay.json")
    canonical = {str(r["id"]) for r in baseline}
    responsive = [r for r in json.loads(saved_path.read_text()) if str(r["id"]) in canonical]
    result = {"baseline": {str(r["id"]): r for r in baseline},
              "multiwindow": {str(r["id"]): r for r in responsive}}
    provenance = dict(baseline_metadata=str(data.relative_to(ROOT)), baseline_metadata_sha256=digest(data),
                      responsive_metadata=str(saved_path.relative_to(ROOT)), responsive_metadata_sha256=digest(saved_path))
    return result, provenance


def load_rows(split):
    meta, provenance = metadata(split)
    result = {}
    provenance["pairs"] = {}
    for mode, path in paths(split).items():
        rows = pairs(path)
        for row in rows:
            saved = meta[mode][row["forecast_id"]]
            # Use the saved integer point, not a float reconstructed from an error.
            row["predicted"] = int(saved["eta_critical_sec"])
            row["quality"] = saved["confidence"]
            row["window_seconds"] = int(saved["window_seconds"])
            assert row["quality"] == row["confidence"]
            assert (repetition_number(row["rep"]) < 10) == (split == "selection")
        result[mode] = rows
        provenance["pairs"][mode] = dict(path=str(path.relative_to(ROOT)), sha256=digest(path),
                                       repetitions=sorted({r["rep"] for r in rows}))
    return result, meta, provenance


def percentiles(rows):
    ratios = [r["remaining"] / r["predicted"] for r in rows]
    low, high = np.percentile(ratios, [5, 95], method="linear") if ratios else (None, None)
    return dict(n=len(rows), q05=float(low) if ratios else None, q95=float(high) if ratios else None)


def fit_factors(rows, candidate):
    rows = [r for r in rows if horizon(r["predicted"]) is not None]
    horizons = {h: percentiles([r for r in rows if horizon(r["predicted"]) == h]) for h in HORIZONS}
    qualities = QUALITIES if candidate in {"C1", "C3"} else (None,)
    histories = HISTORIES if candidate in {"C2", "C3"} else (None,)
    groups = {}
    for h, q, history in itertools.product(HORIZONS, qualities, histories):
        key = "|".join(str(v) for v in (h, q, history) if v is not None)
        group = [r for r in rows if group_key(candidate, r["predicted"], r["quality"], r["window_seconds"]) == key]
        groups[key] = percentiles(group)
        groups[key]["fallback"] = candidate != "C0" and len(group) < 200
    return dict(candidate=candidate, horizon_factors=horizons, groups=groups)


def bounds(row, model):
    f = factor_for(model, row["predicted"], row["quality"], row["window_seconds"])
    return (f["q05"] * row["predicted"], f["q95"] * row["predicted"]) if f else (None, None)


def score(rows, interval):
    widths, covered = [], 0
    for row in rows:
        low, high = interval(row)
        if low is not None and high is not None:
            widths.append(high-low)
            covered += low <= row["remaining"] <= high
    return dict(n=len(rows), range_n=len(widths), covered=covered,
                coverage=covered/len(rows) if rows else None,
                shown_coverage=covered/len(widths) if widths else None,
                median_width_sec=float(np.median(widths)) if widths else None)


def write_json(path, value):
    if path.exists():
        raise ValueError(f"Refusing to overwrite {path}")
    path.write_text(json.dumps(value, indent=2)+"\n", encoding="utf-8")


def select():
    rows, _, provenance = load_rows("selection")
    result = dict(dataset=RUN, inner_train="r00-r04", inner_check="r05-r09",
                  domain="positive predictions <=1800 seconds issued before observed CRITICAL crossing",
                  rule="smallest pooled median width among coverage >=.88 pooled and >=.80 each workload; otherwise C0; exact ties use C0,C1,C2,C3 order",
                  provenance=provenance, modes={})
    for mode, values in rows.items():
        train = [r for r in values if repetition_number(r["rep"]) < 5]
        check = [r for r in values if repetition_number(r["rep"]) >= 5 and horizon(r["predicted"]) is not None]
        candidates = {}
        for candidate in CANDIDATES:
            model = fit_factors(train, candidate)
            pooled = score(check, lambda r: bounds(r, model))
            workloads = {p: score([r for r in check if r["pattern"] == p], lambda r: bounds(r, model)) for p in PATTERNS}
            qualifies = pooled["coverage"] is not None and pooled["coverage"] >= .88 and all(v["coverage"] is not None and v["coverage"] >= .80 for v in workloads.values())
            candidates[candidate] = dict(pooled=pooled, workloads=workloads, qualifies=qualifies, inner_factors=model)
        eligible = [c for c in CANDIDATES if candidates[c]["qualifies"]]
        chosen = min(eligible, key=lambda c: candidates[c]["pooled"]["median_width_sec"]) if eligible else "C0"
        result["modes"][mode] = dict(chosen=chosen, fallback_no_qualifier=not bool(eligible), candidates=candidates)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    write_json(OUTPUT/"inner-selection.json", result)
    for mode, v in result["modes"].items():
        print(mode, "chosen:", v["chosen"], "no qualifier:", v["fallback_no_qualifier"])
        for c, metrics in v["candidates"].items():
            print(c, metrics["pooled"], {p: m["coverage"] for p,m in metrics["workloads"].items()}, metrics["qualifies"])


def committed(relative):
    blob = subprocess.check_output(["git", "show", "HEAD:"+relative], cwd=ROOT)
    if blob.replace(b"\r\n", b"\n") != (ROOT/relative).read_bytes().replace(b"\r\n", b"\n"):
        raise ValueError(f"Commit {relative} before proceeding")
    return blob


def freeze():
    selection_relative = (OUTPUT/"inner-selection.json").relative_to(ROOT).as_posix()
    selection = json.loads(committed(selection_relative))
    if FACTORS.exists():
        raise ValueError("Frozen v2 factors already exist")
    rows, _, provenance = load_rows("selection")
    if provenance != selection["provenance"]:
        raise ValueError("Selection sources changed")
    result = dict(version=2, dataset=RUN, training_split="wave 0, r00-r09",
                  test_split="wave 1, r10-r19; untouched until after freeze commit",
                  horizon_bins="(0,300], (300,900], (900,1800] predicted seconds; no range beyond 1800",
                  history_bins="selected fit window_seconds: [0,300), [300,1800), [1800,infinity)",
                  fit_quality="archived confidence field; HIGH/MEDIUM/LOW",
                  quantiles="forecast-weighted numpy percentile linear: 5th and 95th of observed/predicted",
                  minimum_group_n=200, sparse_group_rule="fall back to horizon-only factors; never pool test data",
                  responsive=dict(short_window_minutes=5, agreement_tolerance=.5),
                  selection=dict(path=selection_relative, sha256=digest(OUTPUT/"inner-selection.json")),
                  inner_results={m: {"chosen":v["chosen"], "fallback_no_qualifier":v["fallback_no_qualifier"],
                                     "candidates":{c:{k:x for k,x in metrics.items() if k != "inner_factors"}
                                                   for c,metrics in v["candidates"].items()}}
                                 for m,v in selection["modes"].items()},
                  provenance=provenance,
                  modes={m:fit_factors(values,selection["modes"][m]["chosen"]) for m,values in rows.items()})
    FACTORS.write_text(yaml.safe_dump(result, sort_keys=False), encoding="utf-8")


def evaluate():
    # Gate precedes every read of second-half results.
    blob = committed("bench/range_calibration_v2.yml")
    factors = yaml.safe_load(blob)
    if (OUTPUT/"evaluation.json").exists():
        raise ValueError("Evaluation already exists")
    old = yaml.safe_load((ROOT/"bench/range_calibration.yml").read_text())
    rows, meta, provenance = load_rows("heldout")
    table, pooled, stable = [], {}, {}
    for mode, values in rows.items():
        def interval(row, version):
            if version == "v2":
                return bounds(row, factors["modes"][mode])
            h = horizon(row["predicted"]) or "30+"
            f = old["modes"][mode][h]
            return f["q05"]*row["predicted"], f["q95"]*row["predicted"]
        for workload in (*PATTERNS, "pooled"):
            for h in (*HORIZONS, "30+", "0-30", "all"):
                subset = [r for r in values if (workload == "pooled" or r["pattern"] == workload)
                          and (h == "all" or h == "0-30" and horizon(r["predicted"]) is not None
                               or h == (horizon(r["predicted"]) or "30+"))]
                for version in ("v1", "v2"):
                    metrics = score(subset, lambda r: interval(r, version))
                    table.append(dict(mode=mode, workload=workload, bin=h, version=version, **metrics))
                    if workload == "pooled" and h == "0-30" and version == "v2":
                        pooled[mode] = metrics["shown_coverage"]
        stable[mode] = {}
        backlog = [r for r in meta[mode].values() if "flat_high" in r["group_id"]]
        for h in ("0-30", "30+", "nonpositive", "all"):
            subset = [r for r in backlog if h == "all" or
                      h == "0-30" and 0 < int(r["eta_critical_sec"]) <= 1800 or
                      h == "30+" and int(r["eta_critical_sec"]) > 1800 or
                      h == "nonpositive" and int(r["eta_critical_sec"]) <= 0]
            for version in ("v1", "v2"):
                shown = 0
                for row in subset:
                    eta = int(row["eta_critical_sec"])
                    if eta <= 0:
                        continue
                    if version == "v1":
                        shown += bool(old["modes"][mode][horizon(eta) or "30+"]["n"])
                    else:
                        shown += factor_for(factors["modes"][mode], eta, row["confidence"], int(row["window_seconds"])) is not None
                stable[mode][h+"/"+version] = dict(n=len(subset), range_n=shown, share=shown/len(subset) if subset else None)
    result = dict(factors_sha256=hashlib.sha256(blob).hexdigest(), v1_factors_sha256=digest(ROOT/"bench/range_calibration.yml"),
                  provenance=provenance, pooled_coverage=pooled,
                  coverage_policy="coverage uses all eligible origins in the row denominator, absent range is uncovered; shown_coverage uses range_n only; tooltip pools displayed 0-30 min ranges",
                  stable_policy="all canonical flat_high forecasts; no-crossing sentinels count as no range",
                  rows=table, stable_backlog=stable)
    write_json(OUTPUT/"evaluation.json", result)
    with (OUTPUT/"evaluation.csv").open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=table[0]); writer.writeheader(); writer.writerows(table)
    print("v2 displayed pooled coverage:", pooled)
    print("stable backlog:", stable)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("part", choices=["select", "freeze", "evaluate"])
    globals()[parser.parse_args().part]()
