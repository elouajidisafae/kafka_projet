"""Explicitly exploratory wave-two comparison of the precommitted responsive rule."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, verify, write_json
from bench.splits import frozen_parameters

PARAMETERS = "bench/forecaster_params_responsive.yml"


def responsive_parameters(root=ROOT):
    params, evidence = frozen_parameters(root, relative=PARAMETERS)
    required = dict(analysis_class="exploratory", evaluation_split="wave_2", forecaster="V2",
                    comparison="V0", method="multiwindow", short_window_minutes=5,
                    agreement_tolerance=.5, interval_level=.9, show_range=False)
    if any(params.get(k) != v for k, v in required.items()) or not params.get("rule"):
        raise ValueError("Responsive parameters must record the fixed exploratory 5-minute / 0.5 rule")
    return params, dict(evidence, params_file=PARAMETERS)


def lock_exploratory(dataset, root=ROOT):
    # Commit gate precedes even the dataset read; no scoring before owner commit.
    params, evidence = responsive_parameters(root)
    manifest = verify(dataset)
    if params.get("dataset") != manifest["run_id"]:
        raise ValueError("Responsive parameters belong to another dataset")
    root = Path(root)
    sources = ["bench/responsive.py", "bench/heldout.py", "bench/replay.py", "bench/forecast_eval.py",
               "bench/variants.py", "bench/splits.py", "bench/secondary.py", "bench/common.py",
               "core/forecasting.py", "core/recommender.py"]
    evidence.update(analysis_class="exploratory",
                    dataset_sha256=hashlib.sha256((Path(dataset)/"SHA256SUMS").read_bytes()).hexdigest(),
                    source_sha256={name:hashlib.sha256((root/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest()
                                   for name in sources})
    output = root/"bench/results"/manifest["run_id"]/"heldout-exploratory"
    output.mkdir(parents=True, exist_ok=True)
    lock = output/"freeze.json"
    if lock.exists():
        if json.loads(lock.read_text()) != evidence:
            raise ValueError("Exploratory evaluation is frozen; source or parameters changed")
    else:
        write_json(lock, evidence)
    return manifest, evidence, output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    args = parser.parse_args()
    from bench.heldout import compare
    compare(args.dataset, exploratory=True)
