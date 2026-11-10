"""Frozen confirmation entry point: check, fresh receipts, record, then evaluate once."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT

TAG = "revision-r1-confirmation-frozen"
PLAN = "bench/confirmation_plan.yml"


def git(*args, root=ROOT):
    return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.DEVNULL)


def canonical_bytes(path):
    return Path(path).read_bytes().replace(b"\r\n", b"\n")


def guard(root=ROOT):
    """No dataset access, subprocess workload, or output creation precedes this gate."""
    root = Path(root)
    try:
        frozen = git("rev-parse", TAG+"^{commit}", root=root).decode().strip()
        head = git("rev-parse", "HEAD", root=root).decode().strip()
        dirty = git("status", "--porcelain", "--untracked-files=normal", root=root).decode().strip()
        raw = git("show", TAG+":"+PLAN, root=root)
    except subprocess.CalledProcessError as exc:
        raise ValueError("Frozen confirmation tag/plan is missing; do not record") from exc
    if head != frozen or dirty:
        raise ValueError("Working tree and HEAD must exactly match the frozen confirmation tag")
    if canonical_bytes(root/PLAN) != raw.replace(b"\r\n", b"\n"):
        raise ValueError("Confirmation plan differs from frozen tag")
    plan = yaml.safe_load(raw)
    if plan["tag"] != TAG or plan["no_tuning_after_recording"] is not True:
        raise ValueError("Invalid confirmation contract")
    for relative, digest in plan["file_sha256"].items():
        if hashlib.sha256(canonical_bytes(root/relative)).hexdigest() != digest:
            raise ValueError("Frozen file hash mismatch: "+relative)
    return plan, dict(tag=TAG, commit=frozen, plan_sha256=hashlib.sha256(raw).hexdigest())


def checks_directory(path):
    path = Path(path).resolve()
    parent = (ROOT/"bench/results").resolve()
    if not path.is_relative_to(parent) or path == parent:
        raise ValueError("Use a fresh subdirectory under bench/results for receipts")
    return path


def check_receipt_stage(path, name, evidence):
    path = checks_directory(path)
    lock = path/"confirmation-checks.json"
    if lock.exists():
        if json.loads(lock.read_text()) != evidence:
            raise ValueError("Receipts belong to another freeze")
    else:
        path.mkdir(parents=True, exist_ok=False)
        lock.write_text(json.dumps(evidence, indent=2)+"\n")
    if (path/(name+"-passed.json")).exists():
        raise ValueError("Refusing to overwrite an existing receipt")
    return path



def validate_receipts(plan,evidence,directory):
    from bench.run_forecast_validation import require_approval_receipts
    directory=Path(directory)
    lock=json.loads((directory/'confirmation-checks.json').read_text())
    if lock != evidence:
        allowed=plan.get('reuse_receipts',{})
        if lock != allowed.get('evidence'):
            raise ValueError('Receipts belong to another freeze/setup')
        for name,digest in allowed.get('files',{}).items():
            if hashlib.sha256((directory/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('Reused receipt changed: '+name)
        if not allowed.get('files'):
            raise ValueError('No pre-registered receipt reuse')
    require_approval_receipts(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["check", "smoke", "calibrate", "record", "evaluate"])
    parser.add_argument("--checks-dir", type=Path)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--workers", type=int, default=8, help="evaluation processes; affects runtime only")
    args = parser.parse_args()
    plan, evidence = guard()
    if args.stage == "check":
        print(json.dumps(evidence, indent=2))
    elif args.stage in {"smoke", "calibrate"}:
        if args.checks_dir is None:
            parser.error("--checks-dir is required")
        name = "smoke" if args.stage == "smoke" else "calibration"
        directory = check_receipt_stage(args.checks_dir, name, evidence)
        if args.stage == "smoke":
            from bench.run_forecast_validation import run
            run(arm="confirmation-smoke", profile="smoke", reps=1, waves=1, duration_min=5, receipt_dir=directory)
        else:
            from bench.calibrate import main as calibrate
            calibrate(receipt_dir=directory)
        guard()
    elif args.stage == "record":
        if args.checks_dir is None:
            parser.error("--checks-dir is required")
        directory = checks_directory(args.checks_dir)
        if json.loads((directory/"confirmation-checks.json").read_text()) != evidence:
            raise ValueError("Use receipts from the current frozen tag")
        from bench.run_forecast_validation import run
        run(arm="confirmation", profile="baseline", reps=10, waves=1, duration_min=60, receipt_dir=directory)
    else:
        if args.dataset is None:
            parser.error("--dataset is required")
        from bench.confirmation_eval import evaluate
        evaluate(args.dataset.resolve(), plan, evidence, workers=args.workers)


if __name__ == "__main__":
    main()
