"""Enforced selection/held-out boundary and committed parameter provenance."""
import hashlib
import subprocess
from pathlib import Path
import yaml
from bench.common import ROOT


def frozen_parameters(root=ROOT):
    root = Path(root)
    relative = "bench/forecaster_params.yml"
    path = root/relative
    if not path.is_file():
        raise ValueError("Held-out evaluation requires committed forecaster_params.yml")
    try:
        committed = subprocess.check_output(["git","show","HEAD:"+relative],cwd=root,stderr=subprocess.DEVNULL)
        commit = subprocess.check_output(["git","log","-1","--format=%H","--",relative],cwd=root,text=True).strip()
    except subprocess.CalledProcessError as exc:
        raise ValueError("Forecaster parameters must be committed before held-out evaluation") from exc
    # Git may check out LF blobs as CRLF on Windows. Hash the committed blob,
    # while allowing only that mechanical checkout conversion in the worktree.
    if committed.replace(b"\r\n", b"\n") != path.read_bytes().replace(b"\r\n", b"\n"):
        raise ValueError("Forecaster parameters differ from their committed version")
    params = yaml.safe_load(committed)
    if params.get("interval_level") != .90:
        raise ValueError("Interval level must remain 0.90")
    return params, dict(params_sha256=hashlib.sha256(committed).hexdigest(),params_commit=commit)


def select_repetitions(manifest, split, candidate=False, root=ROOT):
    if split == "smoke":
        if manifest.get("profile") != "smoke" or manifest.get("config", {}).get("forecast", {}).get("method") != "multiwindow":
            raise ValueError("Smoke replay requires a multiwindow smoke recording")
        return manifest["repetitions"], {}
    if candidate and split not in {"selection", "heldout"}:
        raise ValueError("Candidate evaluation requires an explicit selection or heldout split")
    provenance = {}
    if split == "heldout":
        _, provenance = frozen_parameters(root)
    wave = {"selection":0,"heldout":1}.get(split)
    rows = [r for r in manifest["repetitions"] if wave is None or r["wave"]==wave]
    if not rows:
        raise ValueError("Requested split contains no repetitions")
    return rows, provenance
