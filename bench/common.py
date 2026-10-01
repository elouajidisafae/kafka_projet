"""Dataset integrity and deterministic serialization helpers."""
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def utc():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    return datetime.fromisoformat(value).timestamp()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fields=None):
    rows = list(rows)
    fields = fields or (list(rows[0]) if rows else [])
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def seal(dataset):
    dataset = Path(dataset)
    if (dataset / "SHA256SUMS").exists():
        raise ValueError("Dataset already sealed; refusing to change it")
    lines = [f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(dataset).as_posix()}" for p in sorted(dataset.rglob("*")) if p.is_file()]
    (dataset / "SHA256SUMS").write_text("\n".join(lines)+"\n", encoding="utf-8")


def verify(dataset):
    dataset = Path(dataset).resolve()
    listed = set()
    for line in (dataset / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        path = (dataset/name).resolve()
        if not path.is_relative_to(dataset) or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
            raise ValueError(f"Dataset checksum mismatch: {name}")
        listed.add(name)
    actual = {p.relative_to(dataset).as_posix() for p in dataset.rglob("*") if p.is_file() and p.name != "SHA256SUMS"}
    if listed != actual:
        raise ValueError("Dataset has unlisted or missing files")
    return json.loads((dataset/"manifest.json").read_text())
