"""Export a run atomically from a consistent SQLite snapshot, then seal it."""
from contextlib import closing
import argparse
import json
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, seal, write_csv, write_json


def export(run_id, database, dataset=None):
    dataset = Path(dataset or ROOT/"bench/data"/run_id)
    if (dataset/"SHA256SUMS").exists():
        raise ValueError("Refusing to overwrite sealed recording")
    manifest = json.loads((dataset/"manifest.json").read_text())
    counts = {}
    with closing(sqlite3.connect(f"file:{Path(database).resolve().as_posix()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        for table in ("lag_history", "forecast_log", "recommendation_log"):
            cursor = conn.execute(f"SELECT * FROM {table} WHERE run_id=? ORDER BY id",(run_id,))
            rows = [{k:(v.removeprefix("integer:") if isinstance(v,str) and v.startswith("integer:") and k in {"eta_warning_sec","eta_critical_sec"} else v) for k,v in dict(r).items()} for r in cursor]
            write_csv(dataset/f"{table}.csv",rows,[x[0] for x in cursor.description])
            counts[table] = len(rows)
    if not counts["lag_history"] or not counts["forecast_log"]:
        raise ValueError("Recording contains no history or forecasts")
    manifest["row_counts"] = counts
    write_json(dataset/"manifest.json",manifest)
    seal(dataset)
    return dataset


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--run-id",required=True)
    parser.add_argument("--database",type=Path,required=True,help="Consistent SQLite backup, never a copied live WAL database")
    args=parser.parse_args()
    print(export(args.run_id,args.database))
