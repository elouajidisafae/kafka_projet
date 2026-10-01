"""Replay immutable recordings without a broker or a live database."""
import argparse
from bisect import bisect_right
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
import importlib
import json
from pathlib import Path
import sys
from typing import Protocol
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, read_csv, timestamp, verify, write_csv, write_json


class Forecaster(Protocol):
    name: str
    def fit(self, records: list[dict], now: datetime, thresholds: dict) -> dict: ...


@contextmanager
def configured(module, config):
    previous = module.CONFIG
    module.CONFIG = config
    try:
        yield
    finally:
        module.CONFIG = previous


class Baseline:
    name = "baseline"
    def __init__(self, config):
        self.config = config

    def fit(self, records, now, thresholds):
        from core import forecasting
        # The current fitter anchors ETA on the last observation, never wall clock.
        pair = records[-1] if records else dict(cluster_name="", group_id="", topic="")
        with configured(forecasting, dict(self.config, alerts=thresholds)):
            return forecasting.fit_forecast(records, pair["cluster_name"], pair["group_id"], pair["topic"])


def load_forecaster(name, config):
    if name == "baseline":
        return Baseline(config)
    # Explicit module:factory plug-in, returning the Forecaster protocol.
    module, factory = name.split(":",1)
    return getattr(importlib.import_module(module),factory)(config)


def history_row(row):
    numeric = {"id", "total_lag", "partition_count", "partitions_counted", "consumer_count", "log_end_offset", "committed_offset"}
    return {k: (int(v) if v != "" else None) if k in numeric else v for k,v in row.items()}


def forecast_identity(row):
    """Exact ordered input provenance within a pair; never merge distinct fits."""
    ids = json.loads(row.get("input_ids_json") or "null")
    if not isinstance(ids, list) or not ids:
        raise ValueError("Recording lacks exact input provenance; cannot certify replay fidelity")
    return (row["cluster_name"], row["group_id"], row["topic"], tuple(ids))


def unique_forecast_rows(rows):
    """Use the earliest persisted forecast for each exact input observation."""
    seen = set()
    result = []
    for row in sorted(rows, key=lambda r: (r["recorded_at"], int(r["id"]))):
        identity = forecast_identity(row)
        if identity not in seen:
            result.append(row)
            seen.add(identity)
    return result


def recommendation_observations(logged, recs):
    """Associate cycle recommendations with exact input sets, including API copies."""
    identities = {f["id"]: forecast_identity(f) for f in logged}
    pair_forecasts = defaultdict(list)
    for f in logged:
        pair_forecasts[identities[f["id"]][:3]].append(f)
    timelines = {}
    for key, values in pair_forecasts.items():
        values.sort(key=lambda f: (f["recorded_at"], int(f["id"])))
        timelines[key] = ([f["recorded_at"] for f in values], values)
    observed = defaultdict(set)
    for r in recs:
        if r["rule"] not in {"scale", "topology"}:
            continue
        key = (r["cluster_name"], r["group_id"], r["topic"])
        times, values = timelines.get(key, ([], []))
        index = bisect_right(times, r["recorded_at"]) - 1
        if index < 0:
            raise ValueError("Forecast-driven recommendation has no preceding forecast")
        observed[identities[values[index]["id"]]].add((r["rule"], r["priority"]))
    return identities, observed


def replay(dataset, forecaster="baseline"):
    dataset=Path(dataset)
    manifest=verify(dataset)
    config=manifest["config"]
    model=load_forecaster(forecaster,config)
    history=[history_row(r) for r in read_csv(dataset/"lag_history.csv")]
    by_id={r["id"]:r for r in history}
    grouped=defaultdict(list)
    for row in history:
        grouped[(row["cluster_name"],row["group_id"],row["topic"])].append(row)
    logged=read_csv(dataset/"forecast_log.csv")
    recs=read_csv(dataset/"recommendation_log.csv")
    identities, observed = recommendation_observations(logged, recs)
    checked_recommendations = {}
    from core import recommender
    outputs=[]
    matches=[]
    mismatches=[]
    rec_mismatches=[]
    for logged_f in logged:
        key=(logged_f["cluster_name"],logged_f["group_id"],logged_f["topic"])
        if logged_f.get("input_ids_json"):
            ids=json.loads(logged_f["input_ids_json"])
            records=[by_id[i] for i in ids]
            if any((r["cluster_name"],r["group_id"],r["topic"])!=key for r in records):
                raise ValueError("Forecast input belongs to another pair")
        else:
            raise ValueError("Recording lacks exact input provenance; cannot certify replay fidelity")
        now=datetime.fromisoformat(logged_f["recorded_at"])
        f=model.fit(records,now,config["alerts"])
        parsed = {k:(int(logged_f[k]) if k=="eta_critical_sec" else float(logged_f[k])) for k in ("slope","intercept","r_squared","eta_critical_sec")}
        bad={k:dict(logged=v,replayed=f.get(k)) for k,v in parsed.items() if v!=f.get(k)}
        if bad:
            mismatches.append(dict(forecast_id=logged_f["id"], fields=bad, cause="unexplained"))
        row=records[-1]
        with configured(recommender,config):
            rules=recommender.analyze_row(*key,row["total_lag"],row["group_state"],f.get("trend"),f,
                consumer_count=row.get("consumer_count"),partition_count=row.get("partition_count"), apply_cap=False)
        relevant=[r for r in rules if r["id"].rsplit("-",1)[-1] in {"scale","topology"}]
        expected={(r["id"].rsplit("-",1)[-1],r["priority"]) for r in relevant}
        identity = identities[logged_f["id"]]
        # Every numerical forecast is still checked. Recommendation evidence belongs
        # to the input observation, not to each cached API persistence event.
        if identity in checked_recommendations and checked_recommendations[identity] != expected:
            raise ValueError("Identical forecast inputs produced inconsistent recommendation matches")
        if identity not in checked_recommendations:
            checked_recommendations[identity] = expected
            if expected != observed[identity]:
                rec_mismatches.append(dict(forecast_id=logged_f["id"], expected=sorted(expected),
                    logged=sorted(observed[identity]), cause="unexplained"))
        outputs.append(dict(id=logged_f["id"],recorded_at=logged_f["recorded_at"],**f))
        for r in relevant:
            matches.append(dict(forecast_id=logged_f["id"],recorded_at=logged_f["recorded_at"],cluster_name=key[0],group_id=key[1],topic=key[2],rule=r["id"].rsplit("-",1)[-1],priority=r["priority"],displayed="",metrics_json=json.dumps(r["metrics"],sort_keys=True)))
    count=len(logged)
    if not count:
        raise ValueError("No forecasts to replay")
    result=dict(rows=count, exact_forecast_fraction=1-len(mismatches)/count,
        exact_recommendation_fraction=1-len(rec_mismatches)/len(checked_recommendations),
        recommendation_observations=len(checked_recommendations),
        repeated_input_rows=count-len(checked_recommendations),
        recommendation_comparison="unique pair and ordered input IDs; all forecast rows checked numerically",
        forecast_mismatches=mismatches,recommendation_mismatches=rec_mismatches,
        baseline_fidelity_required=forecaster=="baseline")
    result["passed"] = not mismatches and not rec_mismatches
    out=ROOT/"bench/results"/manifest["run_id"]/model.name
    out.mkdir(parents=True,exist_ok=True)
    write_json(out/"fidelity.json",result)
    write_json(out/"replay.json",outputs)
    write_csv(out/"replayed_recommendations.csv",matches,["forecast_id","recorded_at","cluster_name","group_id","topic","rule","priority","displayed","metrics_json"])
    return result


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--dataset",type=Path,required=True)
    parser.add_argument("--forecaster",default="baseline")
    args=parser.parse_args()
    result=replay(args.dataset,args.forecaster)
    print(json.dumps(result,indent=2))
    if args.forecaster=="baseline" and not result["passed"]:
        raise SystemExit(1)
