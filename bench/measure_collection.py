"""Operator timing at 10, 20 and 40 real consumer groups on one broker."""
import json
from pathlib import Path
import re
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, write_json, read_csv, timestamp
from bench.run_forecast_validation import run
from bench.forecast_eval import quantiles


def main():
    report=[]
    for groups in (10,20,40):
        dataset,_=run(arm=f"timing-{groups}",reps=groups,waves=1,duration_min=2,only_patterns=["creeping"])
        samples=[json.loads(line) for line in (dataset/"collection-timing.jsonl").read_text().splitlines()]
        values=[]
        for sample in samples:
            match=re.search(r'khm_collection_duration_seconds\{cluster="bench"\} ([0-9.eE+-]+)',sample["metrics"])
            count=re.search(r'khm_monitored_pairs\{cluster="bench"\} (\d+)',sample["metrics"])
            if match and count and int(count[1])==groups:
                values.append(float(match[1]))
        if len(values)<2:
            raise RuntimeError(f"Insufficient complete timing samples for {groups} groups")
        history=read_csv(dataset/"lag_history.csv")
        by_group={}
        for row in history:
            by_group.setdefault(row["group_id"],[]).append(timestamp(row["recorded_at"]))
        intervals=[b-a for times in by_group.values() for a,b in zip(sorted(times),sorted(times)[1:])]
        report.append(dict(effective_sampling_seconds=quantiles(intervals),groups=groups,dataset=str(dataset.relative_to(ROOT)),first_observed_seconds=values[0],warm_collection_seconds=quantiles(values[1:]),maximum_seconds=max(values),within_five_seconds=max(values[1:])<=5))
        print(report[-1])
    write_json(ROOT/"bench/results/collection-timing.json",report)


if __name__=="__main__":
    main()
