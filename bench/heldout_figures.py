"""Publication figures and their plotted values from frozen held-out outputs."""
import argparse
from bisect import bisect_right
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.common import ROOT, read_csv, timestamp, verify, write_csv
from bench.replay import unique_forecast_rows


def figures(dataset):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    manifest=verify(dataset)
    output=ROOT/"bench/results"/manifest["run_id"]/"heldout"
    summary=json.loads((output/"summary.json").read_text())
    reps=[r for r in manifest["repetitions"] if r["group_id"] in summary["repetition_ids"]]
    if len(reps)!=40 or any(r["wave"]!=1 for r in reps):
        raise ValueError("Figures require the 40 frozen wave-two repetitions")
    out=output/"figures"
    out.mkdir(exist_ok=True)
    bins=["0-5","5-15","15-30","30+"]
    patterns=["creeping","burst","sawtooth"]
    colors={"V0":"#2678b2","V3":"#d45c20"}
    rows=[]
    fig,axes=plt.subplots(1,3,figsize=(13,4),sharey=False)
    for ax,pattern in zip(axes,patterns):
        for variant in ("V0","V3"):
            values=[]
            for bin_name in bins:
                q=summary["variants"][variant]["patterns"][pattern]["M1_abs_error_"+bin_name]
                rows.append(dict(variant=variant,pattern=pattern,horizon_minutes=bin_name,**q))
                values.append(q)
            convert=lambda key:[v[key] if v[key] is not None else float("nan") for v in values]
            ax.plot(range(4),convert("median"),marker="o",label=variant,color=colors[variant])
            ax.fill_between(range(4),convert("q1"),convert("q3"),alpha=.2,color=colors[variant])
        ax.set(title=pattern,xlabel="Horizon bin (minutes)",ylabel="Absolute error (seconds)")
        ax.set_xticks(range(4),bins); ax.legend(); ax.grid(alpha=.2)
    fig.tight_layout();fig.savefig(out/"error_vs_horizon.png",dpi=300);plt.close(fig)
    write_csv(out/"error_vs_horizon.csv",rows)

    canonical={r["id"] for r in unique_forecast_rows(read_csv(Path(dataset)/"forecast_log.csv"))}
    flat=[r for r in reps if r["pattern"]=="flat_high"]
    rows=[];fills=[];fig,ax=plt.subplots(figsize=(9,4))
    for variant,folder in (("V0","baseline"),("V3","V3")):
        forecasts=json.loads((output/folder/"replay.json").read_text())
        recs=read_csv(output/folder/"replayed_recommendations.csv")
        advised={r["forecast_id"] for r in recs if r["rule"] in {"scale","topology"} and r["priority"] in {"HIGH","MEDIUM"}}
        tracks=[]
        for rep in flat:
            logs=[json.loads(line) for line in (Path(dataset)/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
            meta=next(r for r in logs if r.get("kind")=="metadata")
            start=timestamp(meta["started_at"])
            fills.append(meta["rates"]["fill_seconds"])
            points=sorted((timestamp(f["recorded_at"])-start,str(f["id"]) in advised) for f in forecasts
                          if f["group_id"]==rep["group_id"] and str(f["id"]) in canonical)
            tracks.append(([p[0] for p in points],points,meta["duration_seconds"]))
        series=[]
        for seconds in range(0,3600,5):
            active=0;eligible=0
            for times,points,duration in tracks:
                if seconds>=duration: continue
                eligible+=1
                i=bisect_right(times,seconds)-1
                active+=int(i>=0 and points[i][1])
            fraction=active/eligible if eligible else None
            rows.append(dict(variant=variant,elapsed_seconds=seconds,active_repetitions=active,
                             repetitions=eligible,fraction=fraction))
            series.append(fraction)
        ax.plot([r/60 for r in range(0,3600,5)],series,label=variant,color=colors[variant])
    if len(set(fills))!=1: raise ValueError("Fill durations differ; a single phase band would be misleading")
    ax.axvspan(0,fills[0]/60,color="grey",alpha=.2,label="Fill phase")
    ax.set(xlabel="Elapsed time (minutes)",ylabel="Fraction with active advice",ylim=(-.02,1.02))
    ax.legend();fig.tight_layout();fig.savefig(out/"flat_high_advice.png",dpi=300);plt.close(fig)
    write_csv(out/"flat_high_advice.csv",rows)

    rows=[];fig,axes=plt.subplots(1,3,figsize=(13,4),sharey=True)
    for ax,pattern in zip(axes,patterns):
        values=[]
        for bin_name in bins:
            q=summary["variants"]["V3"]["patterns"][pattern]["M8_coverage_"+bin_name]
            rows.append(dict(variant="V3",pattern=pattern,horizon_minutes=bin_name,nominal=.9,**q))
            values.append(q["median"] if q["median"] is not None else float("nan"))
        ax.plot(range(4),values,marker="o",color=colors["V3"],label="V3 median coverage")
        ax.axhline(.9,color="grey",linestyle="--",label="90% nominal")
        ax.set(title=pattern,xlabel="Horizon bin (minutes)",ylabel="Coverage",ylim=(0,1.02))
        ax.set_xticks(range(4),bins);ax.legend(fontsize=8)
    fig.tight_layout();fig.savefig(out/"interval_coverage.png",dpi=300);plt.close(fig)
    write_csv(out/"interval_coverage.csv",rows)
    print("Figures and plotted CSV values: "+str(out))


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--dataset",type=Path,required=True)
    figures(parser.parse_args().dataset)
