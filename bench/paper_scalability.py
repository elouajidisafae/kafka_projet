"""Five interleaved real-broker repetitions at 10/100/500 group-topic pairs."""
import argparse
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.paper_common import *


def released_config(groups, topics):
    """Use released settings, adapting only benchmark routing and pair selection."""
    cfg=yaml.safe_load((ROOT/"config.yml").read_text(encoding="utf-8"))
    cfg["clusters"]=[dict(cfg["clusters"][0],name="bench",bootstrap_servers="kafka:29092")]
    cfg["include_groups"]=groups
    cfg["exclude_groups"]=[]
    from confluent_kafka.admin import AdminClient
    all_topics=AdminClient({"bootstrap.servers":"localhost:19092"}).list_topics(timeout=20).topics
    cfg["exclude_topics"]=[t for t in all_topics if t not in topics]
    return cfg


def run(commit, smoke=False, released_default=False, output_prefix=None, results_root=None):
    if released_default:
        results_root=Path(results_root or ROOT/"bench/results").resolve()
        pointer=results_root/"final-confirmation-latest.json"
        provenance=json.loads(pointer.read_text(encoding="utf-8"))
        summary=results_root.parent.parent/provenance['summary_path']
        if hashlib.sha256(summary.read_bytes()).hexdigest()!=provenance['sha256']:
            raise ValueError("Final coverage summary hash mismatch")
    output,manifest=begin(str(output_prefix) if output_prefix else "scalability",commit,smoke)
    rows=[]
    try:
        quiet_docker(["khm-bench-kafka-1","khm-bench-khm-1","khm-paper-monitor"])
        image=broker_up(output)
        manifest.update(images={"khm":image_details(image),"kafka":image_details(IMAGES["kafka"])},
                        warmup_seconds=30 if smoke else 60,window_seconds=15 if smoke else 120,
                        repetitions=1 if smoke else 5,
                        configuration="released config.yml" if released_default else "original Table 6 baseline config.bench.yml",
                        workload="One three-partition topic; N committed inactive groups; static lag 300 per pair",
                        memory="VmRSS of KHM PID 1; bytes", sampling="Completed collection cycles only; no duplicate gauge observations")
        if released_default:
            manifest.update(released_config_sha256=hashlib.sha256((ROOT/'config.yml').read_bytes()).hexdigest(),
                            coverage_provenance=provenance,
                            range_factors_sha256=hashlib.sha256((ROOT/'core/range_calibration_v2.yml').read_bytes()).hexdigest())
        write_json(output/"manifest.json",manifest)
        sizes=[10] if smoke else [10,100,500]
        for rep in range(manifest["repetitions"]):
            for size in rotated(sizes,rep):
                directory=output/f"r{rep}-{size}"
                groups,topics=seed_pairs(size,f"paper-scale-{int(time.time())}-{rep}-{size}")
                cfg=released_config(groups,topics) if released_default else benchmark_config(groups,topics)
                start_khm(image,directory,cfg,results_root=results_root if released_default else None)
                wait_for(lambda:len(api("/api/status")["data"])==size,seconds=600)
                time.sleep(manifest["warmup_seconds"])
                text=http("http://127.0.0.1:18080/metrics")["body"]
                seen=metric(text,"khm_collection_cycles_total")
                initial_overruns=metric(text,"khm_collection_overruns_total")
                samples=[];memory=[];start=time.monotonic();deadline=start+manifest["window_seconds"]
                while time.monotonic()<deadline:
                    response=http("http://127.0.0.1:18080/metrics")
                    if response["status"]!=200: raise RuntimeError("Metrics request failed")
                    text=response["body"];cycle=metric(text,"khm_collection_cycles_total")
                    memory.append(dict(timestamp=utc(),rss_bytes=rss("khm-paper-monitor")))
                    if cycle!=seen:
                        if cycle!=seen+1:
                            raise RuntimeError('Missed completed collection cycles; phase gauges cannot recover those observations')
                        count=metric(text,"khm_monitored_pairs")
                        if count!=size: raise RuntimeError(f"Requested {size} pairs, observed {count}")
                        samples.append(dict(timestamp=response["timestamp"],cycle=cycle,pairs=count,
                            collection_seconds=metric(text,"khm_collection_duration_seconds"),
                            forecast_seconds=metric(text,"khm_forecast_duration_seconds")))
                        seen=cycle
                    time.sleep(min(1,max(0,deadline-time.monotonic())))
                final=http("http://127.0.0.1:18080/metrics")["body"]
                write_json(directory/"cycles.json",samples);write_json(directory/"rss.json",memory)
                if len(samples)<2: raise RuntimeError("Fewer than two completed cycles in fixed window; retained as failed, do not extend window")
                row=dict(repetition=rep,pairs=size,cycles=len(samples),
                         collection_seconds=statistics.median(s["collection_seconds"] for s in samples),
                         forecast_seconds=statistics.median(s["forecast_seconds"] for s in samples),
                         rss_bytes=statistics.mean(s["rss_bytes"] for s in memory),
                         overruns=metric(final,"khm_collection_overruns_total")-initial_overruns)
                rows.append(row);write_csv(output/"per_rep.csv",rows)
                command("docker","stop","khm-paper-monitor")
                cleanup_pairs(groups,topics)
                print(row,flush=True)
        write_json(output/"summary.json",dict(smoke=smoke,commit=commit,
            aggregation="median [Q1,Q3] across repetition summaries; phase medians and time-sampled mean RSS per repetition",
            rows=summarise(rows,["pairs"],["collection_seconds","forecast_seconds","rss_bytes","overruns"])))
        finish(output,manifest)
    except BaseException as exc:
        finish(output,manifest,exc)
        raise


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit",required=True)
    parser.add_argument("--smoke",action="store_true")
    parser.add_argument("--released-default",action="store_true",help="Load released config.yml; adapt only broker routing and pair filters")
    parser.add_argument("--output-prefix",type=Path,help="Fresh output directory; smoke appends -smoke")
    parser.add_argument("--results-root",type=Path,help="Read-only results mount for released calibrated coverage provenance")
    args=parser.parse_args();run(args.commit,args.smoke,args.released_default,args.output_prefix,args.results_root)
