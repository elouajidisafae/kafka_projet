"""Owner-run 3-setup ? 3-size ? 5-repetition Docker stats comparison."""
from pathlib import Path
import sys
import statistics
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.stackcmp_common import *


def schedule():
    rows=[]
    for repetition in range(5):
        for index,size in enumerate(rotated([10,100,500],repetition)):
            rows.append((repetition,size,rotated(list(SETUPS),repetition+index)))
    return rows


def reject_background_work():
    import psutil
    browsers=[]
    for process in psutil.process_iter(["name"]):
        if (process.info["name"] or "").lower() in {"chrome.exe","msedge.exe","firefox.exe","chrome","chromium","firefox"}:
            browsers.append(process.info["name"])
    if browsers: raise ValueError("Close browser/background browser processes before measurement: "+", ".join(sorted(set(browsers))))
    running=command("docker","ps","--format","{{.Names}}").splitlines()
    unexpected=[name for name in running if not name.startswith("khm-stackcmp-")]
    if unexpected: raise ValueError("Stop unrelated containers before resource run: "+", ".join(unexpected))


def setup_metadata(setup,prep):
    records={prep["images"][service]["id"]:prep["images"][service] for service in SETUPS[setup]}
    return dict(containers=len(SETUPS[setup]),logical_image_bytes=sum(r["logical_size_bytes"] for r in records.values()),
                image_ids=list(records),configuration_effort=effort(setup),
                image_size_definition="Sum of unique image IDs' docker image inspect Size; uncompressed logical bytes, not exclusive physical disk bytes. Raw docker system df retained separately.")


def run():
    reject_background_work();prep=prepared();output=phase("resources")
    env=environment(output,output/"bootstrap");per_rep=[]
    metadata={setup:setup_metadata(setup,prep) for setup in SETUPS}
    manifest=dict(commit=commit(),status="running",started_at=utc(),images=prep["images"],
                  warmup_seconds=60,window_seconds=120,repetitions=5,sizes=[10,100,500],
                  workload="paper_scalability: 3 partitions, one topic, 300 messages, N inactive committed groups; static lag 300 per pair",
                  measured_setups=metadata,broker="Shared isolated Kafka excluded from all setup totals",
                  memory="Every sample sums docker stats MemUsage across measured containers, then mean over 120s; median [Q1,Q3] across five repetition means",
                  cpu="Every sample sums docker stats CPU percent across measured containers; time mean per repetition, then median [Q1,Q3] across repetitions",
                  no_browser="Process-name preflight, repeated before each measurement window; does not prove zero unrelated host load")
    write_json(output/"manifest.json",manifest)
    try:
        compose(env,"up","-d","--force-recreate","--wait","kafka",timeout=240)
        for repetition,size,setups in schedule():
            groups,topics=seed_pairs(size,f"stackcmp-scale-{int(time.time())}-{repetition}-{size}")
            for setup in setups:
                directory=output/f"r{repetition}-{size}-{setup}"
                env=environment(output,directory,groups,topics)
                start_monitors(env,SETUPS[setup])
                if setup=="khm":
                    wait_for(lambda:len(get_json("http://127.0.0.1:38080/api/status")["data"])==size,seconds=600)
                    rows=get_json("http://127.0.0.1:38080/api/status")["data"]
                    if any(row["total_lag"]!=300 for row in rows): raise ValueError("KHM seeded lag mismatch")
                else:
                    wait_for(lambda:len(query("stackcmp:lag"))==size,seconds=600)
                    if any(float(row["value"][1])!=300 for row in query("stackcmp:lag")): raise ValueError("Exporter seeded lag mismatch")
                time.sleep(60)
                reject_background_work()
                containers=ids(env,SETUPS[setup]);samples=[];overruns=[]
                started=time.monotonic();deadline=started+120
                while time.monotonic()<deadline:
                    before=time.monotonic();sample=stats(containers)
                    if time.monotonic()<=deadline: samples.append(sample)
                    else: overruns.append(sample)
                    time.sleep(max(0,min(5-(time.monotonic()-before),deadline-time.monotonic())))
                write_json(directory/"docker-stats.json",samples)
                write_json(directory/"overrun-samples-excluded.json",overruns)
                if len(samples)<10: raise ValueError("Insufficient Docker stats samples; do not extend the fixed window")
                row=dict(repetition=repetition,pairs=size,setup=setup,samples=len(samples),
                         memory_bytes=statistics.mean(s["memory_bytes"] for s in samples),
                         cpu_percent=statistics.mean(s["cpu_percent"] for s in samples),**metadata[setup])
                per_rep.append(row);write_json(output/"per-repetition.json",per_rep)
                stop_monitors(env)
                print({k:row[k] for k in ["repetition","pairs","setup","memory_bytes","cpu_percent"]},flush=True)
            cleanup(groups,topics)
        cells=[]
        for setup in SETUPS:
            for size in [10,100,500]:
                rows=[r for r in per_rep if r["setup"]==setup and r["pairs"]==size]
                if len(rows)!=5: raise ValueError("Incomplete repetition schedule")
                cells.append(dict(setup=setup,pairs=size,memory_bytes=quantiles([r["memory_bytes"] for r in rows]),
                                  cpu_percent=quantiles([r["cpu_percent"] for r in rows]),**metadata[setup]))
        old_path=ROOT/"bench/results/scalability/summary.json"
        comparison=[]
        if old_path.exists():
            old=json.loads(old_path.read_text())
            for cell in cells:
                if cell["setup"]!="khm": continue
                reference=next(row for row in old["rows"] if row["pairs"]==cell["pairs"])
                comparison.append(dict(pairs=cell["pairs"],new_docker_stats_median_bytes=cell["memory_bytes"]["median"],
                                       archived_rss_median_bytes=reference["rss_bytes"]["median"],
                                       numerical_difference_bytes=cell["memory_bytes"]["median"]-reference["rss_bytes"]["median"],
                                       warning="Different memory definitions (Docker stats vs PID-1 RSS), builds and source revisions; not a like-for-like memory regression"))
        write_json(output/"summary.json",dict(commit=commit(),results=cells,table6_reference=comparison,aggregation=manifest["memory"]))
        (output/"docker-disk-usage.json").write_text(command("docker","system","df","-v","--format","{{json .}}"),encoding="utf-8")
        manifest.update(status="complete",ended_at=utc());write_json(output/"manifest.json",manifest)
        print("Resource comparison complete:",output/"summary.json")
    except BaseException as exc:
        manifest.update(status="failed",ended_at=utc(),error=repr(exc));write_json(output/"manifest.json",manifest);raise
    finally:
        stop_monitors(env);compose(env,"stop","kafka")


if __name__=="__main__": run()
