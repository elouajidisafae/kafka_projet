"""Bounded, validation-only seed/readiness smoke; not a resource measurement."""
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.stackcmp_common import *


def run():
    prepared(); output=phase("smoke");env=environment(output,output/"bootstrap")
    manifest=dict(commit=commit(),analysis_class="validation_only",status="running",started_at=utc())
    write_json(output/"manifest.json",manifest)
    try:
        compose(env,"up","-d","--force-recreate","--wait","kafka",timeout=90)
        groups,topics=seed_pairs(10,"stackcmp-smoke-"+str(int(time.time())))
        env=environment(output,output/"services",groups,topics)
        start_monitors(env,["khm","exporter","prometheus","grafana"])
        khm=wait_for(lambda: get_json("http://127.0.0.1:38080/api/status")["data"] if len(get_json("http://127.0.0.1:38080/api/status")["data"])==10 else None,seconds=60)
        prom=wait_for(lambda:query("stackcmp:lag") if len(query("stackcmp:lag"))==10 else None,seconds=60)
        if any(row["total_lag"]!=300 for row in khm) or any(float(row["value"][1])!=300 for row in prom):
            raise ValueError("Seeded lag does not equal 300 per pair")
        sample=stats(ids(env,["khm","exporter","prometheus","grafana"]))
        write_json(output/"validation.json",dict(pairs=10,lag_per_pair=300,khm_rows=len(khm),prometheus_rows=len(prom),
                   docker_stats_containers=len(sample["containers"]),grafana=get_json("http://127.0.0.1:33000/api/health"),
                   analysis_class="validation_only",not_resource_measurement=True))
        manifest.update(status="complete",ended_at=utc());write_json(output/"manifest.json",manifest)
        print("Validation smoke complete:",output)
    except BaseException as exc:
        manifest.update(status="failed",ended_at=utc(),error=repr(exc));write_json(output/"manifest.json",manifest);raise
    finally:
        stop_monitors(env);compose(env,"stop","kafka")


if __name__=="__main__": run()
