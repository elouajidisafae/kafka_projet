"""Operator entry point for isolated recording, export and offline checks."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import yaml
from bench.common import ROOT, utc, write_json, timestamp

COMPOSE=["docker","compose","-f",str(ROOT/"docker-compose.bench.yml")]


def command(args,env=None,**kwargs):
    return subprocess.run(args,cwd=ROOT,env=env,check=True,**kwargs)


def capture(args):
    return subprocess.check_output(args,cwd=ROOT,text=True).strip()


def fingerprint(profile):
    config_path=ROOT/("config.bench.smoke.yml" if profile=="smoke" else "config.bench.yml")
    settings=dict(config=yaml.safe_load(config_path.read_text()),patterns=yaml.safe_load((ROOT/"bench/patterns.yml").read_text())[profile],
                  workload_sha256=hashlib.sha256((ROOT/"bench/workload.py").read_bytes()).hexdigest())
    return hashlib.sha256(json.dumps(settings,sort_keys=True).encode()).hexdigest()


def require_approval_receipts():
    for name,profile in (("smoke","smoke"),("calibration","baseline")):
        path=ROOT/"bench/results"/(name+"-passed.json")
        if not path.exists():
            raise ValueError(f"Run and pass {name} before the baseline recording")
        receipt=json.loads(path.read_text())
        if not receipt.get("passed") or receipt.get("fingerprint")!=fingerprint(profile):
            raise ValueError(f"{name} result does not cover the current configuration")


def run(arm="baseline",profile="baseline",reps=10,waves=2,duration_min=60,only_patterns=None,method=None):
    import psutil
    if reps<1 or waves<1 or duration_min<=0:
        raise ValueError("Positive repetition counts and duration required")
    if method is not None and (method != "multiwindow" or profile != "smoke" or arm == "baseline"):
        raise ValueError("Method override is supported only for multiwindow smoke")
    if arm=="baseline" and (profile!="baseline" or duration_min!=60 or reps!=10 or waves!=2):
        raise ValueError("Baseline recording requires 10 reps, 2 waves and 60 minutes per wave")
    dirty=capture(["git","status","--porcelain"])
    if arm=="baseline" and dirty:
        raise ValueError("Commit and review the recording source before baseline capture")
    if arm=="baseline":
        require_approval_receipts()
    run_id=f"{'smoke' if profile=='smoke' else arm}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    dataset=ROOT/"bench/data"/run_id
    dataset.mkdir(parents=True)
    config_path=ROOT/("config.bench.smoke.yml" if profile=="smoke" else "config.bench.yml")
    config=yaml.safe_load(config_path.read_text())
    if method:
        from bench.splits import frozen_parameters
        params, provenance = frozen_parameters()
        config["forecast"].update({k:params[k] for k in ("short_window_minutes", "agreement_tolerance", "interval_level")})
        config["forecast"]["method"] = method
    patterns=yaml.safe_load((ROOT/"bench/patterns.yml").read_text())[profile]
    if only_patterns:
        patterns={k:v for k,v in patterns.items() if k in only_patterns}
    repetitions=[]
    suffix=hashlib.sha256(run_id.encode()).hexdigest()[:8]
    for wave in range(waves):
        for pattern in patterns:
            for rep in range(reps):
                number=wave*reps+rep
                name=f"bench-{pattern}-r{number:02}-{suffix}"
                seed=int.from_bytes(hashlib.sha256(f"{run_id}/{name}".encode()).digest()[:4],"big")
                repetitions.append(dict(pattern=pattern,group_id=name,topic=name,wave=wave,seed=seed,rates=patterns[pattern]))
    # Immutable run-specific allowlist: ignore earlier benchmark groups without touching dev.
    config["include_groups"]=[r["group_id"] for r in repetitions]
    snapshot=yaml.safe_dump(config,sort_keys=True)
    (dataset/"config.bench.yml").write_text(snapshot)
    (dataset/"patterns.yml").write_bytes((ROOT/"bench/patterns.yml").read_bytes())
    manifest=dict(configuration_fingerprint=fingerprint(profile),run_id=run_id,arm=arm,profile=profile,status="running",started_at=utc(),ended_at=None,
        commit=capture(["git","rev-parse","HEAD"]),dirty=bool(dirty),working_tree_status=dirty,
        config=config,config_sha256=hashlib.sha256((dataset/"config.bench.yml").read_bytes()).hexdigest(),
        patterns=patterns,patterns_sha256=hashlib.sha256((dataset/"patterns.yml").read_bytes()).hexdigest(),
        host=dict(os=platform.platform(),cpu=platform.processor(),cores=os.cpu_count(),ram_bytes=psutil.virtual_memory().total,python=sys.version,docker=capture(["docker","version","--format","{{.Server.Version}}"]),kafka_image="confluentinc/cp-kafka:7.6.0"),
        repetitions=repetitions,requested_duration_seconds=duration_min*60)
    write_json(dataset/"manifest.json",manifest)
    if method:
        manifest["forecaster_provenance"] = provenance
        write_json(dataset/"manifest.json",manifest)
    env=dict(os.environ,KHM_RUN_ID=run_id,KHM_BENCH_CONFIG=str(dataset/"config.bench.yml"))
    processes=[]
    handles=[]
    stop_file=ROOT/"bench/results"/run_id/"stop"
    stop_file.parent.mkdir(parents=True)
    print(f"Recording {run_id}",flush=True)
    def interrupted(signum,frame):
        raise KeyboardInterrupt
    previous=signal.signal(signal.SIGINT,interrupted)
    try:
        command(COMPOSE+["up","-d","--build","--wait"],env=env)
        manifest["host"]["kafka_image_details"]=json.loads(capture(["docker","image","inspect","confluentinc/cp-kafka:7.6.0","--format","{{json .}}"]));
        manifest["host"]["numpy"]=__import__("numpy").__version__
        deadline=time.monotonic()+90
        while True:
            try:
                with urllib.request.urlopen("http://localhost:18080/api/status",timeout=3) as response:
                    json.load(response)
                break
            except Exception:
                if time.monotonic()>deadline:
                    raise TimeoutError("Monitor health deadline exceeded")
                time.sleep(1)
        from confluent_kafka.admin import AdminClient, NewTopic
        admin=AdminClient({"bootstrap.servers":"localhost:19092"})
        for wave in range(waves):
            current=[r for r in repetitions if r["wave"]==wave]
            futures=admin.create_topics([NewTopic(r["topic"],num_partitions=3,replication_factor=1) for r in current])
            for f in futures.values():
                f.result(30)
            processes=[]
            for rep in current:
                spec=dict(rep,bootstrap="localhost:19092",duration_seconds=duration_min*60,log=str(dataset/"generator"/(rep["group_id"]+".jsonl")))
                spec_path=stop_file.parent/(rep["group_id"]+".json")
                write_json(spec_path,spec)
                handle=(stop_file.parent/(rep["group_id"]+".log")).open("w")
                handles.append(handle)
                processes.append(subprocess.Popen([sys.executable,str(ROOT/"bench/workload.py"),"--spec",str(spec_path),"--stop-file",str(stop_file)],cwd=ROOT,stdout=handle,stderr=subprocess.STDOUT))
            deadline=time.monotonic()+duration_min*60+120
            next_metrics = 0.0
            while any(p.poll() is None for p in processes):
                if stop_file.exists():
                    raise KeyboardInterrupt
                if any(p.poll() not in (None,0) for p in processes):
                    raise RuntimeError(f"Workload failed; inspect {stop_file.parent}")
                if time.monotonic()>deadline:
                    raise TimeoutError("Workload exceeded its duration and startup allowance")
                if time.monotonic() >= next_metrics:
                    with urllib.request.urlopen("http://localhost:18080/metrics",timeout=5) as response:
                        metrics=response.read().decode()
                    with (dataset/"collection-timing.jsonl").open("a",encoding="utf-8") as handle:
                        handle.write(json.dumps(dict(timestamp=utc(),wave=wave,groups=len(current),metrics=metrics))+"\n")
                    next_metrics=time.monotonic()+5
                time.sleep(1)
            if any(p.returncode for p in processes):
                raise RuntimeError("Workload subprocess failure")
            print(f"Wave {wave+1}/{waves} finished",flush=True)
            # Allow one final collector cycle before deleting only this wave's resources.
            time.sleep(6)
            for f in admin.delete_consumer_groups([r["group_id"] for r in current]).values():
                f.result(30)
            for f in admin.delete_topics([r["topic"] for r in current]).values():
                f.result(30)
        manifest["status"]="complete"
    except KeyboardInterrupt:
        manifest["status"]="aborted"
    except Exception as exc:
        manifest["status"]="failed"
        manifest["error"]=repr(exc)
    finally:
        stop_file.touch()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)
        for handle in handles:
            handle.close()
        signal.signal(signal.SIGINT,previous)
        manifest["ended_at"]=utc()
        write_json(dataset/"manifest.json",manifest)
    if manifest["status"]!="complete":
        raise RuntimeError(f"{manifest['status']}: {manifest.get('error','operator interrupt')}; partial data at {dataset}")
    # SQLite backup API includes WAL transactions consistently. Never copy just the live DB.
    with tempfile.TemporaryDirectory() as temp:
        backup=Path(temp)/"recording.db"
        code="import sqlite3,os; s=sqlite3.connect(os.environ['KHM_DB_PATH']); d=sqlite3.connect('/tmp/khm-export.db'); s.backup(d); d.close(); s.close()"
        command(COMPOSE+["exec","-T","khm","python","-c",code],env=env)
        command(COMPOSE+["cp","khm:/tmp/khm-export.db",str(backup)],env=env)
        from bench.export_run import export
        export(run_id,backup,dataset)
    return validate_recording(dataset)


def replay_in_container(dataset, manifest):
    """Check raw interval floats in the same numerical environment as recording."""
    run_id = manifest["run_id"]
    env = dict(os.environ, KHM_RUN_ID=run_id, KHM_BENCH_CONFIG=str(dataset/"config.bench.yml"))
    target = "/tmp/khm-smoke-replay-"+hashlib.sha256(run_id.encode()).hexdigest()[:16]
    command(COMPOSE+["exec", "-T", "khm", "mkdir", "-p", target], env=env)
    command(COMPOSE+["cp", str(dataset)+"/.", "khm:"+target], env=env)
    command(COMPOSE+["exec", "-T", "khm", "python", "bench/replay.py", "--dataset", target,
                    "--forecaster", "V3", "--split", "smoke"], env=env)
    output = ROOT/"bench/results"/run_id/"smoke/V3"
    output.mkdir(parents=True, exist_ok=True)
    command(COMPOSE+["cp", f"khm:/app/bench/results/{run_id}/smoke/V3/.", str(output)], env=env)
    versions = subprocess.check_output(COMPOSE+["exec", "-T", "khm", "python", "-c",
        "import sys,numpy,scipy,json; print(json.dumps(dict(python=sys.version,numpy=numpy.__version__,scipy=scipy.__version__)))"],
        cwd=ROOT, env=env, text=True)
    write_json(output/"replay-environment.json", json.loads(versions))
    return json.loads((output/"fidelity.json").read_text())


def validate_recording(dataset):
    """Resume checks on a sealed recording without repeating the workload."""
    from bench.common import verify
    dataset = Path(dataset).resolve()
    manifest = verify(dataset)
    run_id, profile = manifest["run_id"], manifest["profile"]
    repetitions = manifest["repetitions"]
    method = manifest["config"]["forecast"].get("method") == "multiwindow"
    if method and profile != "smoke":
        raise ValueError("Container V3 validation is restricted to smoke recordings")
    from bench.replay import replay
    from bench.forecast_eval import evaluate
    model = "V3" if method else "baseline"
    split = "smoke" if method else None
    fidelity=replay_in_container(dataset, manifest) if method else replay(dataset, model, split=split)
    summary=evaluate(dataset, model, source="replay" if method else "live", split=split)
    result_path = f"smoke/{model}" if method else model
    print(f"Dataset: {dataset}\nFidelity: {fidelity['passed']}\nEvaluation: bench/results/{run_id}/{result_path}/summary.json",flush=True)
    if not fidelity["passed"]:
        raise RuntimeError("Replay fidelity failed; inspect mismatch lists")
    if profile=="smoke":
        checks=[]
        for rep,result in zip(repetitions,summary["repetitions"]):
            logs=[json.loads(line) for line in (dataset/"generator"/(rep["group_id"]+".jsonl")).read_text().splitlines()]
            start=next(timestamp(row["timestamp"]) for row in logs if "produced" in row)
            crossing=(result["truth"]-start)/60 if result["truth"] is not None else None
            target=(crossing is None if rep["pattern"]=="flat_high" else crossing is not None and rep["rates"]["target_min"]<=crossing<=rep["rates"]["target_max"])
            checks.append(dict(pattern=rep["pattern"],crossing_minutes=crossing,passed=target and result["rate_pass"] and result["commit_pass"] and result["starts_at_zero"]))
        passed=all(r["passed"] for r in checks) and summary["truth_match_fraction"]>=.95
        write_json(ROOT/"bench/results"/run_id/"smoke-checks.json",checks)
        if not passed:
            raise RuntimeError("Smoke pattern/rate/commit/truth checks failed")
        receipt = "multiwindow-smoke-passed.json" if method else "smoke-passed.json"
        write_json(ROOT/"bench/results"/receipt,dict(passed=True,run_id=run_id,fingerprint=manifest["configuration_fingerprint"], config_sha256=manifest["config_sha256"]))
    return dataset,summary


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--arm",default="baseline")
    parser.add_argument("--profile",choices=["baseline","smoke"],default="baseline")
    parser.add_argument("--reps",type=int,default=10)
    parser.add_argument("--waves",type=int,default=2)
    parser.add_argument("--duration-min",type=float,default=60)
    parser.add_argument("--method",choices=["multiwindow"])
    args=parser.parse_args()
    run(args.arm,args.profile,args.reps,args.waves,args.duration_min,method=args.method)
