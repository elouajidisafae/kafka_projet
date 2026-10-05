"""Frozen-source guards and shared mechanics for final paper measurements."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import urllib.error
import urllib.request

import yaml
from confluent_kafka import KafkaException

from bench.common import ROOT, utc, write_json, write_csv
from bench.forecast_eval import quantiles

IMAGES = {"kafka": "confluentinc/cp-kafka:7.6.0",
          "kafdrop": "obsidiandynamics/kafdrop:4.0.2",
          "kafka-ui": "provectuslabs/kafka-ui:v0.7.2"}


def command(*args, timeout=180, env=None):
    return subprocess.check_output(list(args), cwd=ROOT, env=env, text=True,
                                   encoding="utf-8", errors="replace", timeout=timeout).strip()


def frozen_commit(expected):
    actual = command("git", "rev-parse", "HEAD")
    if actual != expected or command("git", "status", "--porcelain"):
        raise ValueError("Use the expected frozen commit with a clean working tree before any run")
    return actual


def host_details():
    import psutil
    def optional(*args):
        try:
            data=subprocess.check_output(list(args),cwd=ROOT,timeout=20)
            return data.decode('utf-16' if b'\x00' in data else 'utf-8',errors='replace').strip()
        except (OSError, subprocess.SubprocessError):
            return None
    cpu=optional('powershell','-NoProfile','-Command',
                 'Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors | ConvertTo-Json -Compress') if os.name=='nt' else platform.processor()
    desktop=optional('powershell','-NoProfile','-Command',
                     '(Get-Item "$env:ProgramFiles\\Docker\\Docker\\Docker Desktop.exe").VersionInfo.ProductVersion') if os.name=='nt' else None
    return dict(os=platform.platform(), cpu=cpu or platform.processor(), logical_cpus=os.cpu_count(),
                ram_bytes=psutil.virtual_memory().total, python=platform.python_version(),
                docker=json.loads(command("docker", "version", "--format", "{{json .}}")),
                docker_info=json.loads(command("docker", "info", "--format", "{{json .}}")),
                wsl_status=optional("wsl", "--status") if os.name=="nt" else None,
                docker_desktop_version=desktop,
                images=IMAGES)


def begin(kind, commit, smoke=False):
    frozen_commit(commit)
    output = ROOT/"bench/results"/(kind+"-smoke" if smoke else kind)
    output.mkdir(parents=True, exist_ok=True)
    if (output/"manifest.json").exists():
        raise ValueError(f"Existing run retained at {output}; review/archive it before another run")
    manifest = dict(commit=commit, started_at=utc(), status="running", smoke=smoke,
                    host=host_details(), source_hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in [*(ROOT/"bench").glob("paper_*.py"), ROOT/"bench/capture.py"]})
    write_json(output/"manifest.json", manifest)
    return output, manifest


def finish(output, manifest, error=None):
    if error is None:
        frozen_commit(manifest['commit'])
    manifest.update(ended_at=utc(), status="failed" if error else "complete")
    if error:
        manifest["error"] = repr(error)
    write_json(output/"manifest.json", manifest)
    print(f"{manifest['status']}: {output}",flush=True)


def quiet_docker(allowed):
    running = set(command("docker", "ps", "--format", "{{.Names}}").splitlines())
    unexpected = running-set(allowed)
    if unexpected:
        raise ValueError("Stop unrelated containers before benchmarking: "+", ".join(sorted(unexpected)))


def http(url, timeout=5):
    start=time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body=response.read().decode("utf-8", errors="replace")
            status=response.status
    except urllib.error.HTTPError as exc:
        status,body=exc.code,exc.read().decode("utf-8", errors="replace")
    except (OSError, TimeoutError) as exc:
        status,body=0,str(exc)
    return dict(timestamp=utc(), seconds=time.monotonic()-start, status=status, body=body)


def api(path, base="http://localhost:18080"):
    response=http(base+path)
    if response["status"]!=200:
        raise RuntimeError(str(response))
    return json.loads(response["body"])


def wait_for(test, seconds=180):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            value=test()
            if value:
                return value
        except (ValueError, OSError, RuntimeError, KafkaException):
            pass
        time.sleep(1)
    raise TimeoutError("Readiness or scenario condition did not become true")


def image_details(image):
    return json.loads(command("docker", "image", "inspect", image))[0]


def rotated(values, repetition):
    n=repetition%len(values)
    return values[n:]+values[:n]


def metric(text, name, cluster="bench"):
    match=re.search(r"^"+re.escape(name)+r'(?:\{cluster="'+re.escape(cluster)+r'"\})? ([0-9.eE+-]+)$', text, re.M)
    if not match:
        raise ValueError("Missing metric "+name)
    return float(match[1])


def byte_quantity(text):
    value,unit=re.match(r"([0-9.]+)\s*([A-Za-z]+)",text).groups()
    factors={"B":1,"kB":1000,"KB":1000,"MB":1000**2,"GB":1000**3,
             "KiB":1024,"MiB":1024**2,"GiB":1024**3}
    return float(value)*factors[unit]


def resources(containers):
    raw=command("docker", "stats", "--no-stream", "--format", "{{json .}}", *containers, timeout=30)
    return {r["Name"]:dict(cpu_percent=float(r["CPUPerc"].rstrip("%")),
            ram_bytes=byte_quantity(r["MemUsage"].split("/")[0]))
            for r in map(json.loads,raw.splitlines())}


def rss(container):
    code="from pathlib import Path; print(next(int(x.split()[1])*1024 for x in Path('/proc/1/status').read_text().splitlines() if x.startswith('VmRSS:')))"
    return int(command("docker","exec",container,"python","-c",code,timeout=20))


def benchmark_config(groups, topics, match=False):
    cfg=yaml.safe_load((ROOT/"config.bench.yml").read_text())
    cfg["forecast"].update(method="baseline",show_range=False)
    cfg["monitor"]["group_topic_match"]=match
    cfg["include_groups"]=groups
    # Only these topics should be visible to this run; application already supports exclusions.
    from confluent_kafka.admin import AdminClient
    all_topics=AdminClient({"bootstrap.servers":"localhost:19092"}).list_topics(timeout=20).topics
    cfg["exclude_topics"]=[t for t in all_topics if t not in topics]
    return cfg


def broker_up(output):
    if "khm-bench-khm-1" in command("docker","ps","--format","{{.Names}}").splitlines():
        command("docker","stop","khm-bench-khm-1")
    env=dict(os.environ,KHM_RUN_ID="paper",KHM_BENCH_CONFIG=str(ROOT/"config.bench.yml"))
    commit=command('git','rev-parse','HEAD')
    # Reuse the existing broker service, but never measure unrelated historical topics.
    # The previous benchmark volume is retained; no second broker is started.
    override=output/'broker-compose.yml'
    override.write_text(yaml.safe_dump({'services':{'kafka':{'volumes':['paper-broker:/var/lib/kafka/data']}},
                                       'volumes':{'paper-broker':{'name':'khm-paper-broker-'+commit}}}),encoding='utf-8')
    command("docker","compose","-f","docker-compose.bench.yml",'-f',str(override),"up","-d","--wait","kafka",env=env)
    from confluent_kafka.admin import AdminClient
    topics=AdminClient({'bootstrap.servers':'localhost:19092'}).list_topics(timeout=20).topics
    unexpected=[name for name in topics if not name.startswith('__')]
    if unexpected:
        raise ValueError('Broker contains topics from another or failed run; retain/review them before proceeding: '+repr(unexpected))
    image="khm-paper:"+commit
    if not command("docker","image","ls","-q",image):
        command("docker","build","-t",image,".",timeout=900)
    return image


def start_khm(image, directory, cfg, name="khm-paper-monitor", network="khm-bench_default", port=18080):
    directory.mkdir(parents=True,exist_ok=True)
    data=directory/"data";data.mkdir(exist_ok=True)
    config=directory/"config.yml";config.write_text(yaml.safe_dump(cfg),encoding="utf-8")
    existing=command("docker","ps","-a","--format","{{.Names}}").splitlines()
    if name in existing:
        command("docker","rm","-f",name)
    command("docker","run","-d","--name",name,"--network",network,"--memory","2g",
            "-p",f"127.0.0.1:{port}:8080","-e","KHM_DB_PATH=/app/data/lag_history.db",
            "--mount",f"type=bind,source={config},target=/app/config.yml",
            "--mount",f"type=bind,source={data},target=/app/data",image)
    wait_for(lambda:http(f"http://localhost:{port}/api/status")["status"]==200)


def seed_pairs(size, prefix):
    """Real committed consumer groups, one topic: N group/topic pairs, no mock broker."""
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    admin=AdminClient({"bootstrap.servers":"localhost:19092"})
    topic=prefix+"-topic"
    for f in admin.create_topics([NewTopic(topic,num_partitions=3,replication_factor=1)]).values(): f.result(30)
    producer=Producer({"bootstrap.servers":"localhost:19092"})
    for i in range(300): producer.produce(topic,partition=i%3,value=b"paper")
    if producer.flush(30): raise RuntimeError("Undelivered workload messages")
    groups=[prefix+f"-g{i:03}" for i in range(size)]
    for group in groups:
        consumer=Consumer({"bootstrap.servers":"localhost:19092","group.id":group,"enable.auto.commit":False})
        try:
            consumer.assign([TopicPartition(topic,p,0) for p in range(3)])
            answer=consumer.commit(offsets=[TopicPartition(topic,p,0) for p in range(3)],asynchronous=False)
            if any(p.error for p in answer): raise RuntimeError("Offset seed failed")
        finally: consumer.close()
    return groups,[topic]


def cleanup_pairs(groups, topics):
    from confluent_kafka.admin import AdminClient
    admin=AdminClient({"bootstrap.servers":"localhost:19092"})
    for request in (admin.delete_consumer_groups(groups),admin.delete_topics(topics)):
        for future in request.values(): future.result(30)
    wait_for(lambda:not set(topics).intersection(admin.list_topics(timeout=10).topics),seconds=60)


def summarise(rows, keys, metrics):
    result=[]
    identities=sorted({tuple(r[k] for k in keys) for r in rows})
    for identity in identities:
        selected=[r for r in rows if tuple(r[k] for k in keys)==identity]
        result.append(dict(zip(keys,identity),**{m:quantiles([r.get(m) for r in selected]) for m in metrics}))
    return result
