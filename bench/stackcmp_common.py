"""Isolated deployment mechanics for the pinned stack comparison."""
from collections import defaultdict
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time
from urllib.request import urlopen
import yaml
from bench.common import ROOT,utc,write_json
from bench.forecast_eval import quantiles
from bench.paper_common import rotated,byte_quantity

RESULTS=ROOT/"bench/results/stack-comparison"
COMPOSE=ROOT/"docker-compose.stackcmp.yml"
CONFIG=ROOT/"bench/stackcmp"
IMAGES={"kafka":"confluentinc/cp-kafka:7.6.0","exporter":"danielqsj/kafka-exporter:v1.10.0",
        "prometheus":"prom/prometheus:v3.5.0","grafana":"grafana/grafana:12.1.0"}
SETUPS={"khm":["khm"],"prometheus":["exporter","prometheus"],"grafana":["exporter","prometheus","grafana"]}
MEASURED=["khm","exporter","prometheus","grafana"]


def command(*args,env=None,timeout=180):
    return subprocess.check_output(list(args),cwd=ROOT,env=env,text=True,encoding="utf-8",errors="replace",timeout=timeout).strip()


def commit():
    if command("git","status","--porcelain"): raise ValueError("Commit source changes before stack comparison")
    return command("git","rev-parse","HEAD")


def phase_name(name):
    attempt = os.environ.get("STACKCMP_ATTEMPT", "")
    if attempt and not re.fullmatch(r"[A-Za-z0-9_-]+", attempt):
        raise ValueError("STACKCMP_ATTEMPT must contain only letters, digits, underscores or hyphens")
    return name + ("-" + attempt if attempt else "")


def phase(name):
    output=RESULTS/phase_name(name)
    output.mkdir(parents=True,exist_ok=False)
    return output


def get_json(url):
    with urlopen(url,timeout=10) as response: return json.load(response)


def wait_for(test,seconds=180):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            result=test()
            if result: return result
        except Exception: pass
        time.sleep(1)
    raise TimeoutError("Readiness/metric condition not observed")


def writable(path):
    path.mkdir(parents=True,exist_ok=True)
    path.chmod(0o777)
    return path


def environment(output,directory,groups=(),topics=()):
    cfg=writable(directory/"config");state=writable(directory/"state")
    for service in MEASURED: writable(state/service)
    template=yaml.safe_load((CONFIG/"khm.yml").read_text())
    template["include_groups"]=list(groups)
    if topics:
        from confluent_kafka.admin import AdminClient
        existing=AdminClient({"bootstrap.servers":"127.0.0.1:39092"}).list_topics(timeout=20).topics
        template["exclude_topics"]=[t for t in existing if t not in topics]
    (cfg/"khm.yml").write_text(yaml.safe_dump(template),encoding="utf-8")
    (cfg/"prometheus.yml").write_bytes((CONFIG/"prometheus.yml").read_bytes())
    return dict(os.environ,STACKCMP_KHM_IMAGE="khm-stackcmp:"+commit(),
                STACKCMP_BROKER_DIR=str(writable(output/"broker")),
                STACKCMP_CONFIG_DIR=str(cfg),STACKCMP_STATE_DIR=str(state),
                STACKCMP_GROUP_REGEX="^"+re.escape(os.path.commonprefix(list(groups)))+".*$" if groups else ".*",
                STACKCMP_TOPIC_REGEX="^"+re.escape(os.path.commonprefix(list(topics)))+".*$" if topics else ".*")


def compose(env,*args,timeout=180):
    return command("docker","compose","-f",str(COMPOSE),"--profile","grafana",*args,env=env,timeout=timeout)


def stop_monitors(env):
    compose(env,"stop",*MEASURED)


def start_monitors(env,services):
    stop_monitors(env)
    compose(env,"up","-d","--no-deps","--force-recreate",*services)
    if "khm" in services: wait_for(lambda:get_json("http://127.0.0.1:38080/api/status"))
    if "prometheus" in services:
        wait_for(lambda:get_json("http://127.0.0.1:39090/api/v1/query?query=up")["status"]=="success")
    if "grafana" in services: wait_for(lambda:get_json("http://127.0.0.1:33000/api/health")["database"]=="ok")


def ids(env,services):
    return {service:compose(env,"ps","-q",service) for service in services}


def query(expression):
    from urllib.parse import urlencode
    result=get_json("http://127.0.0.1:39090/api/v1/query?"+urlencode({"query":expression}))
    if result["status"]!="success": raise ValueError(result)
    return result["data"]["result"]


def image_record(image):
    data=json.loads(command("docker","image","inspect",image))[0]
    return dict(reference=image,id=data["Id"],repo_digests=data.get("RepoDigests",[]),
                logical_size_bytes=data["Size"],layers=data["RootFS"]["Layers"])


def prepared():
    manifest=json.loads((RESULTS/phase_name("prepare")/"manifest.json").read_text())
    if manifest["commit"]!=commit() or manifest["status"]!="complete": raise ValueError("Run prepare for this exact committed source first")
    for record in manifest["images"].values():
        if image_record(record["reference"])["id"]!=record["id"]: raise ValueError("Pinned image changed since preparation")
    return manifest


def prepare():
    output=phase("prepare");sha=commit()
    manifest=dict(commit=sha,status="running",started_at=utc())
    write_json(output/"manifest.json",manifest)
    for image in list(IMAGES.values())+["python:3.11.13-slim-bookworm"]:
        print("Pull:",image,flush=True);command("docker","pull",image,timeout=1200)
    khm="khm-stackcmp:"+sha
    command("docker","build","-f","bench/stackcmp/Dockerfile","--label","org.opencontainers.image.revision="+sha,"-t",khm,".",timeout=1800)
    manifest.update(status="complete",ended_at=utc(),docker_version=command("docker","version","--format","{{json .}}"),
                    images={name:image_record(image) for name,image in dict(IMAGES,khm=khm,python_base="python:3.11.13-slim-bookworm").items()})
    (output/"docker-disk-usage.json").write_text(command("docker","system","df","-v","--format","{{json .}}"),encoding="utf-8")
    write_json(output/"manifest.json",manifest)
    print("Prepared:",output)


def stats(containers):
    raw=command("docker","stats","--no-stream","--format","{{json .}}",*containers.values(),timeout=30)
    values=[json.loads(line) for line in raw.splitlines()]
    if len(values)!=len(containers): raise ValueError("Missing docker stats sample")
    parsed=[dict(container=r["Name"],memory_bytes=byte_quantity(r["MemUsage"].split("/")[0]),
                 cpu_percent=float(r["CPUPerc"].rstrip("%"))) for r in values]
    return dict(timestamp=utc(),containers=parsed,memory_bytes=sum(r["memory_bytes"] for r in parsed),
                cpu_percent=sum(r["cpu_percent"] for r in parsed))


def effort(setup):
    files=[COMPOSE]
    if setup=="khm": files += [CONFIG/"khm.yml"]
    else: files += [CONFIG/"prometheus.yml",CONFIG/"alerts.yml"]
    if setup=="grafana": files += [CONFIG/"datasource.yml",CONFIG/"dashboard-provider.yml",CONFIG/"lag-dashboard.json"]
    return dict(files=len(files),lines=sum(len(p.read_text().splitlines()) for p in files),
                nonblank_noncomment_lines=sum(sum(bool(line.strip()) and not line.lstrip().startswith("#") for line in p.read_text().splitlines()) for p in files),
                paths=[str(p.relative_to(ROOT)) for p in files],
                definition="Hand-authored deployable source files, including the ENTIRE shared compose file for every setup; runtime generated substitutions, build recipes and runner code excluded. Formatting-dependent count, not time/complexity.")


def check_committed_partitions(group, partitions):
    errors = [f"partition {part.partition}: code={part.error.code()} {part.error}"
              for part in partitions if part.error is not None and part.error.code() != 0]
    if errors:
        raise RuntimeError(f"Offset seed failure for {group}: " + "; ".join(errors))


def seed_committed_offsets(consumer, group, topic, timeout=45, retry_delay=.5):
    from confluent_kafka import KafkaError, KafkaException, TopicPartition
    transient = {getattr(KafkaError, name) for name in
                 ("NOT_COORDINATOR", "COORDINATOR_LOAD_IN_PROGRESS", "COORDINATOR_NOT_AVAILABLE",
                  "REQUEST_TIMED_OUT", "_TIMED_OUT", "_WAIT_COORD", "_TRANSPORT") if hasattr(KafkaError, name)}
    deadline = time.monotonic() + timeout
    last = "coordinator not ready"
    while time.monotonic() < deadline:
        try:
            response = consumer.commit(offsets=[TopicPartition(topic,p,0) for p in range(3)], asynchronous=False)
            errors = [part.error for part in response if part.error is not None and part.error.code() != 0]
            if errors:
                if not all(error.code() in transient for error in errors):
                    check_committed_partitions(group, response)
                last = "; ".join(str(error) for error in errors)
            else:
                actual = consumer.committed([TopicPartition(topic,p) for p in range(3)], timeout=min(10,max(.1,deadline-time.monotonic())))
                errors = [part.error for part in actual if part.error is not None and part.error.code() != 0]
                if errors and not all(error.code() in transient for error in errors):
                    check_committed_partitions(group, actual)
                if not errors and len(actual) == 3 and all(part.offset == 0 for part in actual):
                    return
                last = f"offset readback not ready: {actual}"
        except KafkaException as exc:
            if not exc.args or exc.args[0].code() not in transient:
                raise
            last = str(exc)
        consumer.poll(0)  # Let the client refresh coordinator metadata.
        time.sleep(retry_delay)
    raise TimeoutError(f"Offset seed coordinator/readback timeout for {group}: {last}")


def seed_pairs(size,prefix):
    # Identical paper_scalability workload: one 3-partition topic, 300 messages,
    # size inactive groups committed at zero on every partition. Only the port differs.
    from confluent_kafka import Consumer,Producer,TopicPartition
    from confluent_kafka.admin import AdminClient,NewTopic
    admin=AdminClient({"bootstrap.servers":"127.0.0.1:39092"});topic=prefix+"-topic"
    for future in admin.create_topics([NewTopic(topic,3,1)]).values(): future.result(30)
    producer=Producer({"bootstrap.servers":"127.0.0.1:39092"})
    for index in range(300): producer.produce(topic,partition=index%3,value=b"paper")
    if producer.flush(30): raise RuntimeError("Undelivered workload messages")
    groups=[prefix+f"-g{i:03}" for i in range(size)]
    for group in groups:
        consumer=Consumer({"bootstrap.servers":"127.0.0.1:39092","group.id":group,"enable.auto.commit":False,"socket.timeout.ms":10000})
        try:
            consumer.assign([TopicPartition(topic,p,0) for p in range(3)])
            seed_committed_offsets(consumer, group, topic)
        finally: consumer.close()
    return groups,[topic]


def cleanup(groups,topics):
    from confluent_kafka.admin import AdminClient
    admin=AdminClient({"bootstrap.servers":"127.0.0.1:39092"})
    for action,names in [(admin.delete_consumer_groups,groups),(admin.delete_topics,topics)]:
        for future in action(names).values(): future.result(30)
