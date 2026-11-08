"""Owner-run simultaneous bottleneck/stranded comparison; about 7-12 minutes."""
from collections import deque
from dataclasses import dataclass,field
import json
from pathlib import Path
import sys
import threading
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.stackcmp_common import *


@dataclass
class Member:
    topic:str
    rate:float
    ready:threading.Event=field(default_factory=threading.Event)
    stop:threading.Event=field(default_factory=threading.Event)
    error:object=None

    def run(self):
        from confluent_kafka import Consumer,TopicPartition
        consumer=Consumer({"bootstrap.servers":"localhost:39092","group.id":self.topic,
                           "enable.auto.commit":False,"enable.auto.offset.store":False,"auto.offset.reset":"earliest"})
        def assign(c,parts):
            for part in parts: part.offset=0
            c.assign(parts)
            c.commit(offsets=[TopicPartition(part.topic,part.partition,0) for part in parts],asynchronous=False)
            self.ready.set()
        consumer.subscribe([self.topic],on_assign=assign)
        queue=deque();credit=0.;last=time.monotonic()
        try:
            while not self.stop.is_set():
                message=consumer.poll(.05)
                now=time.monotonic();credit=min(100,credit+(now-last)*self.rate);last=now
                if message is not None and not message.error(): queue.append(message)
                offsets={}
                while queue and credit>=1:
                    msg=queue.popleft();offsets[msg.partition()]=msg.offset()+1;credit-=1
                if offsets: consumer.commit(offsets=[TopicPartition(self.topic,p,o) for p,o in offsets.items()],asynchronous=False)
        except BaseException as exc:
            self.error=repr(exc);self.ready.set()
        finally: consumer.close()


def first_firings(snapshot,group,elapsed,khm,prom):
    for rec in snapshot.get("khm",[]):
        if rec["group_id"]==group: khm.setdefault(rec["id"].rsplit("-",1)[-1],elapsed)
    for alert in snapshot.get("prometheus",[]):
        if alert.get("state")=="firing" and alert["labels"].get("consumergroup")==group:
            prom.setdefault(alert["labels"]["alertname"],dict(observed_after_seconds=elapsed,active_at=alert.get("activeAt")))


def observe(output,scenario,group,seconds):
    started=time.monotonic();first_khm={};first_prom={};samples=[]
    with (output/(scenario+".jsonl")).open("x",encoding="utf-8") as handle:
        while time.monotonic()-started<seconds:
            snapshot=dict(timestamp=utc(),elapsed_seconds=time.monotonic()-started)
            try:
                snapshot["khm"]=get_json("http://127.0.0.1:38080/api/recommendations")["recommendations"]
                snapshot["status"]=get_json("http://127.0.0.1:38080/api/status")["data"]
                snapshot["prometheus"]=get_json("http://127.0.0.1:39090/api/v1/alerts")["data"]["alerts"]
            except Exception as exc: snapshot["error"]=repr(exc)
            # Use observation completion time, not an earlier time before HTTP calls.
            elapsed=time.monotonic()-started
            first_firings(snapshot,group,elapsed,first_khm,first_prom)
            handle.write(json.dumps(snapshot)+"\n");samples.append(snapshot)
            time.sleep(min(1,max(0,seconds-(time.monotonic()-started))))
    return dict(group=group,window_seconds=seconds,first_khm_seconds=first_khm,first_prometheus=first_prom,
                polls=len(samples),poll_errors=sum("error" in sample for sample in samples),
                timing="First observed response after scenario event; <=1-second polling plus request latency. No observation means not seen in this fixed window.")


def create_scenario_topics(topics):
    from confluent_kafka.admin import AdminClient, NewTopic
    # Futures do not keep the AdminClient alive; retain it until every result resolves.
    admin = AdminClient({"bootstrap.servers": "localhost:39092"})
    futures = admin.create_topics([NewTopic(topic, 3, 1) for topic in topics])
    for future in futures.values():
        future.result(30)


def run():
    prep=prepared();output=phase("functional")
    env=environment(output,output/"bootstrap")
    members=[];threads=[];pump_stop=threading.Event();pump_error=[]
    manifest=dict(commit=commit(),status="running",started_at=utc(),images=prep["images"],
                  bottleneck=dict(partitions=3,members=1,produce_per_second=60,consume_per_second=10),
                  stranded=dict(partitions=3,members_before=1,members_after=0,waiting_messages=12000),
                  unsupported_rule="StuckRebalance: state metric absent in pinned stock exporter; not simulated")
    write_json(output/"manifest.json",manifest)
    try:
        compose(env,"up","-d","--force-recreate","--wait","kafka",timeout=240)
        from confluent_kafka import Producer
        prefix="stackcmp-"+str(int(time.time()))
        topics=[prefix+"-bottleneck",prefix+"-stranded"]
        create_scenario_topics(topics)
        members=[Member(topics[0],10),Member(topics[1],0)]
        threads=[threading.Thread(target=m.run,daemon=True) for m in members]
        for thread in threads: thread.start()
        for member in members:
            if not member.ready.wait(60) or member.error: raise RuntimeError("Consumer assignment failed: "+str(member.error))
        env=environment(output,output/"comparison",topics,topics)
        start_monitors(env,["khm","exporter","prometheus"])
        wait_for(lambda:len(get_json("http://127.0.0.1:38080/api/status")["data"])==2)
        wait_for(lambda:len(query("stackcmp:lag"))==2)
        producer=Producer({"bootstrap.servers":"localhost:39092"})
        def produce(topic,count):
            for index in range(count): producer.produce(topic,partition=index%3,value=b"stackcmp")
            if producer.flush(20): raise RuntimeError("Undelivered scenario messages")
        def pump():
            try:
                while not pump_stop.is_set():
                    produce(topics[0],60);pump_stop.wait(1)
            except BaseException as exc: pump_error.append(repr(exc))
        manifest["bottleneck_start"]=utc();write_json(output/"manifest.json",manifest)
        worker=threading.Thread(target=pump,daemon=True);worker.start()
        bottleneck=observe(output,"bottleneck",topics[0],180)
        pump_stop.set();worker.join(25)
        if worker.is_alive() or pump_error: raise RuntimeError("Producer failed: "+repr(pump_error))
        produce(topics[1],12000)
        manifest["stranded_stop_requested"]=utc();write_json(output/"manifest.json",manifest)
        members[1].stop.set()
        stranded=observe(output,"stranded",topics[1],120)
        if any(member.error for member in members): raise RuntimeError("Consumer worker failure")
        write_json(output/"summary.json",dict(commit=commit(),bottleneck=bottleneck,stranded=stranded,
                   caution="Same simultaneous Kafka trajectories; monitoring cadences/phases differ. Negative outcomes must be reported, not retried until favourable."))
        manifest.update(status="complete",ended_at=utc());write_json(output/"manifest.json",manifest)
        print("Functional complete:",output/"summary.json")
    except BaseException as exc:
        manifest.update(status="failed",ended_at=utc(),error=repr(exc));write_json(output/"manifest.json",manifest);raise
    finally:
        pump_stop.set()
        for member in members: member.stop.set()
        for thread in threads:
            if thread.is_alive(): thread.join(30)
        stop_monitors(env);compose(env,"stop","kafka")


if __name__=="__main__": run()
