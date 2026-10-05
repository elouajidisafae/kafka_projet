"""Interleaved idle/load/outage measurements for three tools on one broker."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import statistics
import sys
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.paper_common import *

TOOLS={"khm":("khm-paper-monitor","http://localhost:18080/api/status"),
       "kafdrop":("khm-paper-kafdrop","http://localhost:19000/"),
       "kafka-ui":("khm-paper-kafka-ui","http://localhost:18081/api/clusters")}


def served_content(tool, response):
    if response['status']!=200:
        return f"HTTP {response['status']}: unavailable/error response"
    if tool=='kafdrop':
        return 'HTTP 200 HTML overview; retained body requires inspection for broker errors or cached content'
    try:
        body=json.loads(response['body'])
    except ValueError:
        return 'HTTP 200 non-JSON response'
    if tool=='khm':
        return f"Stored lag snapshot ({len(body.get('data', []))} rows); this endpoint does not establish broker reachability"
    return 'Cluster metadata JSON: '+json.dumps(body,sort_keys=True)[:1000]


def start_tools(image, directory, cfg):
    start_khm(image,directory,cfg)
    for tool,port,environment in [
        ("kafdrop",19000,["KAFKA_BROKERCONNECT=kafka:29092","SERVER_PORT=9000"]),
        ("kafka-ui",18081,["KAFKA_CLUSTERS_0_NAME=bench","KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092"])]:
        name=TOOLS[tool][0]
        if name in command("docker","ps","-a","--format","{{.Names}}").splitlines(): command("docker","rm","-f",name)
        args=["docker","run","-d","--name",name,"--network","khm-bench_default","--memory","2g",
              "-p",f"127.0.0.1:{port}:{9000 if tool=='kafdrop' else 8080}"]
        for value in environment: args += ["-e",value]
        command(*args,IMAGES[tool])
    for _,url in TOOLS.values(): wait_for(lambda:http(url)["status"]==200)


def measure_idle(seconds, directory):
    samples=[];deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        samples.append(dict(timestamp=utc(),tools=resources([t[0] for t in TOOLS.values()])))
        time.sleep(min(1,max(0,deadline-time.monotonic())))
    write_json(directory/"resources.json",samples)
    return [dict(tool=t,mean_cpu_percent=statistics.mean(s['tools'][c]['cpu_percent'] for s in samples),
                 mean_ram_bytes=statistics.mean(s['tools'][c]['ram_bytes'] for s in samples)) for t,(c,_) in TOOLS.items()]


def measure_load(seconds,directory):
    timing={};samples=[]
    ready=threading.Barrier(151, action=lambda:timing.update(end=time.monotonic()+seconds))
    def worker(tool,url):
        ready.wait();answers=[]
        while time.monotonic()<timing['end']:
            response=http(url,timeout=10)
            if time.monotonic()<=timing['end']:
                answers.append(dict(tool=tool,timestamp=response['timestamp'],seconds=response['seconds'],status=response['status']))
        return answers
    with ThreadPoolExecutor(max_workers=150) as pool:
        futures=[pool.submit(worker,t,url) for t,(_,url) in TOOLS.items() for _ in range(50)]
        ready.wait()
        while time.monotonic()<timing['end']:
            samples.append(dict(timestamp=utc(),tools=resources([t[0] for t in TOOLS.values()])))
        responses=[r for f in futures for r in f.result()]
    write_csv(directory/"requests.csv",responses);write_json(directory/"resources.json",samples)
    result=[]
    for tool,(container,_) in TOOLS.items():
        selected=[r for r in responses if r['tool']==tool];good=[r for r in selected if r['status']==200]
        result.append(dict(tool=tool,response_seconds=statistics.median(r['seconds'] for r in good) if good else None,
                           throughput_per_second=len(good)/seconds,request_failures=len(selected)-len(good),
                           peak_cpu_percent=max(s['tools'][container]['cpu_percent'] for s in samples),
                           mean_ram_bytes=statistics.mean(s['tools'][container]['ram_bytes'] for s in samples),
                           peak_ram_bytes=max(s['tools'][container]['ram_bytes'] for s in samples)))
    return result


def measure_outage(seconds,directory,topic,group):
    # A post-restart sentinel proves fresh broker data, not just a cached HTTP 200.
    before=api('/api/status')['data']
    base_lag=next(r['total_lag'] for r in before if r['group_id']==group)
    observations=[]
    try:
        command('docker','stop','khm-bench-kafka-1')
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            with ThreadPoolExecutor(max_workers=3) as pool:
                answers=list(pool.map(lambda item:(item[0],http(item[1][1])),TOOLS.items()))
            observations.extend(dict(tool=t,phase='outage',**r) for t,r in answers)
            time.sleep(min(1,max(0,deadline-time.monotonic())))
    finally:
        restart=time.monotonic();command('docker','start','khm-bench-kafka-1')
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient,NewTopic
    admin=AdminClient({'bootstrap.servers':'localhost:19092'})
    wait_for(lambda:admin.list_topics(timeout=3).brokers)
    sentinel=topic+'-recovery'
    for future in admin.create_topics([NewTopic(sentinel,1,1)]).values(): future.result(30)
    producer=Producer({'bootstrap.servers':'localhost:19092'})
    for _ in range(99): producer.produce(topic,value=b'recovery')
    if producer.flush(20): raise RuntimeError('Recovery sentinel not delivered')
    recovered={};deadline=restart+180
    while time.monotonic()<deadline and len(recovered)<3:
        urls={'khm':TOOLS['khm'][1], 'kafdrop':TOOLS['kafdrop'][1],
              'kafka-ui':'http://localhost:18081/api/clusters/bench/topics?perPage=1000'}
        for tool,url in urls.items():
            if tool in recovered: continue
            response=http(url);fresh=False
            if response['status']==200:
                if tool=='khm':
                    fresh=any(r['group_id']==group and r['total_lag']>=base_lag+99 for r in json.loads(response['body'])['data'])
                else: fresh=sentinel in response['body']
            observations.append(dict(tool=tool,phase='recovery',fresh=fresh,**response))
            if fresh: recovered[tool]=time.monotonic()-restart
        time.sleep(1)
    write_json(directory/'outage-responses.json',observations)
    for future in admin.delete_topics([sentinel]).values(): future.result(30)
    result=[]
    for tool in TOOLS:
        responses=[r for r in observations if r['tool']==tool and r['phase']=='outage']
        result.append(dict(tool=tool,api_available=all(r['status']==200 for r in responses),
                           outage_http_200_fraction=sum(r['status']==200 for r in responses)/len(responses),
                           automatic_recovery=tool in recovered,recovery_seconds=recovered.get(tool),
                           serves=' | '.join(sorted({served_content(tool,r) for r in responses}))))
    return result


def run(commit,smoke=False):
    output,manifest=begin('comparative',commit,smoke);rows=[]
    try:
        quiet_docker(['khm-bench-kafka-1','khm-bench-khm-1']+[v[0] for v in TOOLS.values()])
        image=broker_up(output)
        for tool in ['kafdrop','kafka-ui']: command('docker','pull',IMAGES[tool],timeout=900)
        groups,topics=seed_pairs(10,'paper-compare-'+str(int(time.time())))
        cfg=benchmark_config(groups,topics)
        manifest.update(images={k:image_details(v) for k,v in dict(IMAGES,khm=image).items()},
            repetitions=1 if smoke else 5,warmup_seconds=10 if smoke else 60,
            windows_seconds={'idle':10 if smoke else 600,'load':5 if smoke else 120,'outage':5 if smoke else 180},
            endpoints={k:v[1] for k,v in TOOLS.items()},concurrency_per_tool=50,
            fairness='All three tools run simultaneously with equal 2 GiB container limits; same broker and data. Overview endpoints have different semantics; not a feature-equivalent API benchmark.',
            memory='Docker stats memory usage excluding reclaimable cache (bytes); CPU percent where 100% is one core')
        write_json(output/'manifest.json',manifest)
        for rep in range(manifest['repetitions']):
            for scenario in rotated(['idle','load','outage'],rep):
                directory=output/f'r{rep}-{scenario}';directory.mkdir()
                start_tools(image,directory,cfg);time.sleep(manifest['warmup_seconds'])
                seconds=manifest['windows_seconds'][scenario]
                measured=measure_idle(seconds,directory) if scenario=='idle' else measure_load(seconds,directory) if scenario=='load' else measure_outage(seconds,directory,topics[0],groups[0])
                rows.extend(dict(repetition=rep,scenario=scenario,**r) for r in measured)
                fields=sorted({k for r in rows for k in r});write_csv(output/'per_rep.csv',rows,fields)
                for container,_ in TOOLS.values(): command('docker','stop',container)
                print(f'Repetition {rep+1}: {scenario} complete',flush=True)
        metrics=['mean_cpu_percent','mean_ram_bytes','peak_cpu_percent','peak_ram_bytes','response_seconds',
                 'throughput_per_second','request_failures','outage_http_200_fraction','recovery_seconds']
        summary=summarise(rows,['scenario','tool'],metrics)
        for item in summary:
            selected=[r for r in rows if all(r[k]==item[k] for k in ['scenario','tool'])]
            if item['scenario']=='outage':
                item.update(api_available_repetitions=sum(r['api_available'] for r in selected),
                            recovered_repetitions=sum(r['automatic_recovery'] for r in selected),repetitions=len(selected),
                            serves=sorted({r['serves'] for r in selected}))
        write_json(output/'summary.json',dict(commit=commit,smoke=smoke,rows=summary,
                   aggregation='median [Q1,Q3] across repetition statistics; unavailable recovery times remain null'))
        cleanup_pairs(groups,topics);finish(output,manifest)
    except BaseException as exc:
        finish(output,manifest,exc);raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--commit',required=True)
    parser.add_argument('--smoke',action='store_true');args=parser.parse_args();run(args.commit,args.smoke)
