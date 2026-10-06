"""One reproducible three-scenario capture session after frozen benchmarks complete."""
import argparse
import hashlib
from collections import deque
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.paper_common import *
from datetime import datetime


def source_hashes():
    """Record the actual rehearsal tree, including uncommitted public files."""
    paths=command('git','ls-files','--cached','--others','--exclude-standard','-z').split('\0')
    return {name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
            for name in sorted(set(paths)) if name and (ROOT/name).is_file()}


def prepare_capture(commit, rehearsal=False):
    if rehearsal:
        if command('git','rev-parse','HEAD')!=commit:
            raise ValueError('Rehearsal base commit does not match HEAD')
        stamp=datetime.now().strftime('%Y%m%dT%H%M%S%f')
        output=ROOT/'bench/results'/('capture-rehearsal-'+stamp)
        output.mkdir(parents=True)
        manifest=dict(commit=None,base_commit=commit,rehearsal=True,
                      analysis_class='validation_only',eligible_for_paper=False,
                      started_at=utc(),status='running',source_hashes=source_hashes())
        write_json(output/'manifest.json',manifest)
        return output,manifest,'khm-capture-rehearsal:'+stamp.lower()
    # Normal paper captures still require a clean committed tree and full runs.
    frozen_commit(commit)
    images=[]
    for name in ['scalability','comparative']:
        manifest=json.loads((ROOT/'bench/results'/name/'manifest.json').read_text())
        if manifest['commit']!=commit or manifest['status']!='complete' or manifest['smoke']:
            raise ValueError('Complete both full benchmarks on this commit before capture')
        images.append(manifest['images']['khm']['Id'])
    image='khm-paper:'+commit
    if len(set(images+[image_details(image)['Id']]))!=1:
        raise ValueError('Benchmarks and capture must use the same built KHM image')
    output,manifest=begin('capture',commit)
    manifest.update(rehearsal=False,analysis_class='paper_capture',eligible_for_paper=True)
    return output,manifest,image


def finish_capture(output, manifest, error=None):
    if not manifest['rehearsal']:
        return finish(output,manifest,error)
    if error is None and source_hashes()!=manifest['source_hashes']:
        raise ValueError('Source tree changed during rehearsal')
    manifest.update(ended_at=utc(),status='failed' if error else 'complete')
    if error: manifest['error']=repr(error)
    write_json(output/'manifest.json',manifest)
    print(f"{manifest['status']} (validation only): {output}",flush=True)


def settle_charts(page):
    """Render final canvas positions and retain numerical drawing checks."""
    return page.evaluate('''async () => {
      await document.fonts.ready;
      const charts = typeof Chart === 'undefined' ? [] : Object.values(Chart.instances);
      for (const chart of charts) {
        chart.stop();
        chart.options.animation = false;
        chart.options.locale = 'en-GB';
        chart.update('none');
      }
      await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
      return charts.map(chart => {
        let maxError = 0, points = 0;
        chart.data.datasets.forEach((dataset, index) => {
          chart.getDatasetMeta(index).data.forEach((point, j) => {
            const value = dataset.data[j];
            if (value == null || point.skip) return;
            const expected = chart.scales.y.getPixelForValue(value);
            const error = Math.abs(point.y - expected);
            if (!Number.isFinite(error) || error > 0.5)
              throw new Error('Chart is not settled: ' + chart.canvas.id);
            maxError = Math.max(maxError, error); points++;
          });
        });
        return {canvas: chart.canvas.id, points, max_y_error_pixels: maxError,
                labels: chart.data.labels, datasets: chart.data.datasets.map(d => ({label:d.label,data:d.data}))};
      });
    }''')


def audit_excerpt(logs, topic, session_start):
    selected=sorted([r for r in logs if r['event_type']=='CONFIG_CHANGE' or topic in r['message']],
                    key=lambda r:(r['recorded_at'],r['id']))
    changes=[r for r in selected if r['event_type']=='CONFIG_CHANGE']
    if len(changes)!=1:
        raise ValueError('Expected one configuration change')
    change=changes[0]
    details=json.loads(change['details'])
    if details['alerts']['warning_threshold']!=5000:
        raise ValueError('Unexpected changed threshold')
    transitions=[r['severity'] for r in selected if r['id']>change['id'] and r['event_type']=='ALERT']
    if transitions!=['INFO','WARNING','CRITICAL']:
        raise ValueError('Expected resolved OK, WARNING, CRITICAL after configuration change')
    if any(datetime.fromisoformat(r['recorded_at'])<datetime.fromisoformat(session_start) for r in selected):
        raise ValueError('Audit event predates capture session')
    return selected


class ScenarioConsumer:
    """One actual Kafka group member; rate controls processing, not membership."""
    def __init__(self, topic, rate=10):
        self.topic,self.rate=topic,rate
        self.stop=threading.Event();self.ready=threading.Event();self.error=None
        self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()

    def run(self):
        from confluent_kafka import Consumer,TopicPartition
        consumer=Consumer({'bootstrap.servers':'localhost:9092','group.id':self.topic,
                           'enable.auto.commit':False,'enable.auto.offset.store':False,'auto.offset.reset':'earliest'})
        def assign(c,parts):
            for p in parts: p.offset=0
            c.assign(parts)
            c.commit(offsets=[TopicPartition(p.topic,p.partition,0) for p in parts],asynchronous=False)
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
                    m=queue.popleft();offsets[m.partition()]=m.offset()+1;credit-=1
                if offsets:
                    consumer.commit(offsets=[TopicPartition(self.topic,p,o) for p,o in offsets.items()],asynchronous=False)
        except BaseException as exc: self.error=repr(exc)
        finally: consumer.close()

    def close(self):
        self.stop.set();self.thread.join(15)
        if self.thread.is_alive(): raise TimeoutError('Consumer did not stop')
        if self.error: raise RuntimeError(self.error)


def run(commit, rehearsal=False):
    output,manifest,image=prepare_capture(commit,rehearsal)
    consumers=[];pump_stop=threading.Event();browser=None
    try:
        if rehearsal:
            manifest['host']=host_details()
            command('docker','build','-t',image,'.',timeout=900)
        running=command('docker','ps','--format','{{.Names}}').splitlines()
        for name in ['khm-bench-kafka-1','khm-bench-khm-1','khm-paper-monitor','khm-paper-kafdrop','khm-paper-kafka-ui']:
            if name in running: command('docker','stop',name)
        quiet_docker(['khm-zookeeper','khm-kafka','khm-app','khm-producer','khm-consumer-normal','khm-consumer-slow'])
        suffix=str(int(time.time()))[-6:]
        topics=[f's1-bottleneck-{suffix}',f's2-stranded-{suffix}',f's3-masked-{suffix}']
        cfg=yaml.safe_load((ROOT/'config.yml').read_text());cfg['include_groups']=topics
        cfg['monitor']['group_topic_match']=True
        # Allow a fresh CLI process and browser capture inside one collection cycle.
        cfg['monitor']['refresh_interval']=15
        cfg['alerts']=dict(warning_threshold=1000,critical_threshold=10000)
        cfg['forecast'].update(method='baseline',show_range=False)
        config=output/'config.yml';config.write_text(yaml.safe_dump(cfg),encoding='utf-8')
        data=output/'data';data.mkdir()
        env=dict(os.environ,KHM_DEMO_CONFIG=str(config),KHM_DEMO_DATA=str(data))
        compose=['docker','compose','-f','docker-compose.yml']
        command(*compose,'stop','producer-demo','consumer-normal','consumer-slow',env=env)
        override=output/'compose-image.yml'
        override.write_text(yaml.safe_dump({'services':{'kafka-health-monitor':{'image':image,'pull_policy':'never'}}}),encoding='utf-8')
        command(*compose,'-f',str(override),'up','-d','--no-build','--wait','kafka-health-monitor',env=env,timeout=900)
        manifest.update(config=cfg,viewport=dict(width=1440,height=1100,device_scale_factor=2),theme='light',
                        images={name:json.loads(command('docker','inspect',name))[0]['Image'] for name in ['khm-app','khm-kafka','khm-zookeeper']},
                        scenarios=dict(topics=topics,partitions=3,consumer_instances=1,
                                       emerging_produce_per_second=60,emerging_consume_per_second=10,
                                       stranded_messages=12000,masked_batches=[2000,4000,5000]),files=[])
        write_json(output/'manifest.json',manifest)
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient,NewTopic
        admin=AdminClient({'bootstrap.servers':'localhost:9092'})
        for f in admin.create_topics([NewTopic(t,3,1) for t in topics]).values(): f.result(30)
        consumers=[ScenarioConsumer(t) for t in topics]
        for c in consumers:
            if not c.ready.wait(45): raise RuntimeError('Consumer membership did not become ready')
        producer=Producer({'bootstrap.servers':'localhost:9092'})
        def produce(topic,count):
            for i in range(count): producer.produce(topic,partition=i%3,value=b'capture')
            if producer.flush(20): raise RuntimeError('Messages not delivered')
        def status(topic):
            return next((r for r in api('/api/status','http://127.0.0.1:8080')['data'] if r['group_id']==topic),{})
        def pump():
            try:
                while not pump_stop.is_set():
                    produce(topics[0],60);pump_stop.wait(1)
            except BaseException as exc:
                manifest['pump_error']=repr(exc);pump_stop.set()
        pump_thread=threading.Thread(target=pump,daemon=True);pump_thread.start()
        wait_for(lambda:any(r['group_id']==topics[0] and r['id'].endswith('-scale')
                            for r in api('/api/recommendations','http://127.0.0.1:8080')['recommendations']),seconds=180)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(channel='msedge',headless=True)
            context=browser.new_context(viewport={'width':1440,'height':1100},device_scale_factor=2,
                                        locale='en-GB',timezone_id='UTC',color_scheme='light')
            context.add_init_script("localStorage.setItem('khm-theme','light')")
            page=context.new_page()
            errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            def metrics(): return http('http://127.0.0.1:8080/metrics')['body']
            def screenshot(name):
                text=page.locator('body').inner_text()
                for bad in ['Aucune donnee','Mauvais','Configuration mise','dev /','staging /','production /']:
                    if bad in text: raise ValueError('Unexpected capture text: '+bad)
                manifest.setdefault('render_checks',{})[name]=settle_charts(page)
                page.screenshot(path=str(output/name),full_page=True,animations='disabled')
            def capture_pair(figure,listing,topic,chart=False):
                for attempt in range(8):
                    cycle=metric(metrics(),'khm_collection_cycles_total','demo')
                    wait_for(lambda:metric(metrics(),'khm_collection_cycles_total','demo')>cycle,seconds=30)
                    cycle=metric(metrics(),'khm_collection_cycles_total','demo');started=utc()
                    page.goto('http://127.0.0.1:8080',wait_until='networkidle')
                    page.evaluate('async () => {await refresh();await refreshForecast();await refreshHealthScore();await refreshRecommendations();}')
                    recs=api('/api/recommendations','http://127.0.0.1:8080')
                    selected=[r for r in recs['recommendations'] if r['group_id']==topic]
                    required='scale' if chart else 'stranded'
                    if not any(r['id'].endswith('-'+required) for r in selected):
                        raise RuntimeError('Required recommendation missing at capture')
                    cli=command('docker','exec','-e','COLUMNS=200','khm-app','python','main.py','--mode','cli','status','--cluster','demo',timeout=20)
                    title=next(r['title'] for r in selected if r['id'].endswith('-'+required))
                    if topic not in cli or 'Metrics:' not in cli or title not in cli:
                        raise RuntimeError('CLI recommendation missing at capture')
                    if chart:
                        page.locator('.forecast-row[onclick]').filter(has_text=topic).click()
                        page.wait_for_function('forecastChart !== null')
                        page.locator('#forecast-chart-section').scroll_into_view_if_needed()
                        manifest.setdefault('render_checks',{})[figure]=settle_charts(page)
                        page.locator('#forecast-chart-section').screenshot(path=str(output/figure),animations='disabled')
                    else:
                        rows=api('/api/status','http://127.0.0.1:8080')['data'];health=api('/api/health-score','http://127.0.0.1:8080')
                        if health['score']>=90 or not {'WARNING','CRITICAL'} <= {r['status'] for r in rows} or not any(r['group_state']=='EMPTY' for r in rows):
                            raise RuntimeError('Overview is not in the prescribed degraded state')
                        screenshot(figure)
                    text=metrics();ended=utc()
                    if metric(text,'khm_collection_cycles_total','demo')!=cycle: continue
                    write_json(output/(listing+'.json'),dict(started_at=started,ended_at=ended,cycle=cycle,api=selected,cli=cli))
                    (output/(listing+'.txt')).write_text(cli,encoding='utf-8')
                    (output/'listing4-metrics.txt').write_text('\n'.join(line for line in text.splitlines()
                        if line.startswith(('kafka_consumer_lag{','khm_health_score ','khm_collection_duration_seconds{')))+'\n',encoding='utf-8')
                    manifest['files'].append(dict(figure=figure,listing=listing,started_at=started,ended_at=ended,cycle=cycle))
                    manifest['metrics_capture']=dict(file='listing4-metrics.txt',started_at=started,ended_at=ended,cycle=cycle)
                    return
                raise RuntimeError('Could not capture API, CLI and figure within a single collection cycle')
            capture_pair('figure-forecast.png','listing1-bottleneck',topics[0],chart=True)
            pump_stop.set();pump_thread.join(25);consumers[0].rate=0
            if pump_thread.is_alive(): raise RuntimeError('Scenario producer did not stop')
            consumers[1].rate=0;produce(topics[1],12000);consumers[1].close()
            wait_for(lambda:status(topics[1]).get('group_state')=='EMPTY')
            capture_pair('figure-overview.png','listing2-stranded',topics[1])
            consumers[2].rate=0;produce(topics[2],2000)
            wait_for(lambda:status(topics[2]).get('status')=='WARNING')
            manifest['masked_before']=dict(timestamp=utc(),row=status(topics[2]))
            page.goto('http://127.0.0.1:8080/config',wait_until='networkidle')
            page.locator('#warning-threshold').fill('5000')
            # Exercise the existing web form and its real CONFIG_CHANGE audit event.
            page.evaluate('saveConfig()')
            wait_for(lambda:api('/api/config','http://127.0.0.1:8080')['alerts']['warning_threshold']==5000)
            wait_for(lambda:status(topics[2]).get('status')=='OK')
            manifest['masked_after']=dict(timestamp=utc(),row=status(topics[2]))
            produce(topics[2],4000);wait_for(lambda:status(topics[2]).get('status')=='WARNING')
            produce(topics[2],5000);wait_for(lambda:status(topics[2]).get('status')=='CRITICAL')
            for attempt in range(8):
                cycle=metric(metrics(),'khm_collection_cycles_total','demo')
                wait_for(lambda:metric(metrics(),'khm_collection_cycles_total','demo')>cycle,seconds=30)
                cycle=metric(metrics(),'khm_collection_cycles_total','demo');started=utc()
                page.goto('http://127.0.0.1:8080/audit',wait_until='networkidle')
                page.evaluate('loadAuditLogs()')
                selected=audit_excerpt(page.evaluate('currentLogs'),topics[2],manifest['started_at'])
                screenshot('figure-audit.png')
                if metric(metrics(),'khm_collection_cycles_total','demo')==cycle:
                    write_json(output/'listing3-audit.json',selected)
                    manifest['files'].append(dict(figure='figure-audit.png',listing='listing3-audit.json',
                                                 started_at=started,ended_at=utc(),cycle=cycle))
                    break
            else: raise RuntimeError('Audit figure and export crossed collection cycles')
            started=utc();page.goto('http://127.0.0.1:8080/stats',wait_until='networkidle')
            page.wait_for_function('timelineChart !== null')
            screenshot('figure-statistics.png')
            for chart in manifest['render_checks']['figure-statistics.png']:
                for label in chart['labels']:
                    if not datetime.fromisoformat(manifest['started_at']) <= datetime.fromisoformat(label) <= datetime.fromisoformat(utc()):
                        raise ValueError('Statistics chart timestamp outside capture session')
            manifest['files'].append(dict(figure='figure-statistics.png',started_at=started,ended_at=utc()))
            if errors: raise RuntimeError('Browser errors: '+repr(errors))
            browser.close();browser=None
        if manifest.get('pump_error'): raise RuntimeError(manifest['pump_error'])
        finish_capture(output,manifest)
    except BaseException as exc:
        finish_capture(output,manifest,exc);raise
    finally:
        pump_stop.set()
        for c in consumers:
            if c.thread.is_alive(): c.close()
        if browser: browser.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--commit',required=True)
    parser.add_argument('--rehearsal',action='store_true',help='Validation-only run of the working tree; never paper evidence')
    args=parser.parse_args();run(args.commit,args.rehearsal)
