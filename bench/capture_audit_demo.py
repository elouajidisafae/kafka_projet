"""Owner-run illustrative audit scenario; fresh artifacts only, about 3–8 minutes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.capture_health_demo import ROOT, IdleMember, api, command
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import yaml


def wait_for(test, seconds=120):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        if test():
            return
        time.sleep(1)
    raise TimeoutError("Audit scenario did not reach the required observed state")


def has_alert(logs, topic, severity, lag):
    return any(row['event_type']=='ALERT' and row['severity']==severity
               and topic in row['message'] and json.loads(row['details'] or '{}').get('lag')==lag
               for row in logs)


def run():
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output=ROOT/'artifacts'/('audit-demo-'+stamp)
    output.mkdir(parents=True,exist_ok=False)
    topic='audit-demo-'+stamp
    config=yaml.safe_load((ROOT/'config.yml').read_text(encoding='utf-8'))
    config.update(clusters=[dict(name='demo',bootstrap_servers='kafka:29092')],include_groups=[topic])
    config['monitor']=dict(config['monitor'],group_topic_match=True)
    config_path=output/'config.yml'
    config_path.write_text(yaml.safe_dump(config),encoding='utf-8')
    data=output/'data';data.mkdir()
    env=dict(os.environ,KHM_DEMO_CONFIG=str(config_path),KHM_DEMO_DATA=str(data))
    compose=['docker','compose','-f','docker-compose.yml']
    manifest=dict(commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                  benchmark_evidence=False,topic=topic,status='running',scenario='Section 3.3')
    member=IdleMember(topic)
    thread=threading.Thread(target=member.run,daemon=True)
    try:
        command(*compose,'stop','producer-demo','consumer-normal','consumer-slow',env=env)
        command(*compose,'up','-d','--build','--wait','kafka-health-monitor',env=env)
        admin=AdminClient({'bootstrap.servers':'localhost:9092'})
        for future in admin.create_topics([NewTopic(topic,1,1)]).values(): future.result(30)
        thread.start()
        if not member.ready.wait(60) or member.error: raise RuntimeError('Consumer assignment failed')
        producer=Producer({'bootstrap.servers':'localhost:9092'})
        def add(count):
            for _ in range(count): producer.produce(topic,partition=0,value=b'audit-demo')
            if producer.flush(30): raise RuntimeError('Undelivered messages')
        def status(lag, state):
            return any(r['topic']==topic and r['total_lag']==lag and r['status']==state
                       for r in api('/api/status')['data'])
        add(1500)
        wait_for(lambda: has_alert(api('/api/audit'),topic,'WARNING',1500))
        body=dict(alerts=dict(warning_threshold=5000,critical_threshold=10000))
        request=Request('http://127.0.0.1:8080/api/config',data=json.dumps(body).encode(),
                        headers={'Content-Type':'application/json'},method='POST')
        with urlopen(request,timeout=10) as response:
            if not json.load(response).get('success'): raise RuntimeError('Threshold update failed')
        wait_for(lambda: status(1500,'OK'))
        add(4400)
        wait_for(lambda: has_alert(api('/api/audit'),topic,'WARNING',5900))
        add(5000)
        wait_for(lambda: has_alert(api('/api/audit'),topic,'CRITICAL',10900))
        logs=api('/api/audit')
        assert any(r['event_type']=='CONFIG_CHANGE' and
                   json.loads(r['details'])==body for r in logs)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(channel='msedge',headless=True)
            context=browser.new_context(viewport=dict(width=1600,height=1100),device_scale_factor=2,
                                        locale='en-GB',timezone_id='UTC',color_scheme='light')
            context.add_init_script("localStorage.setItem('khm-theme','light')")
            page=context.new_page()
            page.goto('http://127.0.0.1:8080/audit',wait_until='networkidle')
            page.evaluate('loadAuditLogs()')
            page.wait_for_function("document.getElementById('audit-feed').textContent.includes('10900') && document.getElementById('audit-feed').textContent.includes('5900') && document.getElementById('audit-feed').textContent.includes('5000')")
            page.evaluate('document.fonts.ready')
            page.screenshot(path=str(output/'figure-audit.png'),full_page=True,animations='disabled')
            browser.close()
        for name,value in [('audit.json',logs),('status.json',api('/api/status')),('config-change.json',body)]:
            (output/name).write_text(json.dumps(value,indent=2),encoding='utf-8')
        manifest['status']='complete'
        print('Capture saved:',output)
    except BaseException as exc:
        manifest.update(status='failed',error=repr(exc))
        raise
    finally:
        member.stop.set()
        if thread.is_alive(): thread.join(30)
        (output/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')


if __name__=='__main__':
    argparse.ArgumentParser(description=__doc__).parse_args()
    run()
