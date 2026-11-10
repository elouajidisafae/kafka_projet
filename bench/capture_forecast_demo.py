"""Owner-run Fig. 2 capture: established history 12-16 min; fast scenario 5-10 min.

One consumer (10 msg/s), three partitions. Established producer: 15 msg/s,
minimum selected fit history 10 min. Fast producer: 60 msg/s. Illustrative
capture only; no benchmark results/data are read or changed. Output: artifacts/.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import threading
import time

import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = {
    'established': dict(produce=15, minimum_history_seconds=600, timeout_seconds=1200),
    'fast': dict(produce=60, minimum_history_seconds=0, timeout_seconds=180),
}


def capture_ready(fitted, advice, topic, scenario):
    return (fitted.get('enough_data') and fitted.get('eta_critical_low_sec') is not None
            and fitted.get('eta_critical_high_sec') is not None
            and fitted.get('window_seconds', 0) >= SCENARIOS[scenario]['minimum_history_seconds']
            and any(r['topic']==topic and r['id'].endswith('-scale') for r in advice['recommendations']))


def command(*args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True, timeout=1200)


def api(path):
    from urllib.request import urlopen
    with urlopen('http://127.0.0.1:8080'+path, timeout=10) as response:
        return json.load(response)


def capture(method, scenario="established"):
    settings=SCENARIOS[scenario]
    if scenario == 'established':
        import sys
        if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
        from core.range_calibration import measured_coverage
        if not measured_coverage():
            raise ValueError('Complete final confirmation evaluation before the final established Fig. 2 capture')
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output=ROOT/'artifacts'/('forecast-demo-'+stamp)
    output.mkdir(parents=True,exist_ok=False)
    topic='s1-bottleneck-'+stamp
    config=yaml.safe_load((ROOT/'config.yml').read_text(encoding='utf-8'))
    config.update(clusters=[dict(name='demo',bootstrap_servers='kafka:29092')],include_groups=[topic])
    config['alerts']=dict(warning_threshold=1000,critical_threshold=10000)
    config['monitor']=dict(config.get('monitor',{}),group_topic_match=True,refresh_interval=5)
    config['forecast']=dict(config.get('forecast',{}),method=method,show_range=True)
    config_path=output/'config.yml'
    config_path.write_text(yaml.safe_dump(config),encoding='utf-8')
    data=output/'data';data.mkdir()
    env=dict(os.environ,KHM_DEMO_CONFIG=str(config_path),KHM_DEMO_DATA=str(data))
    compose=['docker','compose','-f','docker-compose.yml']
    command(*compose,'stop','producer-demo','consumer-normal','consumer-slow',env=env)
    command(*compose,'up','-d','--build','--wait','kafka-health-monitor',env=env)
    admin=AdminClient({'bootstrap.servers':'127.0.0.1:9092'})
    futures=admin.create_topics([NewTopic(topic,3,1)])
    for future in futures.values(): future.result(30)
    stop=threading.Event();ready=threading.Event();errors=[]

    def consume():
        consumer=Consumer({'bootstrap.servers':'127.0.0.1:9092','group.id':topic,
                           'enable.auto.commit':False,'enable.auto.offset.store':False,
                           'auto.offset.reset':'earliest'})
        def assign(c,parts):
            for part in parts: part.offset=0
            c.assign(parts)
            c.commit(offsets=[TopicPartition(p.topic,p.partition,0) for p in parts],asynchronous=False)
            ready.set()
        try:
            consumer.subscribe([topic],on_assign=assign)
            while not stop.is_set():
                started=time.monotonic()
                messages=consumer.consume(num_messages=10,timeout=.2)
                offsets={}
                for message in messages:
                    if message.error(): raise RuntimeError(str(message.error()))
                    offsets[message.partition()]=message.offset()+1
                if offsets:
                    consumer.commit(offsets=[TopicPartition(topic,p,o) for p,o in offsets.items()],asynchronous=False)
                stop.wait(max(0,1-(time.monotonic()-started)))
        except Exception as exc:
            errors.append(repr(exc));ready.set()
        finally:
            consumer.close()

    def produce():
        producer=Producer({'bootstrap.servers':'127.0.0.1:9092'})
        try:
            while not stop.is_set():
                started=time.monotonic()
                for i in range(settings["produce"]): producer.produce(topic,partition=i%3,value=b'fig2')
                if producer.flush(10): raise RuntimeError('Undelivered demo messages')
                stop.wait(max(0,1-(time.monotonic()-started)))
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            producer.flush(10)

    threads=[threading.Thread(target=consume,daemon=True),threading.Thread(target=produce,daemon=True)]
    manifest=dict(kind='illustrative_live_bottleneck',method=method,benchmark_evidence=False,
                  topic=topic,partitions=3,members=1,produce_per_second=settings['produce'],consume_limit_per_second=10,
                  scenario=scenario,minimum_history_seconds=settings['minimum_history_seconds'],
                  commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                  started_at=datetime.now(timezone.utc).isoformat(),status='running')
    try:
        threads[0].start()
        if not ready.wait(60) or errors: raise RuntimeError('Consumer assignment failed: '+repr(errors))
        threads[1].start()
        deadline=time.monotonic()+settings["timeout_seconds"]
        while True:
            if errors: raise RuntimeError(repr(errors))
            forecasts=api('/api/forecast')
            fitted=next((f for f in forecasts['forecasts'] if f.get('topic')==topic),{})
            advice=api('/api/recommendations')
            if capture_ready(fitted, advice, topic, scenario):
                break
            if time.monotonic()>deadline: raise TimeoutError('Bottleneck forecast/advice did not settle')
            time.sleep(1)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(channel='msedge',headless=True)
            context=browser.new_context(viewport=dict(width=1600,height=1100),device_scale_factor=2,
                                        locale='en-GB',timezone_id='UTC',color_scheme='light')
            context.add_init_script("localStorage.setItem('khm-theme','light')")
            page=context.new_page();page_errors=[]
            page.on('pageerror',lambda error:page_errors.append(str(error)))
            page.goto('http://127.0.0.1:8080',wait_until='networkidle')
            page.evaluate('async () => { await refresh(); await refreshForecast(); }')
            page.wait_for_function('displayedForecasts.some(f => f.enough_data)')
            page.evaluate('loadForecastChart(displayedForecasts.findIndex(f => f.enough_data))')
            page.wait_for_function('forecastChart !== null')
            page.evaluate('''async () => {
              await document.fonts.ready;
              for (const chart of Object.values(Chart.instances)) {
                chart.stop(); chart.options.animation=false; chart.update('none');
              }
              await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
            }''')
            # inner_text reflects the dashboard's uppercase header styling.
            assert 'fit quality' in page.locator('#forecast-table').inner_text().casefold()
            assert 'Point estimate:' in page.locator('#forecast-table').inner_text()
            assert page.evaluate("forecastChart.config.plugins.some(p => p.id === 'criticalCrossingRange')")
            assert page.locator('.forecast-identity strong').first.get_attribute('title')
            assert page.locator('.forecast-row').first.evaluate("e => getComputedStyle(e).display") == 'grid'
            # DOM text can exist outside the screenshot: verify actual bounds too.
            assert page.locator('#forecast-table').evaluate('''table => {
              const right = table.getBoundingClientRect().right;
              const cells = table.querySelectorAll('.forecast-header > span:last-child, .forecast-row > span:last-child');
              return table.scrollWidth <= table.clientWidth && [...cells].every(cell =>
                cell.getBoundingClientRect().right <= right && cell.getBoundingClientRect().right <= innerWidth);
            }'''), 'Fit quality must be visible without horizontal scrolling'
            shown=page.evaluate('displayedForecasts')
            displayed=next(f for f in shown if f.get('topic')==topic)
            assert displayed.get('window_seconds',0) >= settings['minimum_history_seconds']
            assert displayed.get('eta_critical_low_sec') is not None
            (output/'forecast-display.json').write_text(json.dumps(shown,indent=2),encoding='utf-8')
            values={k:displayed.get(k) for k in (
                'current_lag','slope','slope_per_min','eta_critical_sec','eta_critical_min',
                'eta_critical_low_sec','eta_critical_high_sec','confidence','r_squared',
                'window_seconds','range_tooltip')}
            values['advice']=page.locator('#recommendations-container').inner_text()
            values['origin']=displayed.get('display',{}).get('origin')
            (output/'displayed-values.json').write_text(json.dumps(values,indent=2),encoding='utf-8')
            page.screenshot(path=str(output/'figure-forecast.png'),full_page=True,animations='disabled')
            if page_errors: raise RuntimeError(repr(page_errors))
            browser.close()
        for name,value in [('forecast.json',forecasts),('recommendations.json',advice)]:
            (output/name).write_text(json.dumps(value,indent=2),encoding='utf-8')
        manifest['status']='complete'
        print('Capture saved:',output)
    except BaseException as exc:
        manifest.update(status='failed',error=repr(exc));raise
    finally:
        stop.set()
        for thread in threads:
            if thread.is_alive(): thread.join(30)
        manifest['ended_at']=datetime.now(timezone.utc).isoformat()
        (output/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--method',choices=['baseline','multiwindow'],default='multiwindow',help='Forecast mode (default: responsive multiwindow)')
    parser.add_argument('--scenario',choices=SCENARIOS,default='established',help='Established history (default) or the old fast scenario')
    args=parser.parse_args()
    capture(args.method,args.scenario)
