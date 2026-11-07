"""Capture the revised three-pair Health Score demo using real Kafka offsets.

This is an illustrative demo, not a benchmark. Outputs go only under artifacts/.
Run by the owner: Docker build/startup can exceed five minutes.
"""
import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from urllib.request import urlopen

import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]


def command(*args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True, timeout=1200)


def api(path):
    with urlopen("http://127.0.0.1:8080" + path, timeout=10) as response:
        return json.load(response)


@dataclass
class IdleMember:
    topic: str
    ready: threading.Event = field(default_factory=threading.Event)
    stop: threading.Event = field(default_factory=threading.Event)
    error: Exception | None = None

    def run(self):
        consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": self.topic,
                             "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        def assign(c, parts):
            c.assign(parts)
            c.commit(offsets=[TopicPartition(p.topic, p.partition, 0) for p in parts], asynchronous=False)
            c.pause(parts)
            self.ready.set()
        try:
            consumer.subscribe([self.topic], on_assign=assign)
            while not self.stop.is_set():
                consumer.poll(.1)
        except Exception as exc:
            self.error = exc
            self.ready.set()
        finally:
            consumer.close()


def run():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = ROOT / "artifacts" / ("health-demo-" + stamp)
    output.mkdir(parents=True)
    topics = ["health-critical-" + stamp, "health-warning-" + stamp, "health-ok-" + stamp]
    config = yaml.safe_load((ROOT / "config.yml").read_text(encoding="utf-8"))
    config.update(clusters=[{"name": "demo", "bootstrap_servers": "kafka:29092"}], include_groups=topics)
    config["alerts"] = {"warning_threshold": 1000, "critical_threshold": 10000}
    config["monitor"] = dict(config.get("monitor", {}), group_topic_match=True, refresh_interval=5)
    config_path = output / "config.yml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    data = output / "data"
    data.mkdir()
    env = dict(os.environ, KHM_DEMO_CONFIG=str(config_path), KHM_DEMO_DATA=str(data))
    compose = ["docker", "compose", "-f", "docker-compose.yml"]
    # Uses the existing demo stack and port 8080; stop its background workload.
    command(*compose, "stop", "producer-demo", "consumer-normal", "consumer-slow", env=env)
    command(*compose, "up", "-d", "--build", "--wait", "kafka-health-monitor", env=env)
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    for future in admin.create_topics([NewTopic(t, 1, 1) for t in topics]).values():
        future.result(30)
    members = [IdleMember(t) for t in topics]
    threads = [threading.Thread(target=m.run, daemon=True) for m in members]
    try:
        for thread in threads:
            thread.start()
        for member in members:
            if not member.ready.wait(60):
                raise RuntimeError("Group assignment timed out: " + member.topic)
            if member.error:
                raise member.error
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        for topic, count in zip(topics, [12000, 1500, 0]):
            for _ in range(count):
                producer.produce(topic, partition=0, value=b"health-demo")
        if producer.flush(30):
            raise RuntimeError("Messages were not delivered")
        members[0].stop.set()
        threads[0].join(30)
        deadline = time.monotonic() + 120
        while True:
            health = api("/api/health-score")
            status = api("/api/status")
            if (health.get("score") == 56 and health.get("grade") == "Critical"
                    and health.get("availability", {}).get("stopped_pairs") == 1
                    and health.get("active_alert_count") == 2
                    and sorted(r["status"] for r in status["data"]) == ["CRITICAL", "OK", "WARNING"]):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Demo did not settle: " + json.dumps(health))
            time.sleep(1)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel="msedge", headless=True)
            context = browser.new_context(viewport={"width": 1440, "height": 1100}, device_scale_factor=2,
                                          locale="en-GB", timezone_id="UTC", color_scheme="light")
            context.add_init_script("localStorage.setItem('khm-theme', 'light')")
            page = context.new_page()
            page.goto("http://127.0.0.1:8080", wait_until="networkidle")
            page.evaluate("async () => { await refresh(); await refreshHealthScore(); }")
            page.wait_for_function("document.getElementById('score-value').textContent === '56' && document.getElementById('score-grade').textContent === 'Critical'")
            page.evaluate("document.fonts.ready")
            page.screenshot(path=str(output / "figure-overview.png"), full_page=True, animations="disabled")
            page.click("#active-alert-total")
            page.screenshot(path=str(output / "figure-active-alerts.png"), full_page=True, animations="disabled")
            browser.close()
        (output / "health-score.json").write_text(json.dumps(health, indent=2), encoding="utf-8")
        (output / "status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
        manifest = {"kind": "illustrative_live_health_demo", "commit": commit, "dirty": bool(dirty),
                    "captured_at": datetime.now(timezone.utc).isoformat(), "topics": topics,
                    "expected_score": 56, "expected_grade": "Critical", "benchmark_evidence": False}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print("Capture saved:", output)
    finally:
        for member in members:
            member.stop.set()
        for thread in threads:
            if thread.is_alive():
                thread.join(30)


if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__).parse_args()
    run()
