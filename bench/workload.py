"""One isolated producer/consumer repetition with seeded arrival jitter."""
import argparse
import json
import random
import sys
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.common import utc


def rates(pattern, settings, seconds):
    production, consumption = settings["produce"], settings["consume"]
    if pattern == "burst" and settings["burst_at"] <= seconds < settings["burst_at"]+settings["burst_seconds"]:
        production = settings["burst_rate"]
    if pattern == "sawtooth" and seconds % settings["period_seconds"] < settings["pause_seconds"]:
        consumption = 0
    if pattern == "flat_high" and seconds < settings["fill_seconds"]:
        production = settings["fill_rate"]
    return production, consumption


class CommitTracker:
    """Commit processed offsets immediately; keep idle heartbeat commits at 0.5 s."""
    def __init__(self, consumer, topic, offsets, clock=time.monotonic):
        self.consumer, self.topic, self.clock = consumer, topic, clock
        self.offsets = dict(offsets)
        self.last_commit = clock()
        self.max_gap = 0.0
        self.max_latency = 0.0

    def flush(self, offsets, force=False):
        from confluent_kafka import TopicPartition
        started = self.clock()
        if not force and offsets == self.offsets and started-self.last_commit < .5:
            return
        result = self.consumer.commit(
            offsets=[TopicPartition(self.topic,p,o) for p,o in offsets.items()],
            asynchronous=False)
        if any(partition.error is not None for partition in result):
            raise RuntimeError("Broker rejected a workload offset commit")
        completed = self.clock()
        self.max_gap = max(self.max_gap, completed-self.last_commit)
        self.max_latency = max(self.max_latency, completed-started)
        self.last_commit = completed
        self.offsets = dict(offsets)


def run(spec, stop_file):
    from confluent_kafka import Consumer, Producer, TopicPartition
    rng = random.Random(spec["seed"])
    topic = spec["topic"]
    producer = Producer({"bootstrap.servers": spec["bootstrap"], "enable.idempotence": True, "linger.ms": 0})
    consumer = Consumer({"bootstrap.servers": spec["bootstrap"], "group.id": spec["group_id"],
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "auto.offset.reset": "earliest"})
    offsets = {p: 0 for p in range(3)}
    assigned = [False]
    def on_assign(c, partitions):
        if assigned[0]:
            raise RuntimeError("Unexpected reassignment in isolated repetition")
        for tp in partitions:
            tp.offset = 0
        c.assign(partitions)
        c.commit(offsets=[TopicPartition(topic,p,0) for p in offsets], asynchronous=False)
        assigned[0] = True
    consumer.subscribe([topic], on_assign=on_assign)
    deadline = time.monotonic()+60
    while not assigned[0]:
        consumer.poll(0.1)
        if time.monotonic()>deadline:
            raise TimeoutError("Consumer assignment timeout")
    produced = [0]
    def delivered(error, message):
        if error:
            raise RuntimeError(str(error))
        produced[0] += 1
    consumed = 0
    pending = deque()
    commits = CommitTracker(consumer, topic, offsets)
    start = last = last_log = time.monotonic()
    next_jitter = 0
    jitter = 1.0
    produce_credit = consume_credit = expected_produced = expected_consumed = 0.0
    path = Path(spec["log"])
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            def log(elapsed, final=False):
                row = dict(timestamp=utc(), elapsed=elapsed, produced=produced[0], consumed=consumed,
                    committed=sum(commits.offsets.values()), lag=produced[0]-consumed,
                    expected_produced=expected_produced, expected_consumed=expected_consumed,
                    max_commit_gap_seconds=commits.max_gap, max_commit_latency_seconds=commits.max_latency,
                    uncommitted_processed=consumed-sum(commits.offsets.values()), final=final)
                handle.write(json.dumps(row)+"\n")
                handle.flush()
            handle.write(json.dumps(dict(kind="metadata", **{k:v for k,v in spec.items() if k not in {"log","bootstrap"}}, started_at=utc()))+"\n")
            log(0)
            while time.monotonic()-start < spec["duration_seconds"] and not Path(stop_file).exists():
                now = time.monotonic()
                elapsed, dt = now-start, now-last
                last = now
                if elapsed >= next_jitter:
                    jitter = rng.uniform(.9,1.1)
                    next_jitter = int(elapsed)+1
                arrival, service = rates(spec["pattern"],spec["rates"],elapsed)
                expected_produced += arrival*dt
                expected_consumed += service*dt
                produce_credit += arrival*jitter*dt
                # Do not accumulate service capacity while paused or starved.
                consume_credit = min(consume_credit+service*dt, max(1,service)) if service else 0
                while produce_credit >= 1:
                    producer.produce(topic, b"khm", partition=int(produced[0]+int(produce_credit))%3, on_delivery=delivered)
                    produce_credit -= 1
                producer.poll(0)
                if consume_credit >= 1:
                    if not pending:
                        pending.extend(consumer.consume(num_messages=min(100, max(1,int(consume_credit))), timeout=.01))
                    while pending and consume_credit >= 1:
                        message = pending.popleft()
                        if message.error():
                            raise RuntimeError(str(message.error()))
                        offsets[message.partition()] = message.offset()+1
                        consumed += 1
                        consume_credit -= 1
                commits.flush(offsets)
                if now-last_log >= 1:
                    # Read broker offsets, not just the locally requested commit.
                    actual = consumer.committed([TopicPartition(topic,p) for p in offsets], timeout=5)
                    if any(tp.offset != commits.offsets[tp.partition] for tp in actual):
                        raise AssertionError("Broker committed offset differs from processed count")
                    log(elapsed)
                    last_log = now
                time.sleep(.005)
            if producer.flush(10):
                raise RuntimeError("Undelivered messages at shutdown")
            commits.flush(offsets, force=True)
            log(time.monotonic()-start, final=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--stop-file", required=True)
    args = parser.parse_args()
    run(json.loads(args.spec.read_text()), args.stop_file)
