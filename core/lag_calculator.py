from time import perf_counter
from .timing import record_collection
"""
Calcul du lag — supporte plusieurs clusters.
Chaque ConsumerGroupStatus porte maintenant un cluster_name.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from core.kafka_client import describe_groups, consumer_count_for_topic
from core.db import update_group_state_streak
from .config_loader import CONFIG


@dataclass
class PartitionLag:
    partition_id: int
    log_end_offset: int
    committed_offset: int
    lag: int


@dataclass
class ConsumerGroupStatus:
    cluster_name: str
    group_id: str
    topic: str
    partitions: list[PartitionLag] = field(default_factory=list)
    total_lag: int = 0
    status: str = "OK"
    group_state: str = "UNKNOWN"
    consumer_count: int | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    partition_count: int | None = None
    partitions_counted: int | None = None

    def __post_init__(self):
        if self.partition_count is None:
            self.partition_count = len(self.partitions)
        if self.partitions_counted is None:
            self.partitions_counted = len(self.partitions)
        self.total_lag = sum(p.lag for p in self.partitions)
        self.status = _compute_status(self.total_lag)


def _compute_status(total_lag: int) -> str:
    warn = CONFIG["alerts"]["warning_threshold"]
    crit = CONFIG["alerts"]["critical_threshold"]
    if total_lag >= crit:
        return "CRITICAL"
    elif total_lag >= warn:
        return "WARNING"
    return "OK"


def _make_admin(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def _make_consumer(bootstrap_servers: str, group_id: str = "_khm_probe") -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
    })


def get_topics(bootstrap_servers: str) -> list[str]:
    admin = _make_admin(bootstrap_servers)
    metadata = admin.list_topics(timeout=10)
    exclude = set(CONFIG.get("exclude_topics", []))
    return sorted([
        name for name in metadata.topics.keys()
        if not name.startswith("_") and name not in exclude
    ])


def get_consumer_groups(bootstrap_servers: str) -> list[str]:
    admin = _make_admin(bootstrap_servers)
    result = admin.list_consumer_groups()
    exclude = set(CONFIG.get("exclude_groups", []))
    exclude.add("_khm_probe")
    return sorted([
        g.group_id for g in result.result().valid
        if g.group_id not in exclude
    ])


def compute_lag_for_group(
    cluster_name: str,
    bootstrap_servers: str,
    group_id: str,
    topic: str
) -> ConsumerGroupStatus:
    try:
        from confluent_kafka import TopicPartition

        # Étape 1 : LEO via consumer probe
        probe = _make_consumer(bootstrap_servers, "_khm_probe_leo")
        try:
            metadata = probe.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions
            topic_partitions = [TopicPartition(topic, pid) for pid in partitions.keys()]

            end_offsets = {}
            for tp in topic_partitions:
                _, high = probe.get_watermark_offsets(tp, timeout=5)
                end_offsets[tp.partition] = high
        finally:
            probe.close()

        # Étape 2 : Committed offsets via un consumer avec le VRAI group_id
        # On crée un consumer avec le group_id exact du groupe à monitorer
        # enable.auto.commit=False pour ne pas perturber les offsets
        committed_consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        })
        try:
            tps = [TopicPartition(topic, pid) for pid in end_offsets.keys()]
            committed = committed_consumer.committed(tps, timeout=10)
            committed_offsets = {
                tp.partition: tp.offset
                for tp in committed
                if tp.offset >= 0
            }
        finally:
            committed_consumer.close()

        # Étape 3 : Calcul lag = LEO - committed
        parts = sorted([
            PartitionLag(
                partition_id=pid,
                log_end_offset=leo,
                committed_offset=committed_offsets.get(pid, 0),
                lag=max(0, leo - committed_offsets.get(pid, 0)),
            )
            for pid, leo in end_offsets.items()
            if pid in committed_offsets
        ], key=lambda p: p.partition_id)

        return ConsumerGroupStatus(
            cluster_name=cluster_name,
            group_id=group_id,
            topic=topic,
            partitions=parts,
            partition_count=len(end_offsets),
            partitions_counted=len(parts),
        )

    except Exception as e:
        print(f"[lag_calculator] Erreur {group_id}/{topic}: {e}")
        return ConsumerGroupStatus(
            cluster_name=cluster_name,
            group_id=group_id,
            topic=topic,
            status="ERROR",
            total_lag=-1,
        )
    
def compute_all_lags() -> list[ConsumerGroupStatus]:
    """
    Calcule le lag pour TOUS les consumer groups sur TOUS les topics.
    Inclut maintenant l'état du groupe (STABLE, EMPTY, DEAD...).
    """
    results = []
    severity_order = {"CRITICAL": 0, "WARNING": 1, "OK": 2, "ERROR": 3}

    for cluster in CONFIG["clusters"]:
        started = perf_counter()
        first_pair = len(results)
        name    = cluster["name"]
        servers = cluster["bootstrap_servers"]
        print(f"[monitor] Cluster '{name}' → {servers}")

        try:
            descriptions = describe_groups(name)
            groups = list(descriptions)
            topics = get_topics(servers)
            print(f"[monitor]   groups={groups} topics={topics}")
        except Exception as e:
            print(f"[monitor]   Connection error: {e}")
            record_collection(name, perf_counter() - started, 0)
            continue

        # Récupère tous les états en un seul appel Admin (optimisé)
        for group in groups:
            desc = descriptions[group]
            state = desc.state
            update_group_state_streak(name, group, state)
            for topic in topics:
                result = compute_lag_for_group(name, servers, group, topic)
                result.group_state = state
                result.consumer_count = consumer_count_for_topic(desc, topic)
                results.append(result)
                print(f"[monitor]   {name}/{group}/{topic} → lag={result.total_lag} {result.status} [{state}]")

        record_collection(name, perf_counter() - started, len(results) - first_pair)

    results.sort(key=lambda r: (severity_order.get(r.status, 9), -r.total_lag))
    return results