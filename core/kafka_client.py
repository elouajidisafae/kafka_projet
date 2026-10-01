"""
Couche d'accès à Kafka.

Responsabilité unique : se connecter au cluster et lire les offsets
bruts (log-end-offset et committed-offset) pour chaque partition.
Le calcul du lag lui-même est fait dans lag_calculator.py.
"""
from dataclasses import dataclass
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from .config_loader import CONFIG
from .client_pool import client


def _make_admin() -> AdminClient:
    """Crée un client Admin Kafka (lecture des topics/partitions)."""
    return AdminClient({
        "bootstrap.servers": CONFIG["kafka"]["bootstrap_servers"]
    })


def _make_consumer(group_id: str = "_kafka_health_monitor_probe") -> Consumer:
    """
    Crée un Consumer temporaire utilisé uniquement pour lire les offsets.
    group_id spécial pour ne pas polluer les vrais consumer groups.
    """
    return Consumer({
        "bootstrap.servers": CONFIG["kafka"]["bootstrap_servers"],
        "group.id": group_id,
        "enable.auto.commit": False,   # lecture seule, on ne commit rien
        "auto.offset.reset": "latest",
    })


def get_topics() -> list[str]:
    """
    Retourne la liste de tous les topics du cluster,
    en excluant les topics internes (__consumer_offsets, etc.)
    et ceux listés dans exclude_topics du config.
    """
    admin = _make_admin()
    metadata = admin.list_topics(timeout=10)

    exclude = set(CONFIG.get("exclude_topics", []))

    topics = [
        name
        for name in metadata.topics.keys()
        if not name.startswith("_")   # topics internes Kafka
        and name not in exclude
    ]
    return sorted(topics)


def get_consumer_groups() -> list[str]:
    """
    Retourne tous les consumer groups actifs ou inactifs.
    Exclut le groupe sonde du monitor lui-même.
    """
    admin = _make_admin()
    groups_result = admin.list_consumer_groups()

    exclude = set(CONFIG.get("exclude_groups", []))
    exclude.add("_kafka_health_monitor_probe")

    groups = []
    for group in groups_result.result().valid:
        if group.group_id not in exclude:
            groups.append(group.group_id)

    return sorted(groups)


def get_log_end_offsets(topic: str) -> dict[int, int]:
    """
    Retourne le Log-End-Offset (dernier message produit) pour chaque
    partition d'un topic.

    Le LEO représente la position de la "tête" du topic — c'est-à-dire
    combien de messages ont été écrits au total dans cette partition.

    Retourne : {partition_id: log_end_offset}
    """
    consumer = _make_consumer()
    try:
        # Récupère les métadonnées du topic pour connaître les partitions
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = metadata.topics[topic].partitions

        from confluent_kafka import TopicPartition

        # Construit la liste des TopicPartition à interroger
        topic_partitions = [
            TopicPartition(topic, partition_id)
            for partition_id in partitions.keys()
        ]

        # query les offsets de fin (OFFSET_END = position du prochain message)
        # On utilise -1 (OFFSET_END) comme offset pour indiquer qu'on veut la fin
        end_offsets = {}
        for tp in topic_partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            # high = log-end-offset = nombre total de messages dans cette partition
            end_offsets[tp.partition] = high

        return end_offsets

    finally:
        consumer.close()


def get_committed_offsets(group_id: str, topic: str, bootstrap_servers: str) -> dict[int, int]:
    """
    Lit les committed offsets via AdminClient — plus fiable que Consumer.
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = client(AdminClient, {"bootstrap.servers": bootstrap_servers}, locals().get("cluster_name", ""))

    try:
        # Récupère les partitions du topic
        metadata = admin.list_topics(topic, timeout=10)
        partitions = metadata.topics[topic].partitions
        topic_partitions = [TopicPartition(topic, pid) for pid in partitions.keys()]

        # list_consumer_group_offsets lit les vrais committed offsets
        result = admin.list_consumer_group_offsets(
            [{"group.id": group_id, "partitions": topic_partitions}]
        )

        committed_offsets = {}
        for res in result:
            future = result[res]
            group_result = future.result()
            for tp, offset_info in group_result.topic_partitions.items():
                offset = offset_info.offset if offset_info.offset >= 0 else 0
                committed_offsets[tp.partition] = offset

        return committed_offsets

    except Exception as e:
        print(f"[kafka_client] Erreur committed offsets {group_id}/{topic}: {e}")
        return {}
    
    
def normalize_group_state(state) -> str:
    value = str(state).rsplit(".", 1)[-1].upper()
    return {
        "PREPARINGREBALANCE": "PREPARING_REBALANCE",
        "PREPARINGREBALANCING": "PREPARING_REBALANCE",
        "PREPARING_REBALANCING": "PREPARING_REBALANCE",
        "COMPLETINGREBALANCE": "COMPLETING_REBALANCE",
        "COMPLETINGREBALANCING": "COMPLETING_REBALANCE",
        "COMPLETING_REBALANCING": "COMPLETING_REBALANCE",
    }.get(value, value)


@dataclass
class GroupMember:
    member_id: str
    client_id: str
    host: str
    assigned_partitions: dict[str, list[int]]


@dataclass
class GroupDescription:
    group_id: str
    state: str
    members: list[GroupMember]
    assignments_available: bool = True


def describe_groups(cluster_name: str, *, bootstrap_servers: str | None = None,
                    group_ids: list[str] | None = None) -> dict[str, GroupDescription]:
    """Full group descriptions including member assignments."""
    descriptions = {}
    try:
        if bootstrap_servers is None:
            bootstrap_servers = next(c["bootstrap_servers"] for c in CONFIG["clusters"]
                                     if c["name"] == cluster_name)
        admin = client(AdminClient, {"bootstrap.servers": bootstrap_servers}, locals().get("cluster_name", ""))
        if group_ids is None:
            excluded = set(CONFIG.get("exclude_groups", [])) | {
                "_khm_probe", "_khm_probe_leo", "_kafka_health_monitor_probe"}
            group_ids = sorted(g.group_id for g in admin.list_consumer_groups().result().valid
                               if g.group_id not in excluded and (not CONFIG.get("include_groups") or g.group_id in CONFIG["include_groups"]))
        descriptions = {gid: GroupDescription(gid, "UNKNOWN", [], False) for gid in group_ids}
        if not group_ids:
            return descriptions
        futures = admin.describe_consumer_groups(group_ids)
        for gid in group_ids:
            try:
                info = futures[gid].result()
                members = []
                available = True
                for member in info.members:
                    assigned = {}
                    assignment = member.assignment
                    if assignment is None or assignment.topic_partitions is None:
                        available = False
                    else:
                        for tp in assignment.topic_partitions:
                            assigned.setdefault(tp.topic, []).append(tp.partition)
                    members.append(GroupMember(member.member_id, member.client_id,
                                               member.host, assigned))
                descriptions[gid] = GroupDescription(
                    gid, normalize_group_state(info.state), members, available)
            except Exception as exc:
                print(f"[kafka_client] Could not describe group {gid}: {exc}")
    except Exception as exc:
        print(f"[kafka_client] Could not describe groups for {cluster_name}: {exc}")
    return descriptions


def consumer_count_for_topic(desc: GroupDescription | None, topic: str) -> int | None:
    """Members holding >= 1 partition of topic. None when the count is unreliable."""
    if desc is None:
        return None
    state = normalize_group_state(desc.state)
    if state in {"EMPTY", "DEAD"}:
        return 0
    if state != "STABLE" or not desc.assignments_available:
        return None
    return sum(bool(member.assigned_partitions.get(topic)) for member in desc.members)


def get_all_group_states(bootstrap_servers: str, group_ids: list[str]) -> dict[str, str]:
    """
    Retourne l'état de plusieurs consumer groups en un seul appel Admin.
    Plus efficace que d'appeler get_consumer_group_state() en boucle.

    Retourne : {group_id: state_string}
    """
    descriptions = describe_groups("", bootstrap_servers=bootstrap_servers, group_ids=group_ids)
    return {gid: descriptions[gid].state if gid in descriptions else "UNKNOWN" for gid in group_ids}


def get_consumer_group_state(group_id: str, bootstrap_servers: str) -> str:
    """
    Retourne l'état actuel d'un consumer group.

    États possibles :
    - Stable          : consomme normalement
    - Empty           : groupe vide, aucun consumer connecté
    - Dead            : groupe supprimé ou inexistant
    - PreparingRebalance  : rebalancing en cours
    - CompletingRebalance : rebalancing en finalisation

    Utilise l'AdminClient Kafka pour lire les métadonnées du groupe.
    """
    return get_all_group_states(bootstrap_servers, [group_id])[group_id]
