from types import SimpleNamespace as NS
from unittest.mock import MagicMock
import pytest
from core import kafka_client as kc


def future(value):
    result = MagicMock()
    result.result.return_value = value
    return result


def member(name, assignments):
    return NS(member_id=name, client_id=name, host="host", assignment=None if assignments is None else
              NS(topic_partitions=[NS(topic=t, partition=p) for t, ids in assignments.items() for p in ids]))


def setup_admin(monkeypatch, members, state="ConsumerGroupState.STABLE"):
    admin = MagicMock()
    admin.list_consumer_groups.return_value = future(NS(valid=[NS(group_id="g")]))
    admin.describe_consumer_groups.return_value = {"g": future(NS(state=state, members=members))}
    monkeypatch.setattr(kc, "AdminClient", lambda conf: admin)
    monkeypatch.setattr(kc, "CONFIG", {"clusters": [{"name": "dev", "bootstrap_servers": "kafka:29092"}]})
    return admin


def test_topic_membership_not_total_members(monkeypatch):
    setup_admin(monkeypatch, [member("a", {"orders": [0, 1], "logs": [0]}),
                             member("b", {"logs": [1], "clicks": [0]}), member("idle", {})])
    desc = kc.describe_groups("dev")["g"]
    assert desc.members[0].assigned_partitions == {"orders": [0, 1], "logs": [0]}
    assert [kc.consumer_count_for_topic(desc, t) for t in ["orders", "logs", "clicks", "absent"]] == [1, 2, 1, 0]


@pytest.mark.parametrize("state,expected", [("Stable", "STABLE"), ("ConsumerGroupState.PreparingRebalance", "PREPARING_REBALANCE"),
                                           ("COMPLETING_REBALANCE", "COMPLETING_REBALANCE"), ("Empty", "EMPTY"), ("Dead", "DEAD")])
def test_normalization(state, expected):
    assert kc.normalize_group_state(state) == expected


@pytest.mark.parametrize("state", ["PREPARING_REBALANCE", "COMPLETING_REBALANCE", "UNKNOWN"])
def test_unreliable_state_count(state):
    desc = kc.GroupDescription("g", state, [kc.GroupMember("m", "c", "h", {"orders": [0]})])
    assert kc.consumer_count_for_topic(desc, "orders") is None


@pytest.mark.parametrize("state", ["EMPTY", "DEAD"])
def test_empty_and_dead_are_zero(state):
    assert kc.consumer_count_for_topic(kc.GroupDescription("g", state, []), "orders") == 0


def test_missing_assignment_is_unknown(monkeypatch):
    setup_admin(monkeypatch, [member("a", {"orders": [0]}), member("b", None)])
    assert kc.consumer_count_for_topic(kc.describe_groups("dev")["g"], "orders") is None


@pytest.mark.parametrize("failure", ["call", "future", "missing"])
def test_describe_failure_is_unknown(monkeypatch, failure):
    admin = setup_admin(monkeypatch, [])
    if failure == "call":
        admin.describe_consumer_groups.side_effect = RuntimeError("offline")
    elif failure == "future":
        admin.describe_consumer_groups.return_value["g"].result.side_effect = RuntimeError("offline")
    else:
        admin.describe_consumer_groups.return_value = {}
    desc = kc.describe_groups("dev")["g"]
    assert desc.state == "UNKNOWN"
    assert kc.consumer_count_for_topic(desc, "orders") is None


def test_wrapper_preserves_signature(monkeypatch):
    setup_admin(monkeypatch, [], "ConsumerGroupState.Empty")
    assert kc.get_all_group_states("kafka:29092", ["g"]) == {"g": "EMPTY"}
    assert kc.get_all_group_states("kafka:29092", []) == {}


@pytest.mark.parametrize("enum_name,expected", [("PREPARING_REBALANCING", "PREPARING_REBALANCE"),
                                               ("COMPLETING_REBALANCING", "COMPLETING_REBALANCE")])
def test_installed_sdk_rebalance_enum(monkeypatch, enum_name, expected):
    from confluent_kafka import ConsumerGroupState
    state = getattr(ConsumerGroupState, enum_name)
    setup_admin(monkeypatch, [member("a", {"orders": [0]})], state)
    desc = kc.describe_groups("dev")["g"]
    assert desc.state == expected
    assert kc.consumer_count_for_topic(desc, "orders") is None
