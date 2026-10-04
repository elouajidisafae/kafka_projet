from unittest.mock import patch
import pytest
from core import recommender as rec


@pytest.fixture
def settings(monkeypatch):
    config = {"alerts": {"warning_threshold": 1000, "critical_threshold": 10000},
              "recommendations": {"max_per_pair": 2, "max_per_cluster": 10,
                                  "confidence_gate": ["HIGH", "MEDIUM"], "rebalance_cycles_threshold": 3}}
    monkeypatch.setattr(rec, "CONFIG", config)
    return config


def analyze(**changes):
    args = dict(cluster="dev", group_id="g", topic="orders", current_lag=2000,
                state="STABLE", trend="INCREASING", consumer_count=1, partition_count=3,
                forecast=dict(confidence="HIGH", slope_per_min=60.0, r_squared=0.95,
                              eta_critical_sec=120, eta_critical_min=2.0), rebalance_cycles=0)
    args.update(changes)
    return rec.analyze_row(**args)


def suffixes(items):
    return [r["id"].rsplit("-", 1)[-1] for r in items]


@pytest.mark.parametrize("confidence,priority", [("HIGH", "HIGH"), ("MEDIUM", "MEDIUM"), ("LOW", None)])
def test_scale_confidence(settings, confidence, priority):
    result = analyze(forecast=dict(confidence=confidence, eta_critical_sec=1, eta_critical_min=0.0,
                                  slope_per_min=60.0, r_squared=0.7))
    assert suffixes(result) == (["scale"] if priority else [])
    if result:
        assert result[0]["priority"] == priority
        assert result[0]["metrics"]["consumer_count"] == 1


@pytest.mark.parametrize("eta", [-2, -1, 0, None])
def test_scale_eta_boundary(settings, eta):
    assert analyze(forecast=dict(confidence="HIGH", eta_critical_sec=eta)) == []


@pytest.mark.parametrize("count,expected", [(0, "scale"), (2, "scale"), (3, "topology"), (4, "topology")])
def test_topology_mutually_exclusive(settings, count, expected):
    assert suffixes(analyze(consumer_count=count)) == [expected]


@pytest.mark.parametrize("consumer,partitions", [(None, 3), (1, None), (None, None), (0, 0)])
def test_unknown_topology_suppresses_advice(settings, consumer, partitions):
    assert analyze(consumer_count=consumer, partition_count=partitions) == []


@pytest.mark.parametrize("trend", ["STABLE", "DECREASING", None])
def test_no_growth_no_capacity_advice(settings, trend):
    assert analyze(trend=trend) == []


def test_topology_gate_and_eta_independence(settings):
    assert analyze(consumer_count=3, forecast={"confidence": "LOW"}) == []
    assert suffixes(analyze(consumer_count=3, forecast={"confidence": "HIGH", "eta_critical_sec": -1})) == ["topology"]
    settings["recommendations"]["confidence_gate"] = ["HIGH"]
    assert analyze(forecast={"confidence": "MEDIUM", "eta_critical_sec": 120}) == []


@pytest.mark.parametrize("state", ["EMPTY", "DEAD"])
def test_stranded_boundary(settings, state):
    assert suffixes(analyze(state=state, current_lag=1001, trend=None)) == ["stranded"]
    assert analyze(state=state, current_lag=1000, trend=None) == []
    assert analyze(state="STABLE", current_lag=1001, trend=None) == []


def test_stalled_boundary(settings):
    result = analyze(current_lag=10000, trend="STABLE", forecast={"slope_per_min": 0.0})
    assert suffixes(result) == ["stalled"]
    assert result[0]["type"] == "PERFORMANCE"
    assert analyze(current_lag=10000 - 1, trend="STABLE") == []
    assert analyze(current_lag=10000, trend="DECREASING") == []
    assert analyze(current_lag=10000, trend="STABLE", state="UNKNOWN") == []


@pytest.mark.parametrize("state", ["PREPARING_REBALANCE", "COMPLETING_REBALANCE"])
def test_rebalance_boundary(settings, state):
    assert analyze(state=state, consumer_count=None, rebalance_cycles=2) == []
    result = analyze(state=state, consumer_count=None, rebalance_cycles=3)
    assert suffixes(result) == ["rebalance"]
    assert result[0]["metrics"]["rebalance_cycles"] == 3
    settings["recommendations"]["rebalance_cycles_threshold"] = 4
    assert analyze(state=state, consumer_count=None, rebalance_cycles=3) == []


def test_multiple_matches_and_pair_order(settings):
    assert suffixes(analyze(state="EMPTY", consumer_count=0)) == ["stranded"]
    settings["recommendations"]["max_per_pair"] = 1
    result = analyze(state="EMPTY", consumer_count=0, forecast={"confidence": "MEDIUM", "eta_critical_sec": 120})
    assert suffixes(result) == ["stranded"]


def test_three_synthetic_matches_cap(settings):
    # Real rule predicates permit at most two simultaneous matches.
    items = [{"id": "dev-g-orders-topology", "priority": "MEDIUM"},
             {"id": "dev-g-orders-rebalance", "priority": "MEDIUM"},
             {"id": "dev-g-orders-stranded", "priority": "HIGH"}]
    assert suffixes(rec._cap_pair(items)) == ["stranded", "topology"]


def test_per_cluster_cap_never_drops_high(settings, monkeypatch):
    rows = [dict(cluster_name=c, group_id=f"g{i:02}", topic="orders", total_lag=2000,
                 group_state="EMPTY", consumer_count=0, partition_count=3)
            for c in ["dev", "prod"] for i in range(15)]
    monkeypatch.setattr(rec, "get_latest_per_group", lambda: rows)
    monkeypatch.setattr(rec, "forecasts_for_request", lambda: {})
    monkeypatch.setattr(rec, "get_group_state_streaks", lambda: {})
    result = rec.get_recommendations()
    assert len(result) == 30
    assert all(r["priority"] == "HIGH" for r in result)
    assert [r["cluster_name"] for r in result] == ["dev"] * 15 + ["prod"] * 15


def test_medium_cap_is_per_cluster(settings, monkeypatch):
    rows = [dict(cluster_name=c, group_id=f"g{i:02}", topic="orders", total_lag=2000,
                 group_state="STABLE", consumer_count=3, partition_count=3)
            for c in ["prod", "dev"] for i in reversed(range(15))]
    monkeypatch.setattr(rec, "get_latest_per_group", lambda: rows)
    monkeypatch.setattr(rec, "forecasts_for_request", lambda: {
        (r["cluster_name"], r["group_id"], r["topic"]): {"trend": "INCREASING", "confidence": "HIGH"} for r in rows})
    monkeypatch.setattr(rec, "get_group_state_streaks", lambda: {})
    result = rec.get_recommendations()
    assert len(result) == 20
    assert [r["group_id"] for r in result[:10]] == [f"g{i:02}" for i in range(10)]
    assert all(r["cluster_name"] == "dev" for r in result[:10])


def test_return_contract_and_exact_text(settings):
    result = analyze()[0]
    assert set(result) == {"id", "cluster_name", "group_id", "topic", "type", "priority", "title", "advice", "action", "metrics"}
    assert result["title"] == "Scale consumer group"
    assert result["advice"] == "Lag is rising at 60.0 msg/min. CRITICAL threshold projected in ~2.0 min (R2=0.95)."
    assert result["action"] == "Increase consumer instances (current: 1, partitions: 3)."
    variants = [analyze(), analyze(consumer_count=3), analyze(state="EMPTY", trend=None),
                analyze(current_lag=10000, trend="STABLE"),
                analyze(state="PREPARING_REBALANCE", consumer_count=None, rebalance_cycles=3)]
    for items in variants:
        assert items
        for item in items:
            assert item["metrics"]
            assert "severity" not in item and "cluster" not in item
