from types import SimpleNamespace

import pytest

from core.health_score import compute_health_score


def row(status, state="STABLE", cluster="demo"):
    return SimpleNamespace(status=status, group_state=state, cluster_name=cluster,
                           total_lag=12000 if status == "CRITICAL" else 1500 if status == "WARNING" else 0)


@pytest.mark.parametrize("state", ["EMPTY", "DEAD", "Empty", "Dead", "empty", "dead"])
def test_worst_case_reaches_zero_with_case_normalisation(state):
    result = compute_health_score([row("CRITICAL", state)])
    assert result["score"] == 0
    assert result["n_empty"] == 1
    assert result["details"]["penalty_state"] == 15
    assert result["by_cluster"][0]["score"] == 0


def test_global_and_cluster_scores_use_the_same_normalisation():
    result = compute_health_score([row("OK"), row("CRITICAL", "EMPTY", "other")])
    assert result["score"] == round(100 * (1 - (30 + 7.5) / 75))
    assert [c["score"] for c in result["by_cluster"]] == [100, 0]


def test_all_ok_and_no_data():
    assert compute_health_score([row("OK")])["score"] == 100
    empty = compute_health_score([])
    assert empty["total_groups"] == 0 and empty["by_cluster"] == []
    assert empty["grade"] == empty["score_band"] == "No data"
    assert empty["score"] is None


def test_requested_intermediate_scores():
    assert compute_health_score([row("CRITICAL")])["score"] == 20
    result = compute_health_score([row("OK", "EMPTY")] + [row("OK") for _ in range(3)])
    assert result["score"] == 100
    assert result["by_cluster"][0]["score"] == result["score"]


from core.lag_calculator import ConsumerGroupStatus, PartitionLag


def pair(status="OK", state="STABLE", cluster="demo", lag=0, group="payments"):
    # Explicit lag drives the real pair classification.
    return ConsumerGroupStatus(cluster, group, "orders", [PartitionLag(0, lag, 0, lag)], group_state=state)


def test_critical_stopped_pair_cannot_hide_among_one_hundred():
    rows = [pair(group=f"g{i}") for i in range(99)] + [pair(state="EMPTY", lag=12000)]
    result = compute_health_score(rows)
    assert result["score"] == round(100 * (1 - (.6 + .15) / 75)) == 99
    assert result["details"]["penalty_critical"] == .6
    assert result["details"]["penalty_state"] == .15
    assert result["grade"] == "Critical" and result["score_band"] == "Excellent"
    assert result["availability"] == {"stopped_pairs": 1, "waiting_messages": 12000}
    assert result["performance"] == {"critical": 1, "warning": 0}
    assert result["worst_state"] == 2 and result["color"] == "#f87171"
    assert any("payments/orders" in reason for reason in result["ceiling_reasons"])
    assert result["by_cluster"][0]["grade"] == "Critical"


def test_unreachable_cluster_is_counted_without_using_its_stale_lag():
    result = compute_health_score([pair(cluster="live"), pair(cluster="offline", state="EMPTY", lag=12000)],
                                  {"live": {"reachable": True}, "offline": {"reachable": False, "error": "offline"}})
    assert result["score"] == 100 and result["total_groups"] == 1
    assert result["grade"] == "Degraded"
    assert result["monitoring"]["unreachable_clusters"] == 1
    assert result["performance"]["critical"] == 0
    assert result["availability"]["stopped_pairs"] == 0
    clusters = {c["cluster_name"]: c for c in result["by_cluster"]}
    assert clusters["live"]["grade"] == "Excellent"
    assert clusters["offline"]["grade"] == "Degraded"
    assert clusters["offline"]["monitoring"]["unreachable_clusters"] == 1


def test_unknown_error_and_stale_excluded_from_number_but_monitoring_counted():
    unknown = ConsumerGroupStatus("demo", "new", "orders", partition_count=3, partitions_counted=0)
    assert unknown.status == "UNKNOWN"
    error = ConsumerGroupStatus("demo", "broken", "orders", status="ERROR")
    stale = pair(state="EMPTY", lag=12000)
    stale.stale = True
    result = compute_health_score([pair(lag=1500), unknown, error, stale])
    assert result["total_groups"] == 1
    assert result["score"] == 67
    assert result["monitoring"] == {"unreachable_clusters": 0, "error_pairs": 1, "unknown_pairs": 1}
    assert result["grade"] == "Degraded"
    assert result["by_cluster"][0]["monitoring"] == result["monitoring"]


@pytest.mark.parametrize("rows", [[], [pair()], [pair(state="EMPTY")]])
def test_no_active_alerts_uses_score_band(rows):
    result = compute_health_score(rows)
    assert result["grade"] == result["score_band"]
    assert result["active_alert_count"] == 0 and result["ceiling"] is None
    assert result["worst_state"] == 0


def test_ceiling_never_improves_a_worse_score_band():
    result = compute_health_score([pair(state="DEAD", lag=1500)])
    assert result["grade"] == "Critical"
    assert result["score"] == 47
    # A warning on a stopped group with no known waiting messages is not availability.
    r = row("WARNING", "EMPTY")
    r.total_lag = 0
    result = compute_health_score([r])
    assert result["score_band"] == result["grade"] == "Degraded"
    assert result["ceiling"] == "Degraded"



def test_all_clusters_unreachable_have_no_numeric_score():
    result = compute_health_score([], {"a": {"reachable": False}, "b": {"reachable": False}})
    assert result["score"] is None and result["grade"] == "Degraded"
    assert result["monitoring"]["unreachable_clusters"] == 2
    assert all(c["score"] is None and c["grade"] == "Degraded" for c in result["by_cluster"])


@pytest.mark.parametrize("status", ["ERROR", "UNKNOWN"])
def test_all_unmeasured_pairs_have_no_score(status):
    result = compute_health_score([row(status)])
    assert result["score"] is None and result["grade"] == "Degraded"



def test_empty_caught_up_group_is_not_penalised():
    result = compute_health_score([pair(group=f"g{i}") for i in range(9)] + [pair(state="EMPTY")])
    assert result["score"] == 100 and result["grade"] == "Excellent"
    assert result["details"]["penalty_state"] == 0
    assert result["availability"]["stopped_pairs"] == result["n_empty"] == 0



def test_paper_scenario_counts_problem_pairs_not_conditions():
    result = compute_health_score([pair(state="EMPTY", lag=12000), pair(group="warning", lag=1500), pair(group="healthy")])
    assert result["score"] == 56 and result["grade"] == "Critical"
    assert result["active_alert_count"] == len(result["active_alerts"]) == 2
    critical = next(item for item in result["active_alerts"] if item["group_id"] == "payments")
    assert len(critical["reasons"]) == 2
    assert len(result["ceiling_reasons"]) == 2
    assert "CRITICAL" in critical["message"] and "waiting messages" in critical["message"]
