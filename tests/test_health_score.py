from types import SimpleNamespace

import pytest

from core.health_score import compute_health_score


def row(status, state="STABLE", cluster="demo"):
    return SimpleNamespace(status=status, group_state=state, cluster_name=cluster)


@pytest.mark.parametrize("state", ["EMPTY", "DEAD", "Empty", "Dead", "empty", "dead"])
def test_worst_case_reaches_zero_with_case_normalisation(state):
    result = compute_health_score([row("CRITICAL", state)])
    assert result["score"] == 0
    assert result["n_empty"] == 1
    assert result["details"]["penalty_state"] == 15
    assert result["by_cluster"][0]["score"] == 0


def test_global_and_cluster_scores_use_the_same_normalisation():
    result = compute_health_score([row("OK"), row("CRITICAL", "EMPTY", "other")])
    assert result["score"] == round(100 * (1 - (30 + 8) / 75))
    assert [c["score"] for c in result["by_cluster"]] == [100, 0]


def test_all_ok_and_no_data():
    assert compute_health_score([row("OK")])["score"] == 100
    empty = compute_health_score([])
    assert empty["total_groups"] == 0 and empty["by_cluster"] == []
    assert empty["grade"] != compute_health_score([row("OK")])["grade"]


def test_requested_intermediate_scores():
    assert compute_health_score([row("CRITICAL")])["score"] == 20
    result = compute_health_score([row("OK", "EMPTY")] + [row("OK") for _ in range(3)])
    assert result["score"] == 95
    assert result["by_cluster"][0]["score"] == result["score"]
