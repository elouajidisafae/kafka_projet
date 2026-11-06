"""Health scores normalised by the maximum attainable penalty (75).

Fixed weights: critical 60, warning 25, empty/dead 15. Critical and
warning statuses are mutually exclusive. Counts refer to group-topic pairs.
"""


def _measured(results):
    return [r for r in results if r.status not in {"ERROR", "UNKNOWN"}
            and not getattr(r, "stale", False)
            and not (getattr(r, "partition_count", 0) and getattr(r, "partitions_counted", None) == 0)]


def _components(results):
    results = _measured(results)
    total = len(results)
    critical = sum(r.status == "CRITICAL" for r in results)
    warning = sum(r.status == "WARNING" for r in results)
    empty = sum(str(getattr(r, "group_state", "")).upper() in {"EMPTY", "DEAD"} for r in results)
    penalties = (60 * critical / total, 25 * warning / total,
                 15 * empty / total) if total else (0, 0, 0)
    score = max(0, min(100, round(100 * (1 - sum(penalties) / 75))))
    return critical, warning, empty, penalties, score


def _compute_single_score(results: list) -> int:
    return _components(results)[-1]


def _grade(score: int) -> tuple[str, str]:
    """Retourne le grade et la couleur associés au score."""
    if score >= 90: return "Excellent", "#22c55e"
    if score >= 70: return "Bon",       "#4ade80"
    if score >= 50: return "Moyen",     "#facc15"
    if score >= 30: return "Mauvais",   "#fb923c"
    return             "Critique",  "#f87171"


def _empty_score() -> dict:
    return {
        "score": 100, "grade": "Aucune donnee", "color": "#64748b",
        "total_groups": 0, "n_critical": 0, "n_warning": 0,
        "n_ok": 0, "n_empty": 0,
        "details": {
            "penalty_critical": 0, "penalty_warning": 0, "penalty_state": 0,
            "pct_critical": 0.0, "pct_warning": 0.0,
        },
        "by_cluster": [],
    }


def _indicators(results, cluster_status):
    fresh = [r for r in results if not getattr(r, "stale", False)
             and cluster_status.get(r.cluster_name, {}).get("reachable", True)]
    measured = _measured(fresh)
    stopped = [r for r in measured if str(getattr(r, "group_state", "")).upper() in {"EMPTY", "DEAD"}
               and getattr(r, "total_lag", 0) > 0]
    return {
        "performance": {"critical": sum(r.status == "CRITICAL" for r in measured),
                        "warning": sum(r.status == "WARNING" for r in measured)},
        "availability": {"stopped_pairs": len(stopped),
                         "waiting_messages": sum(r.total_lag for r in stopped)},
        "monitoring": {"unreachable_clusters": sum(not s.get("reachable", True) for s in cluster_status.values()),
                       "error_pairs": sum(r.status == "ERROR" for r in fresh),
                       "unknown_pairs": sum(r.status == "UNKNOWN" for r in fresh)},
    }


def _summary(results, cluster_status):
    measured = _measured([r for r in results if cluster_status.get(r.cluster_name, {}).get("reachable", True)])
    total = len(measured)
    critical, warning, empty, penalties, score = _components(measured)
    grade, color = _grade(score)
    return {
        "score": score, "grade": grade, "color": color,
        "total_groups": total, "total": total,
        "n_critical": critical, "n_warning": warning,
        "n_ok": sum(r.status == "OK" for r in measured), "n_empty": empty,
        "details": {
            "penalty_critical": penalties[0], "penalty_warning": penalties[1], "penalty_state": penalties[2],
            "pct_critical": round(100 * critical / total, 1) if total else 0.0,
            "pct_warning": round(100 * warning / total, 1) if total else 0.0,
        },
        **_indicators(results, cluster_status),
    }


def compute_health_score(results: list, cluster_status: dict | None = None) -> dict:
    """Measured-pair score plus independent performance, availability and monitoring counts."""
    cluster_status = cluster_status or {}
    names = list(dict.fromkeys([r.cluster_name for r in results] + list(cluster_status)))
    summary = _summary(results, cluster_status)
    summary["by_cluster"] = [dict(_summary([r for r in results if r.cluster_name == name],
                                          {name: cluster_status[name]} if name in cluster_status else {}),
                                   cluster_name=name) for name in names]
    return summary
