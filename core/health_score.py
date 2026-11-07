"""Health scores normalised by the maximum attainable penalty (75).

Fixed weights: critical 60, warning 25, stopped with waiting messages 15.
The state penalty counts only EMPTY/DEAD pairs with measured lag above zero,
exactly like the availability count. Critical and
warning statuses are mutually exclusive. Counts refer to group-topic pairs.
"""


def _stopped(row):
    return str(getattr(row, "group_state", "")).upper() in {"EMPTY", "DEAD"} and getattr(row, "total_lag", 0) > 0


def _measured(results):
    return [r for r in results if r.status not in {"ERROR", "UNKNOWN"}
            and not getattr(r, "stale", False)
            and not (getattr(r, "partition_count", 0) and getattr(r, "partitions_counted", None) == 0)]


def _components(results):
    results = _measured(results)
    total = len(results)
    critical = sum(r.status == "CRITICAL" for r in results)
    warning = sum(r.status == "WARNING" for r in results)
    empty = sum(_stopped(r) for r in results)
    penalties = (60 * critical / total, 25 * warning / total,
                 15 * empty / total) if total else (0, 0, 0)
    score = max(0, min(100, round(100 * (1 - sum(penalties) / 75)))) if total else None
    return critical, warning, empty, penalties, score


def _compute_single_score(results: list) -> int:
    return _components(results)[-1]


def _grade(score: int | None) -> tuple[str, str]:
    """Retourne le grade et la couleur associés au score."""
    if score is None: return "No data", "#64748b"
    if score >= 90: return "Excellent", "#22c55e"
    if score >= 70: return "Good",       "#4ade80"
    if score >= 50: return "Degraded",     "#facc15"
    if score >= 30: return "Poor",   "#fb923c"
    return             "Critical",  "#f87171"


def _indicators(results, cluster_status):
    fresh = [r for r in results if not getattr(r, "stale", False)
             and cluster_status.get(r.cluster_name, {}).get("reachable", True)]
    measured = _measured(fresh)
    stopped = [r for r in measured if _stopped(r)]
    return {
        "performance": {"critical": sum(r.status == "CRITICAL" for r in measured),
                        "warning": sum(r.status == "WARNING" for r in measured)},
        "availability": {"stopped_pairs": len(stopped),
                         "waiting_messages": sum(r.total_lag for r in stopped)},
        "monitoring": {"unreachable_clusters": sum(not s.get("reachable", True) for s in cluster_status.values()),
                       "error_pairs": sum(r.status == "ERROR" for r in fresh),
                       "unknown_pairs": sum(r.status == "UNKNOWN" for r in fresh)},
    }


def _active_alerts(results, cluster_status):
    alerts = []
    for name, state in cluster_status.items():
        if not state.get("reachable", True):
            alerts.append({"kind": "unreachable_cluster", "severity": 1, "cluster_name": name,
                           "message": f"Unreachable cluster: {name}", "error": state.get("error"),
                           "last_successful_collection": state.get("last_successful_collection")})
    for row in results:
        if getattr(row, "stale", False) or not cluster_status.get(row.cluster_name, {}).get("reachable", True):
            continue
        target = f"{row.cluster_name}/{getattr(row, 'group_id', '?')}/{getattr(row, 'topic', '?')}"
        details = {"cluster_name": row.cluster_name, "group_id": getattr(row, "group_id", None),
                   "topic": getattr(row, "topic", None), "lag": getattr(row, "total_lag", None),
                   "group_state": getattr(row, "group_state", None)}
        if row.status in {"CRITICAL", "WARNING", "ERROR", "UNKNOWN"}:
            alerts.append(dict(details, kind=row.status.lower(), severity=2 if row.status == "CRITICAL" else 1,
                               message=f"{row.status} pair: {target}"))
        if row in _measured([row]) and _stopped(row):
            alerts.append(dict(details, kind="stopped_pair", severity=2,
                               message=f"1 stopped group with waiting messages: {target} ({row.total_lag} messages)"))
    return alerts


def _apply_label(summary, results, cluster_status):
    alerts = _active_alerts(results, cluster_status)
    worst = max((a["severity"] for a in alerts), default=0)
    band, _ = _grade(summary["score"])
    labels = ["Critical", "Poor", "Degraded", "Good", "Excellent"]
    ceiling = "Critical" if worst == 2 else "Degraded" if worst == 1 else None
    final = ceiling if band == "No data" and ceiling else labels[min(labels.index(band), labels.index(ceiling))] if ceiling else band
    colors = ["#f87171", "#fb923c", "#facc15", "#4ade80", "#22c55e"]
    summary.update(score_band=band, grade=final, color=colors[labels.index(final)] if final in labels else "#64748b",
                   ceiling=ceiling, worst_state=worst,
                   ceiling_reasons=[a["message"] for a in alerts if a["severity"] == worst],
                   active_alerts=alerts, active_alert_count=len(alerts))
    return summary


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
    summary = _apply_label(_summary(results, cluster_status), results, cluster_status)
    summary["by_cluster"] = [dict(_apply_label(_summary([r for r in results if r.cluster_name == name],
                                          {name: cluster_status[name]} if name in cluster_status else {}),
                                          [r for r in results if r.cluster_name == name],
                                          {name: cluster_status[name]} if name in cluster_status else {}),
                                   cluster_name=name) for name in names]
    return summary
