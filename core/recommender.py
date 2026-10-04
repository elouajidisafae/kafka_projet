"""Operational recommendations from lag trends, topology, and persisted group state."""
from collections import Counter

from .config_loader import CONFIG
from .forecasting import forecasts_for_request
from .db import get_latest_per_group, get_group_state_streaks

PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
RULE_ORDER = {name: i for i, name in enumerate(("scale", "topology", "stranded", "stalled", "rebalance"))}


def _cap_pair(recommendations: list[dict]) -> list[dict]:
    recommendations.sort(key=lambda r: (PRIORITY_ORDER[r["priority"]],
                                        RULE_ORDER[r["id"].rsplit("-", 1)[-1]]))
    return recommendations[:CONFIG.get("recommendations", {}).get("max_per_pair", 2)]


def get_recommendations(*, persist: bool = False) -> list[dict]:
    recommendations = []
    all_matches = []
    forecasts = forecasts_for_request()
    streaks = get_group_state_streaks()
    for row in get_latest_per_group():
        cluster, group, topic = row["cluster_name"], row["group_id"], row["topic"]
        forecast = forecasts.get((cluster, group, topic), {})
        streak = streaks.get((cluster, group))
        cycles = streak["streak_count"] if streak and streak["state"] == row["group_state"] else 0
        matches = analyze_row(
            cluster, group, topic, row["total_lag"], row["group_state"],
            forecast.get("trend"), forecast, consumer_count=row.get("consumer_count"),
            partition_count=row.get("partition_count"), rebalance_cycles=cycles, apply_cap=False)
        all_matches.extend(matches)
        recommendations.extend(_cap_pair(matches))

    recommendations.sort(key=lambda r: (PRIORITY_ORDER[r["priority"]], r["cluster_name"],
                                        r["group_id"], r["topic"]))
    counts = Counter()
    result = []
    cap = CONFIG.get("recommendations", {}).get("max_per_cluster", 10)
    for rec in recommendations:
        cluster = rec["cluster_name"]
        if rec["priority"] == "HIGH" or counts[cluster] < cap:
            result.append(rec)
            counts[cluster] += 1
    if persist:
        from .db import save_recommendations
        save_recommendations(all_matches, {r["id"] for r in result})
    return result


def analyze_row(cluster, group_id, topic, current_lag, state, trend, forecast,
                *, consumer_count=None, partition_count=None, rebalance_cycles=0, apply_cap=True) -> list[dict]:
    """Evaluate every rule, retaining the highest-priority matches for this pair."""
    settings = CONFIG.get("recommendations", {})
    confidence = forecast.get("confidence")
    gate = confidence in {"HIGH", "MEDIUM"} and confidence in settings.get("confidence_gate", ["HIGH", "MEDIUM"])
    gate = gate and forecast.get("advice_eligible", True)
    slope_per_min = forecast.get("slope_per_min")
    r_squared = forecast.get("r_squared")
    eta_critical_min = forecast.get("eta_critical_min")
    topology_known = consumer_count is not None and partition_count is not None and partition_count > 0
    values = dict(current_lag=current_lag, slope_per_min=slope_per_min, r_squared=r_squared,
                  confidence=confidence, eta_critical_min=eta_critical_min,
                  consumer_count=consumer_count, partition_count=partition_count,
                  group_state=state, rebalance_cycles=rebalance_cycles)
    result = []

    def emit(suffix, kind, priority, title, advice, action, keys):
        result.append(dict(id=f"{cluster}-{group_id}-{topic}-{suffix}", cluster_name=cluster,
                           group_id=group_id, topic=topic, type=kind, priority=priority,
                           title=title, advice=advice, action=action,
                           metrics={key: values[key] for key in keys}))

    interval_text = f"R2={r_squared}"
    if forecast.get("method") == "multiwindow" and CONFIG.get("forecast", {}).get("show_range", False):
        low, high = forecast.get("eta_low_sec"), forecast.get("eta_high_sec")
        interval_text = ("range unavailable" if low is None else
                         f"range {low / 60:.1f} min or later" if high is None else
                         f"range {low / 60:.1f}–{high / 60:.1f} min")

    if (trend == "INCREASING" and gate and state not in {"EMPTY", "DEAD"}
            and topology_known and consumer_count < partition_count
            and (forecast.get("eta_critical_sec") or 0) > 0):
        emit("scale", "PERFORMANCE", confidence, "Scale consumer group",
             f"Lag is rising at {slope_per_min} msg/min. CRITICAL threshold projected in ~{eta_critical_min} min ({interval_text}).",
             f"Increase consumer instances (current: {consumer_count}, partitions: {partition_count}).",
             ("slope_per_min", "r_squared", "confidence", "eta_critical_min", "consumer_count", "partition_count"))

    if trend == "INCREASING" and gate and topology_known and consumer_count >= partition_count:
        emit("topology", "CAPACITY", "MEDIUM", "Partition count may be limiting",
             f"Lag is rising at {slope_per_min} msg/min while all {partition_count} partitions are already assigned to {consumer_count} consumers.",
             f"Review partition count for topic '{topic}' before adding consumers.",
             ("slope_per_min", "confidence", "consumer_count", "partition_count"))

    if state in {"EMPTY", "DEAD"} and current_lag > CONFIG["alerts"]["warning_threshold"]:
        emit("stranded", "AVAILABILITY", "HIGH", "Stranded messages",
             f"{current_lag} messages pending. Group state: {state}. These messages will not self-resolve.",
             f"Restart the consumer for group '{group_id}'.", ("current_lag", "group_state"))

    if current_lag >= CONFIG["alerts"]["critical_threshold"] and trend == "STABLE" and state == "STABLE":
        emit("stalled", "PERFORMANCE", "HIGH", "Possible processing bottleneck",
             f"Lag is {current_lag} messages and not decreasing (slope {slope_per_min} msg/min).",
             "Check consumer throughput and processing errors.", ("current_lag", "slope_per_min", "group_state"))

    if (state in {"PREPARING_REBALANCE", "COMPLETING_REBALANCE"}
            and rebalance_cycles >= settings.get("rebalance_cycles_threshold", 3)):
        emit("rebalance", "STABILITY", "MEDIUM", "Group rebalance not completing",
             f"Group has remained in {state} for {rebalance_cycles} consecutive collection cycles.",
             "Check consumer liveness and session timeout configuration.", ("group_state", "rebalance_cycles"))
    return _cap_pair(result) if apply_cap else result
