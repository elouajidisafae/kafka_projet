"""
Predictive Lag Forecasting — régression linéaire sur l'historique SQLite.
"""
from copy import deepcopy
from threading import RLock
from time import monotonic, perf_counter
import json
import numpy as np
from .db import get_lag_history, get_lag_history_bulk, save_forecast, parse_iso_utc
from .config_loader import CONFIG

MIN_POINTS = 5
FORECAST_WINDOW_HOURS = 1


def _parse_timestamp(ts: str) -> float:
    """Convertit un timestamp ISO en secondes depuis epoch."""
    return parse_iso_utc(ts).timestamp()


def forecast_lag(
    cluster_name: str,
    group_id: str,
    topic: str,
    window_hours: float | None = None,
) -> dict:
    if window_hours is None:
        window_hours = CONFIG.get("forecast", {}).get("window_hours", FORECAST_WINDOW_HOURS)
    records = get_lag_history(cluster_name, group_id, topic, last_hours=window_hours)
    return fit_forecast(records, cluster_name, group_id, topic)


def fit_forecast(records: list[dict], cluster_name: str, group_id: str, topic: str) -> dict:
    """Fit the configured forecast without database or other I/O."""
    warn = CONFIG["alerts"]["warning_threshold"]
    crit = CONFIG["alerts"]["critical_threshold"]

    if records:
        timestamps = np.array([_parse_timestamp(r["recorded_at"]) for r in records])
        if np.any(np.diff(timestamps) < 0):
            raise ValueError("Forecast timestamps are not non-decreasing.")

    if len(records) < MIN_POINTS:
        return {
            "enough_data": False,
            "reason": f"Only {len(records)} points (minimum {MIN_POINTS})",
            "current_lag": records[-1]["total_lag"] if records else 0,
        }

    timestamps = np.array([_parse_timestamp(r["recorded_at"]) for r in records])
    lags = np.array([r["total_lag"] for r in records])

    t0 = timestamps[0]
    x = timestamps - t0
    y = lags

    coeffs = np.polyfit(x, y, deg=1)
    slope, intercept = coeffs[0], coeffs[1]

    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    if r_squared >= 0.85:
        confidence = "HIGH"
    elif r_squared >= 0.5:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    deadband = float(CONFIG.get("forecast", {}).get("trend_deadband_msgs_per_sec", 0.05))
    if slope > deadband:
        trend = "INCREASING"
    elif slope < -deadband:
        trend = "DECREASING"
    else:
        trend = "STABLE"

    current_lag = int(lags[-1])
    now_offset = timestamps[-1] - t0

    def eta_seconds(threshold: int) -> int:
        if current_lag >= threshold:
            return -1
        if slope <= 0:
            return -2
        t_reach = (threshold - intercept) / slope
        seconds_from_now = t_reach - now_offset
        return max(0, int(seconds_from_now))

    eta_warn = eta_seconds(warn)
    eta_crit = eta_seconds(crit)
    t_5min = now_offset + 5 * 60
    t_15min = now_offset + 15 * 60
    pred_5min = max(0, int(slope * t_5min + intercept))
    pred_15min = max(0, int(slope * t_15min + intercept))

    result = {
        "enough_data": True,
        "cluster_name": cluster_name,
        "group_id": group_id,
        "topic": topic,
        "current_lag": current_lag,
        "slope": round(float(slope), 4),
        "slope_per_min": round(float(slope * 60), 1),
        "intercept": round(float(intercept), 4),
        "trend": trend,
        "confidence": confidence,
        "r_squared": round(float(r_squared), 3),
        "eta_warning_sec": eta_warn,
        "eta_critical_sec": eta_crit,
        "eta_warning_min": round(eta_warn / 60, 1) if eta_warn >= 0 else eta_warn,
        "eta_critical_min": round(eta_crit / 60, 1) if eta_crit >= 0 else eta_crit,
        "predicted_lag_5min": pred_5min,
        "predicted_lag_15min": pred_15min,
        "n_points": len(records),
        "window_seconds": int((timestamps[-1] - timestamps[0]).item()),
        "warning_threshold": warn,
        "critical_threshold": crit,
    }
    return result


def _sort_forecasts(results):
    def sort_key(f):
        if not f.get("enough_data"):
            return (3, 0)
        if f["trend"] == "INCREASING":
            eta = f["eta_critical_sec"]
            return (0, eta if eta >= 0 else float("inf"))
        elif f["trend"] == "STABLE":
            return (1, 0)
        else:
            return (2, 0)

    return sorted(results, key=sort_key)


_cache_lock = RLock()
_cache = None
_cache_time = 0.0
_cache_signature = None
_cache_inputs = {}


def _signature():
    from . import db
    return (str(db.DB_PATH), json.dumps(CONFIG, sort_keys=True))


def _compute(cluster_name=None):
    from .db import get_latest_per_group
    from .timing import record_forecast
    started = perf_counter()
    rows = get_latest_per_group(cluster_name)
    history = get_lag_history_bulk(cluster_name, CONFIG.get("forecast", {}).get("window_hours", 1))
    fetch_duration = perf_counter() - started
    forecasts = {}
    inputs = {}
    durations = {}
    counts = {}
    for row in rows:
        key = (row["cluster_name"], row["group_id"], row["topic"])
        if key in forecasts:
            continue
        started = perf_counter()
        records = history.get(key, [])
        forecasts[key] = fit_forecast(records, *key)
        inputs[key] = [r["id"] for r in records] if CONFIG.get("forecast", {}).get("record_inputs", False) else None
        durations[key[0]] = durations.get(key[0], 0.0) + perf_counter() - started
        counts[key[0]] = counts.get(key[0], 0) + 1
    for cluster in CONFIG.get("clusters", []):
        if cluster_name is None or cluster["name"] == cluster_name:
            counts.setdefault(cluster["name"], 0)
            durations.setdefault(cluster["name"], 0.0)
    for cluster, count in counts.items():
        # Shared bulk-read time is apportioned by pair count, not counted twice.
        record_forecast(cluster, durations[cluster] + fetch_duration * count / max(1, len(forecasts)), count)
    return forecasts, inputs


def compute_cycle_forecasts(cluster_name: str | None = None) -> dict:
    """Publish a complete cycle atomically; scoped computations never replace it."""
    global _cache, _cache_time, _cache_signature, _cache_inputs
    with _cache_lock:
        values, inputs = _compute(cluster_name)
        if cluster_name is None:
            _cache = values
            _cache_inputs = inputs
            _cache_time = monotonic()
            _cache_signature = _signature()
        return deepcopy(values)


def get_cached_forecasts(max_age_seconds: float | None = None) -> dict | None:
    with _cache_lock:
        age = CONFIG.get("monitor", {}).get("refresh_interval", 5) if max_age_seconds is None else max_age_seconds
        if _cache is None or _cache_signature != _signature() or monotonic() - _cache_time >= age:
            return None
        return deepcopy(_cache)


def forecasts_for_request() -> dict:
    # Single-flight cache misses: another request cannot fit the same cycle concurrently.
    with _cache_lock:
        values = get_cached_forecasts()
        return compute_cycle_forecasts() if values is None else values


def _persist(values):
    if CONFIG.get("forecast", {}).get("persist_every_cycle", False):
        for forecast in values:
            key = (forecast.get("cluster_name"), forecast.get("group_id"), forecast.get("topic"))
            ids = _cache_inputs.get(key)
            if ids is None:
                save_forecast(forecast)
            else:
                save_forecast(forecast, input_ids=ids)


def forecast_all() -> list[dict]:
    with _cache_lock:
        values = compute_cycle_forecasts()
        _persist(values.values())
        return _sort_forecasts(values.values())


def cached_forecast_all() -> list[dict]:
    with _cache_lock:
        values = forecasts_for_request()
        _persist(values.values())
        return _sort_forecasts(values.values())
