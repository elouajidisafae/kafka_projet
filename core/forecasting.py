"""
Predictive Lag Forecasting — régression linéaire sur l'historique SQLite.
"""
import numpy as np
from .db import get_lag_history, save_forecast, parse_iso_utc
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
    warn = CONFIG["alerts"]["warning_threshold"]
    crit = CONFIG["alerts"]["critical_threshold"]
    if window_hours is None:
        window_hours = CONFIG.get("forecast", {}).get("window_hours", FORECAST_WINDOW_HOURS)
    records = get_lag_history(cluster_name, group_id, topic, last_hours=window_hours)

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


def forecast_all() -> list[dict]:
    from .db import get_latest_per_group
    rows = get_latest_per_group()

    results = []
    seen = set()
    for row in rows:
        key = (row["cluster_name"], row["group_id"], row["topic"])
        if key in seen:
            continue
        seen.add(key)

        forecast = forecast_lag(row["cluster_name"], row["group_id"], row["topic"])
        if CONFIG.get("forecast", {}).get("persist_every_cycle", False):
            save_forecast(forecast)
        results.append(forecast)

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

    results.sort(key=sort_key)
    return results
