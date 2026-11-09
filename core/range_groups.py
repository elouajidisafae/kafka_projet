"""Live-observable grouping shared by offline calibration and runtime display."""
import math

HORIZONS = ("0-5", "5-15", "15-30")
QUALITIES = ("HIGH", "MEDIUM", "LOW")
HISTORIES = ("under-5", "5-30", "30+")


def horizon(seconds):
    if seconds is None or not math.isfinite(seconds) or seconds <= 0 or seconds > 1800:
        return None
    return "0-5" if seconds <= 300 else "5-15" if seconds <= 900 else "15-30"


def history_bin(seconds):
    if seconds is None or not math.isfinite(seconds) or seconds < 0:
        return None
    return "under-5" if seconds < 300 else "5-30" if seconds < 1800 else "30+"


def group_key(candidate, predicted, quality, window_seconds):
    label = horizon(predicted)
    if label is None:
        return None
    parts = [label]
    if candidate in {"C1", "C3"}:
        if quality not in QUALITIES:
            return None
        parts.append(quality)
    if candidate in {"C2", "C3"}:
        history = history_bin(window_seconds)
        if history is None:
            return None
        parts.append(history)
    if candidate not in {"C0", "C1", "C2", "C3"}:
        raise ValueError("Unknown calibration candidate")
    return "|".join(parts)


def factor_for(model, predicted, quality, window_seconds):
    label = horizon(predicted)
    if label is None:
        return None
    key = group_key(model["candidate"], predicted, quality, window_seconds)
    factor = model["groups"].get(key, {})
    # Sparse or unavailable live grouping fields fall back to horizon only.
    if model["candidate"] != "C0" and factor.get("n", 0) < 200:
        factor = model["horizon_factors"].get(label, {})
    return factor if factor.get("n", 0) else None
