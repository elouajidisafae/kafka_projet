"""Post-baseline analyses; phase boundaries come only from generator metadata."""
from bisect import bisect_right
from collections import defaultdict
import statistics

from bench.common import timestamp


def secondary_metrics(rep, history, forecasts, recommendations, metadata, warning, critical):
    # Imported here to avoid a module cycle with the evaluation entry point.
    from bench.forecast_eval import breach, horizon_bin
    median = lambda values: statistics.median(values) if values else None
    start = timestamp(metadata["started_at"])
    rates = metadata["rates"]
    samples = [(timestamp(r["recorded_at"]), int(r["total_lag"])) for r in history]
    warn = breach(samples, warning)
    truth = breach(samples, critical)
    ordered = sorted(forecasts, key=lambda r: (r["recorded_at"], int(r["id"])))
    times = [timestamp(f["recorded_at"]) for f in ordered]
    advice = sorted((r for r in recommendations if r["rule"] in {"scale", "topology"}
                     and r["priority"] in {"HIGH", "MEDIUM"}), key=lambda r: r["recorded_at"])
    first = timestamp(advice[0]["recorded_at"]) if advice else None
    delta = lambda a, b: a-b if a is not None and b is not None else None
    metrics = {"S2_T_warn": warn, "S2_T_adv": first, "S2_T_critical": truth,
               "S2_critical_minus_warning": delta(truth, warn),
               "S2_critical_minus_advice": delta(truth, first),
               "S2_warning_minus_advice": delta(warn, first)}
    index = bisect_right(times, first)-1 if first is not None else -1
    trigger = ordered[index] if index >= 0 else None
    eta = float(trigger["eta_critical_sec"]) if trigger else -2
    metrics["S3_first_advice_eta_error_seconds"] = (
        times[index]+eta-truth if truth is not None and eta > 0 else None)
    errors = defaultdict(list)
    burst = defaultdict(list)
    for f, time in zip(ordered, times):
        eta = float(f["eta_critical_sec"])
        if truth is None or time >= truth or eta <= 0:
            continue
        error = time+eta-truth
        horizon = horizon_bin(truth-time)
        errors[(horizon, f["confidence"])].append(abs(error))
        if rep["pattern"] == "burst":
            beginning = start+rates["burst_at"]
            end = beginning+rates["burst_seconds"]
            phase = "before" if time < beginning else "after" if time >= end else None
            if phase:
                burst[(phase, horizon)].append(error)
                if truth-time <= 900:
                    burst[(phase, "0-15")].append(error)
    for horizon in ("0-5", "5-15", "15-30", "30+"):
        for confidence in ("HIGH", "MEDIUM", "LOW"):
            metrics[f"S4_abs_error_{horizon}_{confidence}"] = median(errors[(horizon, confidence)])
    for phase in ("before", "after"):
        for horizon in ("0-5", "5-15", "15-30", "30+", "0-15"):
            values = burst[(phase, horizon)]
            metrics[f"S5_{phase}_M1_{horizon}"] = median([abs(x) for x in values])
            metrics[f"S5_{phase}_M2_{horizon}"] = median(values)
    # Map advice to its preceding forecast observation so two rules in one cycle
    # count once, and persistence latency cannot move a cycle across fill end.
    advised = {bisect_right(times, timestamp(r["recorded_at"]))-1 for r in advice}
    for phase in ("fill", "after_fill"):
        fraction = None
        if rep["pattern"] == "flat_high":
            end = start+rates["fill_seconds"]
            eligible = {i for i, time in enumerate(times)
                        if (start <= time < end if phase == "fill" else time >= end)}
            fraction = len(eligible & advised)/len(eligible) if eligible else None
        metrics[f"S1_{phase}_advice_fraction"] = fraction
    return metrics
