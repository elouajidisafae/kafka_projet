"""Offline linear-forecast variants; the original baseline fitter is unchanged."""
from datetime import datetime
import numpy as np
from scipy.stats import t as student_t
from bench.replay import Baseline


def prediction_interval(records, threshold, level=.90):
    """First crossings of OLS prediction bands, measured from the last sample."""
    if level != .90:
        raise ValueError("Prediction interval level is fixed at 0.90")
    n = len(records)
    if n < 3:
        return None, None
    times = np.array([datetime.fromisoformat(r["recorded_at"]).timestamp() for r in records])
    x = times-times[0]
    y = np.array([r["total_lag"] for r in records], dtype=float)
    mean = float(x.mean())
    sxx = float(np.sum((x-mean)**2))
    if sxx <= 0:
        return None, None
    slope, intercept = np.polyfit(x, y, 1)
    residual_variance = float(np.sum((y-(slope*x+intercept))**2)/(n-2))
    k2 = float(student_t.ppf(.95, n-2)**2 * residual_variance)
    # Solve in future seconds, avoiding cancellation from absolute timestamps.
    offset = float(x[-1]-mean)
    fitted = float(slope*x[-1]+intercept)
    gap = float(threshold-fitted)
    if k2 < 1e-20:
        eta = 0.0 if gap <= 0 else gap/slope if slope > 0 else None
        eta = eta if eta is not None and eta <= 86400 else None
        return eta, eta
    a = float(slope*slope-k2/sxx)
    b = float(-2*gap*slope-2*k2*offset/sxx)
    c = float(gap*gap-k2*(1+1/n+offset*offset/sxx))
    roots = np.roots([a,b,c] if abs(a)>1e-20 else [b,c]) if abs(a)+abs(b)>1e-20 else []
    def crossing(sign):
        def band(seconds):
            return fitted+slope*seconds+sign*np.sqrt(k2*(1+1/n+(offset+seconds)**2/sxx))
        if band(0) >= threshold:
            return 0.0
        for root in sorted(float(r.real) for r in roots if abs(r.imag)<1e-7):
            if 0 <= root <= 86400 and abs(band(root)-threshold) <= max(1e-5,abs(threshold)*1e-8):
                return root
        return None
    return crossing(1), crossing(-1)


class Variant(Baseline):
    def __init__(self, config, name, short_minutes=5, tolerance=.5):
        super().__init__(config)
        self.name, self.short_minutes, self.tolerance = name, short_minutes, tolerance

    def fit(self, records, now, thresholds):
        long = super().fit(records, now, thresholds)
        if not long.get("enough_data"):
            return long
        result = dict(long)
        selected = records
        if self.name in {"V2", "V3"}:
            end = datetime.fromisoformat(records[-1]["recorded_at"]).timestamp()
            short_records = [r for r in records if datetime.fromisoformat(r["recorded_at"]).timestamp() >= end-self.short_minutes*60]
            short = super().fit(short_records, now, thresholds)
            valid = short.get("enough_data", False)
            short_slope = short.get("slope") if valid else None
            change = valid and abs(short_slope-long["slope"])/max(abs(long["slope"]),.05)>self.tolerance
            if change:
                result, selected = dict(short), short_records
            result.update(window_used="short" if change else "long", slope_short=short_slope,
                          slope_long=long["slope"], regime_change=bool(change),
                          advice_eligible=valid and short["trend"]=="INCREASING")
        if self.name in {"V1", "V3"}:
            low, high = prediction_interval(selected, thresholds["critical_threshold"])
            result.update(eta_low_sec=low, eta_high_sec=high, interval_level=.90)
        return result


def V1(config):
    return Variant(config, "V1")


def V2(config):
    settings=config["forecast"]
    return Variant(config, "V2", settings["short_window_minutes"], settings["agreement_tolerance"])


def V3(config):
    settings=config["forecast"]
    return Variant(config, "V3", settings["short_window_minutes"], settings["agreement_tolerance"])
