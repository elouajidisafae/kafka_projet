"""Offline Holt/ARIMA replay adapters; live forecasting is never modified."""
from datetime import datetime
import math
import warnings
import numpy as np
from bench.replay import Baseline


def regular_series(records, step=5):
    """Right-edge grid anchored to the last input; causal carry-forward only.

    A step is filled when its interval (t-step,t] contains no observation.
    No values before the first observation or after the last are invented.
    """
    if not records or step <= 0:
        raise ValueError("Positive interval and nonempty history required")
    times = np.array([datetime.fromisoformat(r["recorded_at"]).timestamp() for r in records])
    values = np.array([r["total_lag"] for r in records], dtype=float)
    if not np.isfinite(values).all() or np.any(np.diff(times) < 0):
        raise ValueError("Invalid or unordered inputs")
    grid = times[-1] - np.arange(int((times[-1]-times[0])/step)+1)[::-1]*step
    indices = np.searchsorted(times, grid, side="right")-1
    previous = np.searchsorted(times, grid-step, side="right")-1
    filled = int(np.sum(indices == previous))
    return values[indices], filled


def holt_projection(values, alpha, beta, steps):
    # Standard undamped additive Holt recursions; deterministic known initial state.
    # All requested beta values are valid here, including beta > alpha.
    level, trend = float(values[0]), float(values[1]-values[0])
    for value in values[1:]:
        old = level
        level = alpha*value+(1-alpha)*(level+trend)
        trend = beta*(level-old)+(1-beta)*trend
    return level + np.arange(1, steps+1)*trend


def first_crossing(projected, threshold, step):
    indices = np.flatnonzero(np.asarray(projected) >= threshold)
    return int((indices[0]+1)*step) if len(indices) else -2


class OfflineModel(Baseline):
    def __init__(self, config, name, **settings):
        super().__init__(config)
        self.name, self.settings = name, settings

    def fit(self, records, now, thresholds):
        result = dict(enough_data=False, fit_failed=False, filled_steps=0)
        if records:
            result.update({k: records[-1][k] for k in ("cluster_name", "group_id", "topic")})
        if len(records) < 5:
            return dict(result, reason="Insufficient data", eta_critical_sec=None)
        try:
            step = self.config["monitor"]["refresh_interval"]
            values, filled = regular_series(records, step)
            result["filled_steps"] = filled
            if len(values) < 5:
                return dict(result, reason="Insufficient regular steps", eta_critical_sec=None)
            steps = math.ceil(86400/step)
            if self.name == "Holt":
                projected = holt_projection(values, self.settings["alpha"], self.settings["beta"], steps)
            elif self.name == "ARIMA":
                from statsmodels.tsa.arima.model import ARIMA
                from statsmodels.tools.sm_exceptions import ConvergenceWarning
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    # d=1: trend='t' in levels gives a constant in first differences (drift).
                    fitted = ARIMA(values, order=(self.settings["p"], 1, self.settings["q"]), trend="t").fit()
                    if not fitted.mle_retvals.get("converged", True) or any(issubclass(w.category, ConvergenceWarning) for w in caught):
                        raise RuntimeError("ARIMA optimiser did not converge")
                    projected = np.asarray(fitted.forecast(steps), dtype=float)
            else:
                raise ValueError("Unknown offline model")
            if not np.isfinite(projected).all():
                raise ValueError("Nonfinite forecast")
            current = records[-1]["total_lag"]
            def eta(threshold):
                return -1 if current >= threshold else first_crossing(projected, threshold, step)
            return dict(result, enough_data=True, current_lag=current, eta_critical_sec=eta(thresholds["critical_threshold"]),
                        eta_warning_sec=eta(thresholds["warning_threshold"]),
                        predicted_lag_5min=max(0, float(projected[math.ceil(300/step)-1])),
                        predicted_lag_15min=max(0, float(projected[math.ceil(900/step)-1])),
                        confidence="NOT_APPLICABLE")
        except Exception as exc:
            return dict(result, fit_failed=True, reason=f"{type(exc).__name__}: {exc}", eta_critical_sec=None)


def Holt(config):
    return OfflineModel(config, "Holt", **config["model_comparison"]["Holt"])


def ARIMA(config):
    return OfflineModel(config, "ARIMA", **config["model_comparison"]["ARIMA"])
