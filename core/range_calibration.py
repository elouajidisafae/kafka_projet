"""Runtime CRITICAL crossing ranges; absence of calibration fails closed."""
from functools import lru_cache
import math
from pathlib import Path
import yaml
PATH = Path(__file__).with_name("range_calibration.yml")

@lru_cache(maxsize=1)
def load():
    try:
        value = yaml.safe_load(PATH.read_text())
        if not isinstance(value, dict): return {}
        return value
    except (OSError, ValueError, yaml.YAMLError):
        return {}

def apply(result, method, settings):
    for name in ("warning", "critical"):
        for side in ("low", "high"):
            result[f"eta_{name}_formula_{side}_sec"] = result.get(f"eta_{name}_{side}_sec")
            result[f"eta_{name}_{side}_sec"] = None
    result["eta_low_sec"] = result["eta_high_sec"] = None
    result["range_coverage"] = None
    result["range_tooltip"] = "Range unavailable"
    data = load()
    # Calibration applies only to the shipped responsive rule.
    if method == "multiwindow" and (settings.get("short_window_minutes",5) != 5 or settings.get("agreement_tolerance",.5) != .5):
        return result
    eta = result.get("eta_critical_sec")
    if not result.get("enough_data") or eta is None or not math.isfinite(eta) or eta <= 0: return result
    label = "0-5" if eta <= 300 else "5-15" if eta <= 900 else "15-30" if eta <= 1800 else "30+"
    factor = data.get("modes",{}).get(method,{}).get(label,{})
    lo, hi = factor.get("q05"), factor.get("q95")
    if not factor.get("n") or lo is None or hi is None or not (0 <= lo <= hi < float("inf")): return result
    coverage = data.get("pooled_coverage",{}).get(method)
    result.update(eta_critical_low_sec=lo*eta, eta_critical_high_sec=hi*eta,
                  eta_low_sec=lo*eta, eta_high_sec=hi*eta, range_coverage=coverage)
    if coverage is not None:
        result["range_tooltip"] = f"Range calibrated on benchmark workloads; measured coverage {coverage*100:.1f}%"
    return result
