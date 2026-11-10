"""Runtime CRITICAL crossing ranges; absence of calibration fails closed."""
from functools import lru_cache
import math
import json
from pathlib import Path
import yaml
from .range_groups import factor_for

PATH = Path(__file__).with_name("range_calibration_v2.yml")
RESULTS_PATH = Path(__file__).resolve().parents[1]/"bench/results/final-confirmation-latest.json"

@lru_cache(maxsize=1)
def load():
    try:
        value = yaml.safe_load(PATH.read_text())
        if not isinstance(value, dict): return {}
        return value
    except (OSError, ValueError, yaml.YAMLError):
        return {}


def measured_coverage():
    try:
        return _coverage(str(RESULTS_PATH), RESULTS_PATH.stat().st_mtime_ns)
    except OSError:
        return {}


@lru_cache(maxsize=2)
def _coverage(path, stamp):
    try:
        pointer = json.loads(Path(path).read_text())
        import hashlib
        source = Path(__file__).resolve().parents[1]/pointer['summary_path']
        raw = source.read_bytes()
        if hashlib.sha256(raw).hexdigest() != pointer['sha256']:
            return {}
        result = json.loads(raw)
        return {mode: next(r['coverage'] for r in result['ranges'][model]['table']
                          if r['pattern']=='pooled' and r['bin']=='0-30' and r['version']=='v2')
                for mode,model in pointer['model_modes'].items()}
    except (OSError, ValueError, AttributeError, KeyError, StopIteration):
        return {}


def apply(result, method, settings):
    for name in ("warning", "critical"):
        for side in ("low", "high"):
            result[f"eta_{name}_formula_{side}_sec"] = result.get(f"eta_{name}_{side}_sec")
            result[f"eta_{name}_{side}_sec"] = None
    result["eta_low_sec"] = result["eta_high_sec"] = None
    result["range_coverage"] = None
    result["range_tooltip"] = "Range unavailable"
    result["range_status"] = "unavailable"
    eta = result.get("eta_critical_sec")
    if result.get("enough_data") and eta is not None and math.isfinite(eta) and eta > 1800:
        result["range_status"] = "beyond_30_minutes"
        return result
    data = load()
    # Calibration applies only to the shipped responsive rule.
    if method == "multiwindow" and (settings.get("short_window_minutes",5) != 5 or settings.get("agreement_tolerance",.5) != .5):
        return result
    if not result.get("enough_data") or eta is None or not math.isfinite(eta) or eta <= 0: return result
    model = data.get("modes", {}).get(method)
    if not model:
        return result
    factor = factor_for(model, eta, result.get("confidence"), result.get("window_seconds")) or {}
    lo, hi = factor.get("q05"), factor.get("q95")
    if not factor.get("n") or lo is None or hi is None or not (0 <= lo <= hi < float("inf")): return result
    coverage = measured_coverage().get(method)
    result.update(eta_critical_low_sec=lo*eta, eta_critical_high_sec=hi*eta,
                  eta_low_sec=lo*eta, eta_high_sec=hi*eta, range_coverage=coverage,
                  range_status="calibrated",
                  range_tooltip="Range calibrated on benchmark workloads; measured coverage unavailable")
    if coverage is not None:
        result["range_tooltip"] = f"Range calibrated on benchmark workloads; measured coverage {coverage*100:.1f}%"
    return result
