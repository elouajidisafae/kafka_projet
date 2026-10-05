"""Thread-safe measurements for completed collection and forecast phases."""
import logging
from threading import Lock

_lock = Lock()
_values = {}
_log = logging.getLogger(__name__)


def record_collection(cluster, seconds, pairs):
    with _lock:
        _values.setdefault(cluster, {}).update(collection=seconds)
    _log.debug("collection cluster=%s duration_seconds=%.6f pairs=%d", cluster, seconds, pairs)


def record_forecast(cluster, seconds, pairs):
    with _lock:
        metrics = _values.setdefault(cluster, {})
        metrics.update(forecast=seconds, pairs=pairs, cycles=metrics.get("cycles", 0) + 1)
    _log.debug("forecast cluster=%s duration_seconds=%.6f pairs=%d", cluster, seconds, pairs)


def prometheus_lines():
    with _lock:
        values = {cluster: dict(metrics.get("completed", metrics), overruns=metrics.get("overruns", 0))
                  for cluster, metrics in _values.items()}
    lines = []
    for name, field in (("khm_collection_duration_seconds", "collection"),
                        ("khm_forecast_duration_seconds", "forecast"),
                        ("khm_monitored_pairs", "pairs")):
        lines.append(f"# TYPE {name} gauge")
        for cluster, metrics in sorted(values.items()):
            label = cluster.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            lines.append(f'{name}{{cluster="{label}"}} {metrics.get(field, 0)}')
    lines.append("# TYPE khm_collection_overruns_total counter")
    lines.append("# TYPE khm_forecast_cycles_total counter")
    lines.append("# TYPE khm_collection_cycles_total counter")
    for cluster, metrics in sorted(values.items()):
        import json
        lines.append(f'khm_collection_overruns_total{{cluster={json.dumps(cluster)}}} {metrics.get("overruns", 0)}')
        lines.append(f'khm_forecast_cycles_total{{cluster={json.dumps(cluster)}}} {metrics.get("cycles", 0)}')
        lines.append(f'khm_collection_cycles_total{{cluster={json.dumps(cluster)}}} {metrics.get("completed_cycles", 0)}')
    return lines


def record_overrun(cluster):
    with _lock:
        metrics = _values.setdefault(cluster, {})
        metrics["overruns"] = metrics.get("overruns", 0) + 1


def record_cycle(cluster):
    """Publish phase measurements together only after the collector finishes."""
    with _lock:
        metrics = _values.setdefault(cluster, {})
        metrics["completed_cycles"] = metrics.get("completed_cycles", 0) + 1
        metrics["completed"] = {k:v for k,v in metrics.items() if k != "completed"}
