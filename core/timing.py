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
        _values.setdefault(cluster, {}).update(forecast=seconds, pairs=pairs)
    _log.debug("forecast cluster=%s duration_seconds=%.6f pairs=%d", cluster, seconds, pairs)


def prometheus_lines():
    with _lock:
        values = {cluster: dict(metrics) for cluster, metrics in _values.items()}
    lines = []
    for name, field in (("khm_collection_duration_seconds", "collection"),
                        ("khm_forecast_duration_seconds", "forecast"),
                        ("khm_monitored_pairs", "pairs")):
        lines.append(f"# TYPE {name} gauge")
        for cluster, metrics in sorted(values.items()):
            label = cluster.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            lines.append(f'{name}{{cluster="{label}"}} {metrics.get(field, 0)}')
    return lines
