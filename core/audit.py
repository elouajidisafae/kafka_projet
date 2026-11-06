"""
Module de Piste d'Audit (Audit Trail)
Permet d'enregistrer les événements système pour la conformité et l'analyse.
"""
import json
from threading import Lock
from .db import save_audit_log, get_audit_logs

def log_event(event_type: str, severity: str, message: str, details: dict = None):
    """
    Enregistre un événement dans la piste d'audit.
    event_type: 'CONFIG_CHANGE', 'ALERT', 'RECOMMENDATION', 'SYSTEM'
    severity: 'INFO', 'WARNING', 'CRITICAL'
    """
    details_str = json.dumps(details) if details else None
    save_audit_log(event_type, severity, message, details_str)
    print(f"[AUDIT] [{severity}] {event_type}: {message}")

def fetch_logs(limit: int = 100):
    """Récupère les derniers logs d'audit."""
    return get_audit_logs(limit)


_failure_lock = Lock()
_failures = set()


def record_failure_transition(cluster: str, *, group: str | None = None,
                              topic: str | None = None, error: str | None = None):
    """Audit failure/recovery edges once per process, shared by web and CLI."""
    key = (cluster, group, topic)
    failed = error is not None
    with _failure_lock:
        if failed == (key in _failures):
            return
        details = {"cluster": cluster}
        target = cluster
        if group is not None:
            details.update(group=group, topic=topic)
            target = f"{cluster}/{group}/{topic}"
        if failed:
            details["error"] = error
        label = "Lag measurement" if group is not None else "Cluster collection"
        message = f"{label} {'failed' if failed else 'recovered'}: {target}"
        if failed:
            message += f": {error}"
        log_event("SYSTEM", "WARNING" if failed else "INFO", message, details)
        if failed:
            _failures.add(key)
        else:
            _failures.remove(key)
