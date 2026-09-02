"""
Persistance SQLite — historique du lag avec support multi-cluster.
"""
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config_loader import CONFIG

DB_PATH = Path(__file__).parent.parent / "lag_history.db"


def utc_now_iso() -> str:
    """Current UTC time as a timezone-aware ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def parse_iso_utc(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp, treating naive values as UTC (legacy rows)."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lag_history (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_name      TEXT    NOT NULL DEFAULT 'default',
                group_id          TEXT    NOT NULL,
                topic             TEXT    NOT NULL,
                total_lag         INTEGER NOT NULL,
                log_end_offset    INTEGER,
                committed_offset  INTEGER,
                partition_count   INTEGER,
                partitions_counted INTEGER,
                status            TEXT    NOT NULL,
                group_state       TEXT    NOT NULL DEFAULT 'Unknown',
                run_id            TEXT,
                recorded_at       TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forecast_log (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_name       TEXT    NOT NULL,
                group_id           TEXT    NOT NULL,
                topic              TEXT    NOT NULL,
                slope              REAL    NOT NULL,
                intercept          REAL    NOT NULL,
                r_squared          REAL    NOT NULL,
                confidence         TEXT    NOT NULL,
                trend              TEXT    NOT NULL,
                current_lag        INTEGER NOT NULL,
                n_points           INTEGER NOT NULL,
                window_seconds     INTEGER NOT NULL,
                eta_warning_sec    INTEGER,
                eta_critical_sec   INTEGER,
                predicted_lag_5min INTEGER,
                predicted_lag_15min INTEGER,
                warning_threshold  INTEGER NOT NULL,
                critical_threshold INTEGER NOT NULL,
                run_id             TEXT,
                recorded_at        TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_forecast_lookup
            ON forecast_log (cluster_name, group_id, topic, recorded_at)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT    NOT NULL,
                severity    TEXT    NOT NULL,
                message     TEXT    NOT NULL,
                details     TEXT,
                recorded_at TEXT    NOT NULL
            )
        """)
        for stmt in [
            "ALTER TABLE lag_history ADD COLUMN log_end_offset INTEGER",
            "ALTER TABLE lag_history ADD COLUMN committed_offset INTEGER",
            "ALTER TABLE lag_history ADD COLUMN partition_count INTEGER",
            "ALTER TABLE lag_history ADD COLUMN partitions_counted INTEGER",
            "ALTER TABLE lag_history ADD COLUMN run_id TEXT",
        ]:
            try:
                conn.execute(stmt)
            except Exception:
                pass


def save_lag(cluster_name: str, group_id: str, topic: str,
             total_lag: int, status: str, group_state: str = "Unknown",
             *, log_end_offset: int | None = None, committed_offset: int | None = None,
             partition_count: int | None = None, partitions_counted: int | None = None,
             run_id: str | None = None):
    now = utc_now_iso()
    with _get_connection() as conn:
        conn.execute(
            """INSERT INTO lag_history
               (cluster_name, group_id, topic, total_lag, log_end_offset, committed_offset,
                partition_count, partitions_counted, status, group_state, run_id, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cluster_name, group_id, topic, total_lag, log_end_offset, committed_offset,
                partition_count, partitions_counted, status, group_state, run_id or CONFIG.get("run_id"), now
            )
        )
        conn.commit()


def get_lag_history(cluster_name: str, group_id: str, topic: str, last_hours: int = 1) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=last_hours)).isoformat()
    with _get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM lag_history
               WHERE cluster_name=? AND group_id=? AND topic=? AND recorded_at>=?
               ORDER BY recorded_at ASC""",
            (cluster_name, group_id, topic, since)
        ).fetchall()
    return [dict(row) for row in rows]


def get_latest_per_group(cluster_name: str = None) -> list[dict]:
    with _get_connection() as conn:
        if cluster_name:
            rows = conn.execute(
                """SELECT cluster_name, group_id, topic, total_lag, status, group_state,
                          MAX(recorded_at) as recorded_at
                   FROM lag_history WHERE cluster_name=?
                   GROUP BY cluster_name, group_id, topic
                   ORDER BY total_lag DESC""",
                (cluster_name,)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT cluster_name, group_id, topic, total_lag, status, group_state,
                          MAX(recorded_at) as recorded_at
                   FROM lag_history
                   GROUP BY cluster_name, group_id, topic
                   ORDER BY cluster_name, total_lag DESC"""
            ).fetchall()
    return [dict(row) for row in rows]


def purge_old_records():
    retention = CONFIG["monitor"]["history_retention_days"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention)).isoformat()
    with _get_connection() as conn:
        deleted = conn.execute(
            "DELETE FROM lag_history WHERE recorded_at < ?",
            (cutoff,)
        ).rowcount
        conn.commit()
    return deleted


def save_audit_log(event_type: str, severity: str, message: str, details: str = None):
    now = utc_now_iso()
    with _get_connection() as conn:
        conn.execute(
            """INSERT INTO audit_logs (event_type, severity, message, details, recorded_at)
               VALUES (?, ?, ?, ?, ?)""",
            (event_type, severity, message, details, now)
        )
        conn.commit()


def get_audit_logs(limit: int = 100) -> list[dict]:
    with _get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM audit_logs ORDER BY recorded_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def save_forecast(forecast: dict, run_id: str | None = None) -> None:
    """Persist one forecast_lag() result. No-op when enough_data is False."""
    if not forecast.get("enough_data"):
        return

    payload = {
        "cluster_name": forecast["cluster_name"],
        "group_id": forecast["group_id"],
        "topic": forecast["topic"],
        "slope": float(forecast["slope"]),
        "intercept": float(forecast["intercept"]),
        "r_squared": float(forecast["r_squared"]),
        "confidence": forecast["confidence"],
        "trend": forecast["trend"],
        "current_lag": int(forecast["current_lag"]),
        "n_points": int(forecast["n_points"]),
        "window_seconds": int(forecast["window_seconds"]),
        "eta_warning_sec": forecast.get("eta_warning_sec"),
        "eta_critical_sec": forecast.get("eta_critical_sec"),
        "predicted_lag_5min": forecast.get("predicted_lag_5min"),
        "predicted_lag_15min": forecast.get("predicted_lag_15min"),
        "warning_threshold": int(forecast["warning_threshold"]),
        "critical_threshold": int(forecast["critical_threshold"]),
        "run_id": run_id or CONFIG.get("run_id"),
        "recorded_at": utc_now_iso(),
    }
    with _get_connection() as conn:
        conn.execute(
            """
            INSERT INTO forecast_log (
                cluster_name, group_id, topic, slope, intercept, r_squared, confidence, trend,
                current_lag, n_points, window_seconds, eta_warning_sec, eta_critical_sec,
                predicted_lag_5min, predicted_lag_15min, warning_threshold, critical_threshold,
                run_id, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["cluster_name"], payload["group_id"], payload["topic"], payload["slope"], payload["intercept"],
                payload["r_squared"], payload["confidence"], payload["trend"], payload["current_lag"], payload["n_points"],
                payload["window_seconds"], payload["eta_warning_sec"], payload["eta_critical_sec"], payload["predicted_lag_5min"],
                payload["predicted_lag_15min"], payload["warning_threshold"], payload["critical_threshold"], payload["run_id"],
                payload["recorded_at"],
            ),
        )
        conn.commit()