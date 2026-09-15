"""
Persistance SQLite — historique du lag avec support multi-cluster.
"""
import sqlite3
import os
from threading import Lock
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config_loader import CONFIG

DB_PATH = Path(os.environ.get("KHM_DB_PATH", str(Path(__file__).parent.parent / "lag_history.db")))


def utc_now_iso() -> str:
    """Current UTC time as a timezone-aware ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def parse_iso_utc(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp, treating naive values as UTC (legacy rows)."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class _Connection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def _get_connection() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), factory=_Connection)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
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
                group_state       TEXT    NOT NULL DEFAULT 'UNKNOWN',
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS group_state_streak (
                cluster_name TEXT NOT NULL,
                group_id TEXT NOT NULL,
                state TEXT NOT NULL,
                streak_count INTEGER NOT NULL,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (cluster_name, group_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lag_history_lookup ON lag_history (cluster_name, group_id, topic, recorded_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lag_history_recorded ON lag_history (recorded_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_recorded ON forecast_log (recorded_at)")
        for stmt in [
            "ALTER TABLE lag_history ADD COLUMN log_end_offset INTEGER",
            "ALTER TABLE lag_history ADD COLUMN committed_offset INTEGER",
            "ALTER TABLE lag_history ADD COLUMN partition_count INTEGER",
            "ALTER TABLE lag_history ADD COLUMN partitions_counted INTEGER",
            "ALTER TABLE lag_history ADD COLUMN run_id TEXT",
            "ALTER TABLE lag_history ADD COLUMN consumer_count INTEGER",
        ]:
            try:
                conn.execute(stmt)
            except Exception:
                pass


def save_lag(cluster_name: str, group_id: str, topic: str,
             total_lag: int, status: str, group_state: str = "UNKNOWN",
             *, log_end_offset: int | None = None, committed_offset: int | None = None,
             partition_count: int | None = None, partitions_counted: int | None = None,
             run_id: str | None = None, consumer_count: int | None = None):
    from .kafka_client import normalize_group_state
    group_state = normalize_group_state(group_state)
    now = utc_now_iso()
    with _get_connection() as conn:
        conn.execute(
            """INSERT INTO lag_history
               (cluster_name, group_id, topic, total_lag, log_end_offset, committed_offset,
                partition_count, partitions_counted, consumer_count, status, group_state, run_id, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cluster_name, group_id, topic, total_lag, log_end_offset, committed_offset,
                partition_count, partitions_counted, consumer_count, status, group_state, run_id or CONFIG.get("run_id"), now
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


def get_lag_history_bulk(cluster_name: str | None = None, last_hours: float = 1.0) -> dict:
    """Fetch one history window, preserving timestamp order within each pair."""
    since = (datetime.now(timezone.utc) - timedelta(hours=last_hours)).isoformat()
    sql = "SELECT * FROM lag_history WHERE recorded_at>=?"
    params = [since]
    if cluster_name is not None:
        sql += " AND cluster_name=?"
        params.append(cluster_name)
    sql += " ORDER BY cluster_name, group_id, topic, recorded_at ASC"
    with _get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    grouped = {}
    for row in rows:
        key = (row["cluster_name"], row["group_id"], row["topic"])
        grouped.setdefault(key, []).append(dict(row))
    return grouped


def get_group_state_streaks() -> dict:
    with _get_connection() as conn:
        rows = conn.execute("SELECT * FROM group_state_streak").fetchall()
    return {(row["cluster_name"], row["group_id"]): dict(row) for row in rows}


def get_latest_per_group(cluster_name: str = None) -> list[dict]:
    with _get_connection() as conn:
        if cluster_name:
            rows = conn.execute(
                """SELECT cluster_name, group_id, topic, total_lag, status, group_state,
                          partition_count, partitions_counted, consumer_count,
                          MAX(recorded_at) as recorded_at
                   FROM lag_history WHERE cluster_name=?
                   GROUP BY cluster_name, group_id, topic
                   ORDER BY total_lag DESC""",
                (cluster_name,)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT cluster_name, group_id, topic, total_lag, status, group_state,
                          partition_count, partitions_counted, consumer_count,
                          MAX(recorded_at) as recorded_at
                   FROM lag_history
                   GROUP BY cluster_name, group_id, topic
                   ORDER BY cluster_name, total_lag DESC"""
            ).fetchall()
    return [dict(row) for row in rows]


def purge_old_records():
    settings = CONFIG.get("retention", {})
    retention = settings.get("days", CONFIG["monitor"]["history_retention_days"])
    batch_size = max(1, int(settings.get("prune_batch_size", 5000)))
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention)).isoformat()
    deleted_lag = 0
    for table in ("lag_history", "forecast_log"):
        while True:
            with _get_connection() as conn:
                deleted = conn.execute(
                    f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} "
                    "WHERE recorded_at < ? ORDER BY recorded_at LIMIT ?)",
                    (cutoff, batch_size)).rowcount
                conn.commit()
            if table == "lag_history":
                deleted_lag += deleted
            if deleted < batch_size:
                break
    return deleted_lag


_prune_lock = Lock()
_prune_cycles = {}


def maybe_purge_old_records():
    """Prune on the configured cycle cadence, with bounded transactions."""
    with _prune_lock:
        key = str(DB_PATH)
        cycle = _prune_cycles.get(key, 0) + 1
        every = max(1, int(CONFIG.get("retention", {}).get("prune_every_cycles", 60)))
        if cycle < every:
            _prune_cycles[key] = cycle
            return 0
        deleted = purge_old_records()
        _prune_cycles[key] = 0
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


def update_group_state_streak(cluster_name: str, group_id: str, state: str) -> None:
    """Record one observation per group per collection cycle, across processes."""
    from .kafka_client import normalize_group_state
    with _get_connection() as conn:
        conn.execute("""
            INSERT INTO group_state_streak VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(cluster_name, group_id) DO UPDATE SET
                streak_count = CASE WHEN state = excluded.state THEN streak_count + 1 ELSE 1 END,
                state = excluded.state,
                last_seen = excluded.last_seen
        """, (cluster_name, group_id, normalize_group_state(state), utc_now_iso()))


def get_group_state_streak(cluster_name: str, group_id: str) -> dict | None:
    with _get_connection() as conn:
        row = conn.execute("SELECT * FROM group_state_streak WHERE cluster_name=? AND group_id=?",
                           (cluster_name, group_id)).fetchone()
    return dict(row) if row else None
