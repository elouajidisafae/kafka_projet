"""Deterministic pair values with timestamps relative to the supplied clock."""
from datetime import datetime, timedelta, timezone

def populate(db, pairs, points=8):
    now = datetime.now(timezone.utc)
    with db._get_connection() as conn:
        conn.executemany(
            "INSERT INTO lag_history (cluster_name,group_id,topic,total_lag,status,group_state,partition_count,partitions_counted,consumer_count,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [("dev", f"g{i:04}", "orders", 100 + j * 19, "OK", "STABLE", 3, 3, 1,
              (now - timedelta(seconds=(points-j)*10)).isoformat())
             for i in range(pairs) for j in range(points)])

