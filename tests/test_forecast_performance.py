"""Behavior and operation-count checks for collection performance."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from time import perf_counter
from unittest.mock import patch

import pytest
from core import db, forecasting as fc, recommender, timing
from core import lag_calculator as lag
from core.kafka_client import GroupDescription, GroupMember


@pytest.fixture(params=["baseline", "multiwindow"])
def database(tmp_path, monkeypatch, request):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "history.db")
    monkeypatch.setattr(fc, "_cache", None)
    monkeypatch.setitem(fc.CONFIG, "forecast", dict(fc.CONFIG["forecast"], method=request.param, persist_every_cycle=False))
    db.init_db()
    return db.DB_PATH


def populate(pairs, points=8):
    import runpy
    from pathlib import Path
    helper = runpy.run_path(str(Path(__file__).parent / "fixtures" / "synthetic_history.py"))
    helper["populate"](db, pairs, points)


@pytest.mark.parametrize("pairs", [1, 200])
def test_one_history_query(database, monkeypatch, pairs):
    populate(pairs)
    statements = []
    original = db._get_connection
    def traced():
        conn = original()
        conn.set_trace_callback(statements.append)
        return conn
    monkeypatch.setattr(db, "_get_connection", traced)
    assert len(fc.forecast_all()) == pairs
    queries = [q for q in statements if q.startswith("SELECT")]
    assert len(queries) == 2  # one latest snapshot, one bulk window
    assert len([q for q in queries if "recorded_at>=" in q]) == 1
    assert len(recommender.get_recommendations()) == pairs  # HIGH items survive the cluster cap
    # Recommendation reads are two constant queries: latest rows and all streaks.
    assert len([q for q in statements if q.startswith("SELECT")]) == 4


def test_bulk_matches_single_and_scoped(database):
    populate(3)
    bulk = db.get_lag_history_bulk("dev")
    assert len(bulk) == 3
    for key, rows in bulk.items():
        assert rows == db.get_lag_history(*key)
        assert fc.fit_configured_forecast(rows, *key) == fc.forecast_lag(*key)
    assert db.get_lag_history_bulk("absent") == {}


def test_once_per_pair_then_expiry(database, monkeypatch):
    populate(4)
    clock = [100.0]
    monkeypatch.setattr(fc, "monotonic", lambda: clock[0])
    with patch.object(fc, "fit_configured_forecast", wraps=fc.fit_configured_forecast) as fit:
        fc.forecast_all()
        recommender.get_recommendations()
        fc.cached_forecast_all()
        assert fit.call_count == 4
        clock[0] += fc.CONFIG["monitor"]["refresh_interval"]
        fc.cached_forecast_all()
        assert fit.call_count == 8


def test_concurrent_cache_miss_and_defensive_copy(database):
    populate(5)
    with patch.object(fc, "fit_configured_forecast", wraps=fc.fit_configured_forecast) as fit:
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: fc.forecasts_for_request(), range(16)))
        assert fit.call_count == 5
    assert all(value == values[0] and len(value) == 5 for value in values)
    values[0].clear()
    assert len(fc.get_cached_forecasts()) == 5
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda i: fc.compute_cycle_forecasts() if i % 2 else fc.forecasts_for_request(), range(8)))
    assert all(len(value) == 5 for value in results)


def test_scoped_cache_does_not_replace_all(database):
    populate(2)
    fc.compute_cycle_forecasts()
    assert fc.compute_cycle_forecasts("absent") == {}
    assert len(fc.get_cached_forecasts()) == 2


def test_config_change_invalidates_cache(database, monkeypatch):
    populate(1)
    fc.compute_cycle_forecasts()
    monkeypatch.setitem(fc.CONFIG, "alerts", {"warning_threshold": 3, "critical_threshold": 10})
    assert fc.get_cached_forecasts() is None
    assert next(iter(fc.forecasts_for_request().values()))["critical_threshold"] == 10


def test_membership_once_per_cluster(database, monkeypatch):
    monkeypatch.setattr(lag, "CONFIG", dict(lag.CONFIG, clusters=[{"name": c, "bootstrap_servers": c} for c in ("a", "b")]))
    descriptions = {f"g{i}": GroupDescription(f"g{i}", "STABLE", [GroupMember("m", "c", "h", {"orders": [0]})]) for i in range(5)}
    with patch.object(lag, "describe_groups", return_value=descriptions) as describe:
        monkeypatch.setattr(lag, "get_topics", lambda _: [f"t{i}" for i in range(5)])
        monkeypatch.setattr(lag, "compute_lag_for_group", lambda c, b, g, t: lag.ConsumerGroupStatus(c, g, t))
        assert len(lag.compute_all_lags()) == 50
        assert describe.call_count == 2


def test_indexes_migration_wal_and_plans(database):
    with db._get_connection() as conn:
        for index in ("idx_lag_history_lookup", "idx_lag_history_recorded"):
            conn.execute(f"DROP INDEX {index}")
    db.init_db()
    with db._get_connection() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1
        for sql in (
            "SELECT * FROM lag_history WHERE recorded_at >= '2026' ORDER BY cluster_name,group_id,topic,recorded_at",
            "DELETE FROM lag_history WHERE id IN (SELECT id FROM lag_history WHERE recorded_at < '2026' ORDER BY recorded_at LIMIT 5000)",
            "DELETE FROM forecast_log WHERE id IN (SELECT id FROM forecast_log WHERE recorded_at < '2026' ORDER BY recorded_at LIMIT 5000)",
        ):
            plan = " ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql))
            assert "USING" in plan and "INDEX" in plan
        assert {r[1] for r in conn.execute("PRAGMA index_list(lag_history)")} >= {"idx_lag_history_lookup", "idx_lag_history_recorded"}


def test_prune_cadence_and_bounded_transactions(database, monkeypatch):
    populate(12)
    forecasts = fc.forecast_all()
    for value in forecasts:
        db.save_forecast(value)
    with db._get_connection() as conn:
        conn.execute("UPDATE lag_history SET recorded_at='2000-01-01'")
        conn.execute("UPDATE forecast_log SET recorded_at='2000-01-01'")
    monkeypatch.setitem(db.CONFIG, "retention", {"days": 7, "prune_every_cycles": 3, "prune_batch_size": 5})
    assert db.maybe_purge_old_records() == 0
    assert db.maybe_purge_old_records() == 0
    assert len(db.get_latest_per_group()) == 12
    assert db.maybe_purge_old_records() == 96
    with db._get_connection() as conn:
        assert conn.execute("SELECT count(*) FROM lag_history").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM forecast_log").fetchone()[0] == 0


def test_metrics_match_snapshot(database):
    from interfaces.web import metrics
    populate(3)
    start = perf_counter()
    fc.forecast_all()
    elapsed = perf_counter() - start
    timing.record_collection("dev", elapsed, 3)
    output = metrics()
    for name in ("khm_collection_duration_seconds", "khm_forecast_duration_seconds", "khm_monitored_pairs"):
        assert f"# TYPE {name} gauge" in output
    assert 'khm_monitored_pairs{cluster="dev"} 3' in output
    assert 0 <= timing._values["dev"]["forecast"] <= elapsed


@pytest.mark.parametrize("rows", [2000, 200000])
def test_retention_concurrent_reader(database, monkeypatch, rows):
    import os
    import sqlite3
    from threading import Event
    if rows == 200000 and os.environ.get("KHM_LARGE_RETENTION_TEST") != "1":
        pytest.skip("Owner-run large retention check: set KHM_LARGE_RETENTION_TEST=1")
    populate(1)
    db.save_forecast(fc.forecast_all()[0])
    with db._get_connection() as conn:
        conn.execute("UPDATE forecast_log SET recorded_at='2000-01-01'")
        conn.executemany("INSERT INTO lag_history (cluster_name,group_id,topic,total_lag,status,recorded_at) VALUES ('dev','old','orders',1,'OK','2000-01-01')", [()] * rows)
        columns = [r[1] for r in conn.execute("PRAGMA table_info(forecast_log)") if r[1] != "id"]
        names = ",".join(columns)
        original = tuple(conn.execute(f"SELECT {names} FROM forecast_log LIMIT 1").fetchone())
        conn.executemany(f"INSERT INTO forecast_log ({names}) VALUES ({','.join('?' for _ in columns)})", [original] * (rows-1))
    monkeypatch.setitem(db.CONFIG, "retention", {"days": 7, "prune_batch_size": 5000})
    writer_started, reader_done, writer_finished = Event(), Event(), Event()
    original_connection = db._get_connection
    batches = []
    def traced_connection():
        conn = original_connection()
        previous = [conn.total_changes]
        def trace(sql):
            if sql.startswith("DELETE"):
                if not writer_started.is_set():
                    writer_started.set()
                    assert reader_done.wait(5), "Concurrent reader blocked"
            if sql == "COMMIT":
                batches.append(conn.total_changes - previous[0])
                previous[0] = conn.total_changes
        conn.set_trace_callback(trace)
        return conn
    monkeypatch.setattr(db, "_get_connection", traced_connection)
    def reader():
        assert writer_started.wait(5)
        # Hold a read snapshot while all bounded write transactions run.
        conn = sqlite3.connect(str(database))
        try:
            conn.execute("BEGIN")
            assert conn.execute("SELECT count(*) FROM lag_history").fetchone()[0] >= rows
            reader_done.set()
            count = conn.execute("SELECT count(*) FROM forecast_log").fetchone()[0]
            assert writer_finished.wait(300 if rows == 200000 else 10)
            return count
        finally:
            conn.close()
    def writer():
        try:
            return db.purge_old_records()
        finally:
            writer_finished.set()
    with ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(reader)
        pruning = pool.submit(writer)
        assert pruning.result(timeout=300 if rows == 200000 else 10) == rows
        assert reading.result(timeout=10) == rows
    assert max(batches) <= 5000
    assert sum(batches) == rows * 2
    with original_connection() as conn:
        assert conn.execute("SELECT count(*) FROM forecast_log").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM lag_history").fetchone()[0] == 8
