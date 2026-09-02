"""
Tests de la couche SQLite.

On utilise une base temporaire pour chaque test
afin de ne pas polluer la base de production.
"""
import pytest
import tempfile
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


@pytest.fixture
def tmp_db(tmp_path):
    """
    Fixture qui crée une base SQLite temporaire pour chaque test.
    La base est automatiquement supprimée après le test.
    """
    db_path = tmp_path / "test_lag_history.db"
    with patch("core.db.DB_PATH", db_path):
        from core.db import init_db
        init_db()
        yield db_path


class TestDatabase:

    def test_init_creates_table(self, tmp_db):
        """init_db() crée bien la table lag_history."""
        import sqlite3
        with sqlite3.connect(str(tmp_db)) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        table_names = [t[0] for t in tables]
        assert "lag_history" in table_names

    def test_save_and_retrieve_lag(self, tmp_db):
        """Un lag sauvegardé est bien récupéré."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_lag_history
            save_lag("dev", "my-group", "orders", 1500, "WARNING")
            records = get_lag_history("dev", "my-group", "orders", last_hours=1)

        assert len(records) == 1
        assert records[0]["total_lag"] == 1500
        assert records[0]["status"]    == "WARNING"

    def test_multiple_saves_ordered_by_time(self, tmp_db):
        """Les enregistrements sont retournés dans l'ordre chronologique."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_lag_history
            save_lag("dev", "my-group", "orders", 100,  "OK")
            save_lag("dev", "my-group", "orders", 500,  "OK")
            save_lag("dev", "my-group", "orders", 1200, "WARNING")
            records = get_lag_history("dev", "my-group", "orders", last_hours=1)

        assert len(records) == 3
        assert records[0]["total_lag"] == 100
        assert records[1]["total_lag"] == 500
        assert records[2]["total_lag"] == 1200

    def test_get_latest_per_group(self, tmp_db):
        """get_latest_per_group retourne la dernière valeur par groupe."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_latest_per_group
            save_lag("dev", "group-a", "orders", 100, "OK")
            save_lag("dev", "group-a", "orders", 200, "OK")
            save_lag("dev", "group-b", "clicks", 500, "OK")
            rows = get_latest_per_group()

        assert len(rows) == 2
        lags = {r["group_id"]: r["total_lag"] for r in rows}
        assert lags["group-a"] == 200   # dernière valeur
        assert lags["group-b"] == 500

    def test_purge_removes_old_records(self, tmp_db):
        """purge_old_records supprime les entrées trop anciennes."""
        import sqlite3
        from datetime import datetime, timedelta

        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        new_ts = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute(
                "INSERT INTO lag_history (cluster_name, group_id, topic, total_lag, status, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("dev", "old-group", "orders", 100, "OK", old_ts)
            )
            conn.execute(
                "INSERT INTO lag_history (cluster_name, group_id, topic, total_lag, status, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("dev", "new-group", "orders", 200, "OK", new_ts)
            )
            conn.commit()

        with patch("core.db.DB_PATH", tmp_db):
            with patch("core.db.CONFIG", {"monitor": {"history_retention_days": 7}}):
                from core.db import purge_old_records
                purge_old_records()

        # On vérifie directement dans la base que l'ancien enregistrement est supprimé
        with sqlite3.connect(str(tmp_db)) as conn:
            remaining = conn.execute(
                "SELECT group_id FROM lag_history"
            ).fetchall()

        assert len(remaining) == 1
        assert remaining[0][0] == "new-group"

    def test_filter_by_cluster(self, tmp_db):
        """get_latest_per_group filtre bien par cluster."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_latest_per_group
            save_lag("dev",  "group-a", "orders", 100, "OK")
            save_lag("prod", "group-a", "orders", 999, "OK")
            rows = get_latest_per_group(cluster_name="dev")

        assert len(rows) == 1
        assert rows[0]["total_lag"] == 100

    def test_lag_history_migration_extends_schema_idempotently(self, tmp_db):
        """la migration ajoute les colonnes demandées et reste idempotente."""
        import sqlite3
        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute("DROP TABLE IF EXISTS lag_history")
            conn.execute(
                """
                CREATE TABLE lag_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cluster_name TEXT NOT NULL DEFAULT 'default',
                    group_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    total_lag INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    group_state TEXT NOT NULL DEFAULT 'Unknown',
                    recorded_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO lag_history (cluster_name, group_id, topic, total_lag, status, recorded_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("dev", "group-a", "orders", 42, "OK", "2024-01-01T00:00:00+00:00")
            )
            conn.commit()

        with patch("core.db.DB_PATH", tmp_db):
            from core.db import init_db
            init_db()
            init_db()

        with sqlite3.connect(str(tmp_db)) as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(lag_history)")]
            assert "log_end_offset" in columns
            assert "committed_offset" in columns
            assert "partition_count" in columns
            assert "partitions_counted" in columns
            assert "run_id" in columns

            row = conn.execute(
                "SELECT log_end_offset, committed_offset, partition_count, partitions_counted, run_id FROM lag_history WHERE group_id=?",
                ("group-a",)
            ).fetchone()
            assert row[0] is None and row[1] is None and row[2] is None and row[3] is None and row[4] is None

    def test_save_lag_records_offsets_and_invariant(self, tmp_db):
        """les offsets enregistrés doivent respecter l'invariant total_lag = LEO - committed."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_lag_history
            save_lag(
                "dev", "my-group", "orders", 250, "WARNING",
                log_end_offset=300,
                committed_offset=50,
                partition_count=2,
                partitions_counted=2,
                run_id="bench-s1"
            )
            row = get_lag_history("dev", "my-group", "orders", last_hours=1)[0]

        assert row["total_lag"] == 250
        assert row["log_end_offset"] == 300
        assert row["committed_offset"] == 50
        assert row["partition_count"] == 2
        assert row["partitions_counted"] == 2
        assert row["run_id"] == "bench-s1"
        assert row["total_lag"] == row["log_end_offset"] - row["committed_offset"]

    def test_run_id_defaults_to_null(self, tmp_db):
        """si KHM_RUN_ID est absent, la colonne run_id est NULL."""
        with patch("core.db.DB_PATH", tmp_db):
            from core.db import save_lag, get_lag_history
            save_lag("dev", "group-a", "orders", 10, "OK")
            row = get_lag_history("dev", "group-a", "orders", last_hours=1)[0]

        assert row["run_id"] is None