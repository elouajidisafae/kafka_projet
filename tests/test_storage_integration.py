import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
import pytest
from core import db
from core.config_loader import load_config
from core.forecasting import forecast_lag


@pytest.fixture
def database(tmp_path, monkeypatch):
    path = tmp_path / "new" / "nested" / "history.db"
    monkeypatch.setattr(db, "DB_PATH", path)
    db.init_db()
    return path


def test_parent_creation_and_env_path(database, tmp_path):
    assert database.exists()
    path = tmp_path / "subprocess" / "history.db"
    code = "from core.db import init_db, DB_PATH; init_db(); print(DB_PATH)"
    env = dict(os.environ, KHM_DB_PATH=str(path))
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True)
    assert path.exists()
    assert str(path) in result.stdout
    env.pop("KHM_DB_PATH")
    result = subprocess.run([sys.executable, "-c", "from core.db import DB_PATH; print(DB_PATH)"],
                            env=env, capture_output=True, text=True, check=True)
    assert Path(result.stdout.strip()).resolve() == Path(db.__file__).resolve().parent.parent / "lag_history.db"


def test_latest_topology_columns(database):
    db.save_lag("dev", "g", "orders", 5, "OK", "ConsumerGroupState.Stable",
                partition_count=3, partitions_counted=2, consumer_count=1)
    for rows in [db.get_latest_per_group(), db.get_latest_per_group("dev")]:
        assert len(rows) == 1
        row = rows[0]
        assert (row["partition_count"], row["partitions_counted"], row["consumer_count"]) == (3, 2, 1)
        assert row["group_state"] == "STABLE"
    db.save_lag("dev", "g", "orders", 6, "OK", "PreparingRebalance")
    assert db.get_latest_per_group()[0]["consumer_count"] is None
    assert db.get_latest_per_group()[0]["group_state"] == "PREPARING_REBALANCE"
    db.init_db()
    db.init_db()
    assert len(db.get_lag_history("dev", "g", "orders")) == 2


def test_streak_persists_and_resets(database):
    db.update_group_state_streak("dev", "g", "PreparingRebalance")
    db.update_group_state_streak("dev", "g", "PREPARING_REBALANCE")
    assert db.get_group_state_streak("dev", "g")["streak_count"] == 2
    subprocess.run([sys.executable, "-c", "from core.db import update_group_state_streak; update_group_state_streak('dev','g','PREPARING_REBALANCE')"],
                   env=dict(os.environ, KHM_DB_PATH=str(database)), check=True)
    assert db.get_group_state_streak("dev", "g")["streak_count"] == 3
    db.update_group_state_streak("dev", "g", "CompletingRebalance")
    assert db.get_group_state_streak("dev", "g")["streak_count"] == 1
    db.update_group_state_streak("prod", "g", "STABLE")
    assert db.get_group_state_streak("dev", "g")["state"] == "COMPLETING_REBALANCE"


def test_collection_counts_once_per_group(database, monkeypatch):
    from core import lag_calculator as lag
    from core.kafka_client import GroupDescription, GroupMember
    monkeypatch.setattr(lag, "CONFIG", {"clusters": [{"name": "dev", "bootstrap_servers": "kafka"}],
                                       "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}})
    monkeypatch.setattr(lag, "describe_groups", lambda _: {"g": GroupDescription("g", "STABLE", [GroupMember("m", "c", "h", {"orders": [0]})])})
    monkeypatch.setattr(lag, "get_topics", lambda _: ["orders", "logs", "clicks"])
    monkeypatch.setattr(lag, "compute_lag_for_group", lambda c, b, g, t: lag.ConsumerGroupStatus(c, g, t))
    rows = lag.compute_all_lags()
    assert db.get_group_state_streak("dev", "g")["streak_count"] == 1
    assert {r.topic: r.consumer_count for r in rows} == {"orders": 1, "logs": 0, "clicks": 0}
    lag.compute_all_lags()
    assert db.get_group_state_streak("dev", "g")["streak_count"] == 2


@pytest.mark.parametrize("config,override,expected", [({}, None, 1), ({"window_hours": 2}, None, 2), ({"window_hours": 2}, 0.5, 0.5)])
def test_forecast_window(config, override, expected):
    with patch("core.forecasting.CONFIG", {"alerts": {"warning_threshold": 1000, "critical_threshold": 10000}, "forecast": config}), patch("core.forecasting.get_lag_history", return_value=[]) as query:
        forecast = forecast_lag("dev", "g", "orders", window_hours=override)
        query.assert_called_once_with("dev", "g", "orders", last_hours=expected)
        assert forecast["reason"] == "Only 0 points (minimum 5)"


def test_new_defaults(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yml"
    config_path.write_text("alerts:\n  warning_threshold: 500\n")
    monkeypatch.setattr("core.config_loader.CONFIG_PATH", config_path)
    config = load_config()
    assert config["forecast"] == {"persist_every_cycle": True, "trend_deadband_msgs_per_sec": 0.05, "window_hours": 1,
                                  "method": "baseline", "short_window_minutes": 5, "agreement_tolerance": .5, "interval_level": .90, "show_range": False}
    assert config["recommendations"] == {"max_per_cluster": 10, "max_per_pair": 2, "rebalance_cycles_threshold": 3, "confidence_gate": ["HIGH", "MEDIUM"]}
