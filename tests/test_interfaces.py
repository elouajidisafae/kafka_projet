import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient
from rich.console import Console
from io import StringIO

from core import db
from core.lag_calculator import ConsumerGroupStatus, PartitionLag
from interfaces import cli as cli_module
from interfaces import web


def test_cli_status_persists_topology_and_renders_recommendations(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "history.db")
    row = ConsumerGroupStatus("dev", "g", "orders", [PartitionLag(0, 1600, 100, 1500)],
                              group_state="EMPTY", consumer_count=0, partition_count=3)
    monkeypatch.setattr(cli_module, "compute_all_lags", lambda: [row])
    output = StringIO()
    monkeypatch.setattr(cli_module, "console", Console(file=output, width=180, color_system=None))
    result = CliRunner().invoke(cli_module.cli, ["status", "--cluster", "dev"])
    assert result.exit_code == 0, result.exception
    saved = db.get_latest_per_group()[0]
    assert saved["consumer_count"] == 0 and saved["partition_count"] == 3
    assert saved["group_state"] == "EMPTY"
    rendered = output.getvalue()
    assert "HIGH" in rendered and "Stranded messages" in rendered
    assert "1500 messages pending" in rendered and "Metrics:" in rendered
    assert "dev/g/orders" in rendered


def test_web_recommendation_api_uses_new_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "history.db")
    db.init_db()
    db.save_lag("dev", "g", "orders", 1500, "WARNING", "EMPTY", consumer_count=0, partition_count=3)
    # No context manager: do not launch the real background Kafka collector.
    response = TestClient(web.app).get("/api/recommendations")
    assert response.status_code == 200
    rec = response.json()["recommendations"][0]
    assert rec["priority"] == "HIGH" and rec["cluster_name"] == "dev"
    assert rec["metrics"] == {"current_lag": 1500, "group_state": "EMPTY"}
    assert "severity" not in rec


def test_error_is_live_but_never_history(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "errors.db")
    db.init_db()
    row = ConsumerGroupStatus("dev", "g", "t", status="ERROR")
    monkeypatch.setattr(cli_module, "compute_all_lags", lambda: [row])
    monkeypatch.setattr("core.forecasting.forecast_all", lambda: None)
    monkeypatch.setattr("core.recommender.get_recommendations", lambda **kw: [])
    assert cli_module._collect_and_save()[0].status == "ERROR"
    db.save_lag("dev", "g", "t", 0, "ERROR")
    assert db.get_lag_history("dev", "g", "t") == []
    from dataclasses import asdict
    monkeypatch.setattr(web, "_current_rows", [asdict(row)])
    client = TestClient(web.app)
    assert client.get("/api/status").json()["data"][0]["status"] == "ERROR"
    assert 'kafka_consumer_status{cluster="dev",group="g",topic="t"} -1' in client.get("/metrics").text
    output = StringIO()
    Console(file=output, width=180).print(cli_module._build_table([row]))
    assert "ERROR" in output.getvalue()


def test_unknown_partitions_visible(monkeypatch):
    from dataclasses import asdict
    row = ConsumerGroupStatus("dev", "g", "t", [PartitionLag(0, 100, 90, 10)], partition_count=3)
    assert row.partitions_without_commit == 2
    monkeypatch.setattr(web, "_current_rows", [asdict(row)])
    client = TestClient(web.app)
    assert client.get("/api/status").json()["data"][0]["partitions_without_commit"] == 2
    assert 'kafka_consumer_partitions_without_commit{cluster="dev",group="g",topic="t"} 2' in client.get("/metrics").text
    output = StringIO()
    Console(file=output, width=180).print(cli_module._build_table([row]))
    assert "No commit" in output.getvalue()


@pytest.mark.parametrize("warm_start", [False, True])
def test_cluster_outage_serves_stored_snapshot_then_recovers(tmp_path, monkeypatch, warm_start):
    from core import lag_calculator as lag
    from core.kafka_client import GroupDescription
    from unittest.mock import Mock
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "outage.db")
    db.init_db()
    config = dict(lag.CONFIG, clusters=[{"name": "dev", "bootstrap_servers": "kafka"}])
    monkeypatch.setattr(lag, "CONFIG", config)
    monkeypatch.setattr(web, "CONFIG", config)
    monkeypatch.setattr(web, "_current_rows", None)
    monkeypatch.setattr(web, "_cluster_statuses", {})
    monkeypatch.setattr(lag, "describe_groups", lambda *a, **kw: {"g": GroupDescription("g", "EMPTY", [])})
    topics = Mock(return_value=["t"])
    monkeypatch.setattr(lag, "get_topics", topics)
    row = ConsumerGroupStatus("dev", "g", "t", [PartitionLag(0, 20, 10, 10)])
    monkeypatch.setattr(lag, "compute_lag_for_group", lambda *a: row)
    db.save_lag("dev", "g", "t", 10, "OK", partition_count=1, partitions_counted=1)
    stored_time = db.get_latest_per_group()[0]["recorded_at"]
    if warm_start:
        web._publish_collection(lag.compute_all_lags())
    topics.side_effect = RuntimeError("broker unavailable")
    web._publish_collection(lag.compute_all_lags())
    client = TestClient(web.app)
    payload = client.get("/api/status").json()
    state = payload["cluster_status"]["dev"]
    assert state["status"] == "UNREACHABLE" and state["reachable"] is False
    assert state["last_successful_collection"] is not None
    assert payload["data"][0]["total_lag"] == 10
    assert payload["data"][0]["stale"] is True
    assert payload["data"][0]["recorded_at"] == stored_time
    metrics = client.get("/metrics").text
    assert 'khm_cluster_reachable{cluster="dev"} 0' in metrics
    assert 'kafka_consumer_snapshot_stale{cluster="dev",group="g",topic="t"} 1' in metrics
    assert 'kafka_consumer_last_success_timestamp_seconds{' in metrics
    assert 'kafka_consumer_lag{cluster="dev",group="g",topic="t"} 10' in metrics
    assert len(db.get_lag_history("dev", "g", "t")) == 1
    topics.side_effect = None
    row = ConsumerGroupStatus("dev", "g", "t", [PartitionLag(0, 30, 10, 20)])
    web._publish_collection(lag.compute_all_lags())
    payload = client.get("/api/status").json()
    assert payload["cluster_status"]["dev"]["reachable"] is True
    assert payload["data"][0]["stale"] is False
    assert payload["data"][0]["total_lag"] == 20
    metrics = client.get("/metrics").text
    assert 'khm_cluster_reachable{cluster="dev"} 1' in metrics
    assert 'kafka_consumer_snapshot_stale{cluster="dev",group="g",topic="t"} 0' in metrics
    from pathlib import Path
    script = Path("static/js/dashboard.js").read_text(encoding="utf-8")
    assert "UNREACHABLE" in script and "STALE" in script and "last_successful_collection" in script



def test_unknown_status_on_all_interfaces_without_false_lag(tmp_path, monkeypatch):
    from dataclasses import asdict
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "unknown.db")
    db.init_db()
    unknown = ConsumerGroupStatus("demo", "new", "orders", partition_count=3, partitions_counted=0)
    monkeypatch.setattr(web, "_current_rows", [asdict(unknown)])
    client = TestClient(web.app)
    assert client.get("/api/status").json()["data"][0]["status"] == "UNKNOWN"
    metrics = client.get("/metrics").text
    assert 'kafka_consumer_status{cluster="demo",group="new",topic="orders"} -2' in metrics
    assert 'kafka_consumer_lag{cluster="demo",group="new",topic="orders"}' not in metrics
    db.save_lag("demo", "new", "orders", 0, "UNKNOWN")
    assert db.get_lag_history("demo", "new", "orders") == []
    output = StringIO()
    Console(file=output, width=180).print(cli_module._build_table([unknown]))
    assert "UNKNOWN" in output.getvalue() and "N/A" in output.getvalue()


def test_health_metrics_and_active_alert_dialog(monkeypatch):
    from core.health_score import compute_health_score
    row = ConsumerGroupStatus("demo", "payments", "orders", [PartitionLag(0, 12000, 0, 12000)], group_state="EMPTY")
    summary = compute_health_score([row], {"offline": {"reachable": False}})
    monkeypatch.setattr(web, "_last_health_score", summary)
    monkeypatch.setattr(web, "_current_rows", [])
    client = TestClient(web.app)
    response = client.get("/api/health-score").json()
    assert response["grade"] == "Critical"
    assert response["active_alert_count"] == len(response["active_alerts"]) == 3
    output = client.get("/metrics").text
    expected = {"worst_state": 2, "critical_pairs": 1, "warning_pairs": 0,
                "stopped_pairs": 1, "waiting_messages": 12000, "unreachable_clusters": 1,
                "error_pairs": 0, "unknown_pairs": 0}
    for field, value in expected.items():
        assert f"# TYPE khm_health_{field} gauge" in output
        assert f"khm_health_{field} {value}\n" in output
    assert 'khm_health_unreachable_clusters{cluster="offline"} 1' in output
    assert 'khm_health_worst_state{cluster="demo"} 2' in output
    assert 'khm_health_score{cluster="demo"} 0' in output
    page = client.get("/").text
    assert 'id="active-alert-total"' in page and 'id="active-alert-dialog"' in page



def test_no_data_omits_global_and_cluster_score_metrics(monkeypatch):
    from core.health_score import compute_health_score
    monkeypatch.setattr(web, "_last_health_score", compute_health_score([], {"offline": {"reachable": False}}))
    monkeypatch.setattr(web, "_current_rows", [])
    client = TestClient(web.app)
    assert client.get("/api/health-score").json()["score"] is None
    lines = client.get("/metrics").text.splitlines()
    assert not any(line.startswith(("khm_health_score ", "khm_health_score{")) for line in lines)
    assert 'khm_health_unreachable_clusters 1' in lines
