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
