"""Exact regression outputs against a fixed synthetic SQLite history."""
import builtins
import json
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core import db, forecasting, recommender

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 1, 15, 12, tzinfo=timezone.utc)
SETTINGS = {
    "alerts": {"warning_threshold": 1000, "critical_threshold": 10000},
    "forecast": {"window_hours": 1, "persist_every_cycle": False,
                 "trend_deadband_msgs_per_sec": 0.05},
    "monitor": {"refresh_interval": 5, "history_retention_days": 7},
    "recommendations": {"max_per_pair": 2, "max_per_cluster": 10,
                        "confidence_gate": ["HIGH", "MEDIUM"],
                        "rebalance_cycles_threshold": 3},
    "run_id": None,
}


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)


def canonical(value):
    # Keep array order: it is part of the externally visible contract.
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False,
                      allow_nan=False) + "\n"


@pytest.fixture
def golden_database(tmp_path, monkeypatch):
    target = tmp_path / "golden.db"
    shutil.copyfile(FIXTURES / "golden.db", target)
    monkeypatch.setattr(db, "DB_PATH", target)
    monkeypatch.setattr(db, "datetime", FixedDatetime)
    for module in (db, forecasting, recommender):
        monkeypatch.setattr(module, "CONFIG", deepcopy(SETTINGS))
    # Use a private copy so migrations and writes cannot alter the reference.
    db.init_db()
    return target


@pytest.mark.parametrize("name,call", [
    ("forecasts", forecasting.forecast_all),
    ("recommendations", recommender.get_recommendations),
])
def test_golden_output(golden_database, name, call):
    expected = (FIXTURES / "golden_expected" / f"{name}.json").read_text(encoding="utf-8")
    assert canonical(call()) == expected


def test_golden_detects_rounding_change(golden_database, monkeypatch):
    expected = (FIXTURES / "golden_expected" / "forecasts.json").read_text(encoding="utf-8")
    original_round = builtins.round

    def changed_round(number, ndigits=None):
        return original_round(number, 0 if ndigits == 4 else ndigits)

    # Temporarily perturb the actual forecasting path, never the snapshot.
    monkeypatch.setattr(forecasting, "round", changed_round, raising=False)
    with pytest.raises(AssertionError):
        assert canonical(forecasting.forecast_all()) == expected
