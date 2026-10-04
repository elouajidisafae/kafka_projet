"""
Chargement de la configuration multi-cluster depuis config.yml.
"""
import os
from copy import deepcopy
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "config.yml"

DEFAULTS = {
    "clusters": [
        {"name": "default", "bootstrap_servers": "localhost:9092", "color": "teal"}
    ],
    "monitor": {"refresh_interval": 5, "history_retention_days": 7},
    "retention": {"days": 7, "prune_every_cycles": 60, "prune_batch_size": 5000},
    "alerts": {"warning_threshold": 1000, "critical_threshold": 10000},
    "forecast": {
        "method": "baseline",
        "short_window_minutes": 5,
        "agreement_tolerance": 0.5,
        "interval_level": 0.90,
        "show_range": False,
        "persist_every_cycle": True,
        "trend_deadband_msgs_per_sec": 0.05,
        "window_hours": 1,
    },
    "recommendations": {
        "max_per_cluster": 10,
        "max_per_pair": 2,
        "rebalance_cycles_threshold": 3,
        "confidence_gate": ["HIGH", "MEDIUM"],
    },
    "web": {"host": "0.0.0.0", "port": 8080},
    "exclude_topics": [],
    "exclude_groups": [],
    "run_id": os.getenv("KHM_RUN_ID"),
}


def load_config() -> dict:
    config = deepcopy(DEFAULTS)
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            user_config = yaml.safe_load(f) or {}
        for section, values in user_config.items():
            if isinstance(values, dict) and section in config:
                config[section].update(values)
            else:
                config[section] = values
    if CONFIG_PATH.exists() and "days" not in (user_config.get("retention") or {}):
        config["retention"]["days"] = config["monitor"]["history_retention_days"]
    config["run_id"] = os.getenv("KHM_RUN_ID")
    return config


CONFIG = load_config()
