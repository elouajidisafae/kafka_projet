# Golden regression fixture

`golden.db` contains synthetic data only. It is an intentional exception to the runtime database ignore rule.

`test_golden_output.py` copies the database into a temporary directory, freezes the clock at 2026-01-15 12:00 UTC, and applies fixed configuration. The reference file is never opened for application writes by the test. Histories span three hours and include rising, falling, flat, sparse and expired series, per-topic topology, and stable, empty and rebalancing groups.

`golden_expected/forecasts.json` and `golden_expected/recommendations.json` capture the complete outputs from source commit `2c321f381309dc0572ad2178fb87bbb36644af26`. JSON object keys are canonicalized; list ordering and rounded floating-point values are compared exactly. Forecast persistence is disabled in this output comparison to keep the reference inputs fixed.

Run `python -m pytest tests/test_golden_output.py -q`. The mutation check temporarily changes forecasting precision and verifies that the output comparison rejects it.

Do not regenerate these snapshots merely to make a performance refactor pass. Investigate any difference first. Historical expected outputs remain fixed during behavior-preserving changes.

`multiwindow_expected/` contains separate snapshots for a 15-minute short window, tolerance 1.0 and nominal 90% interval. They add window/interval metadata and chart points; the fixed histories retain their baseline point forecasts, while scale advice includes its range. `tests/test_multiwindow.py` checks these outputs and live/offline equivalence, including a regime change. The original snapshots still exercise the baseline path unchanged.
