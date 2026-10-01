# Changelog

## Unreleased

### Controlled forecast evaluation

- Record recommendation matches before display caps, including display status and run tags.
- Reuse Kafka clients and schedule non-overlapping collection cycles with overrun counters.
- Add an isolated workload stack, checksummed dataset export and exact-input offline replay.
- Score forecast errors, advice lead time and negative controls per repetition.


### Collection performance

- Fetch forecast history in bulk and reuse forecasts within a collection cycle.
- Index SQLite history, enable WAL, and prune lag and forecast history in bounded transactions at a configurable cadence.
- Expose per-cluster phase timings and monitored-pair counts as Prometheus gauges.
- Protect forecast and recommendation outputs with fixed regression snapshots.

### Consumer recommendations and deployment

- Persist Docker history in the named data volume via `KHM_DB_PATH`.
- Collect per-topic consumer membership, distinguish unknown counts, and expose topology in stored snapshots.
- Persist consecutive group-state streaks across collection processes.
- Add confidence-gated scaling, topology, stranded-message, stalled-processing, and stuck-rebalance recommendations with structured metrics and configurable caps.
- Render recommendation priorities and metrics in CLI status and the dashboard.
- Configure forecast windows and reconcile the critical threshold to 10000.
- Add MIT licensing and citation metadata; align dependency support with Python 3.11-3.13.

### Lag history and forecasting

- Added persisted lag offsets and forecast history.
- Added UTC-safe forecasting and migration support.
- Added run tagging, trend deadband configuration, and invalid-offset exclusion.