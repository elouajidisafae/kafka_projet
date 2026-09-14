# Changelog

## Unreleased

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