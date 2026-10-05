# Kafka Health Monitor

Monitor Apache Kafka consumer lag, extrapolate linear trends, and generate recommendations from consumer-group state and topic membership. KHM provides a web dashboard, CLI, audit log, SQLite history and Prometheus metrics.

## Quick start

```bash
docker compose up -d
```

Open **http://localhost:8080**. The supplied stack includes Kafka and demo workloads. It configures one cluster named `demo`; configure distinct broker endpoints to monitor independent clusters.

For local development with Python **3.11-3.13**:

```bash
pip install -r requirements.txt
python main.py --mode web
```

To collect status and recommendations from the terminal:

```bash
python main.py --mode cli status
python main.py --mode cli status --cluster demo
```

## Configuration and storage

Set brokers in `config.yml` under `clusters`. Key settings:

| Setting | Default | Purpose |
|---|---|---|
| `alerts.warning_threshold` / `critical_threshold` | 1000 / 10000 | Lag thresholds in messages |
| `monitor.refresh_interval` | 5 | Collection interval in seconds |
| `forecast.window_hours` | 1 | History used for linear extrapolation |
| `forecast.method` | baseline | `baseline` is the default; `multiwindow` enables responsive window selection |
| `forecast.short_window_minutes` / `agreement_tolerance` | 5 / 0.5 | Short-window length and relative slope disagreement needed to select it |
| `forecast.show_range` | false | Opt in to interval ranges in advice and the dashboard band |
| `forecast.interval_level` | 0.90 | Fixed nominal prediction level for multiwindow ranges and bands |
| `forecast.trend_deadband_msgs_per_sec` | 0.05 | Slopes within this range are stable |
| `forecast.persist_every_cycle` | true | Save forecasts; API requests also save cached results |
| `recommendations.max_per_pair` / `max_per_cluster` | 2 / 10 | Display limits; HIGH priority bypasses the cluster limit |
| `recommendations.confidence_gate` | [HIGH, MEDIUM] | Confidence levels for scale/topology advice; LOW is excluded |
| `recommendations.rebalance_cycles_threshold` | 3 | Consecutive rebalancing observations before advice |
| `retention.days` | 7 | History retention; overrides `monitor.history_retention_days` |
| `retention.prune_every_cycles` / `prune_batch_size` | 60 / 5000 | Cleanup frequency and maximum rows per transaction |

Scaling advice requires rising lag, a positive critical ETA and spare partitions. Topology advice flags consumers at partition capacity. Unknown membership suppresses both; other rules cover stranded messages, stalled processing and persistent rebalancing.

Multiwindow advice also requires a rising short-window trend. Empty/dead groups receive restart advice instead of scaling advice. Interval ranges are estimates: held-out coverage was below the nominal 90%; an open upper bound appears as “or later.” Baseline remains the default. To enable responsive mode, set `forecast.method: multiwindow`; its defaults are 5 minutes / 0.5. Ranges stay hidden unless `forecast.show_range: true`. In an exploratory wave-two comparison (10 repetitions per pattern), responsive V2 reduced median post-burst 0-15-minute absolute ETA error from 345.1 to 28.2 seconds, but increased the median flat-high false-advice cycle fraction from 5.84% to 18.57%. All 10 repetitions improved on the former and worsened on the latter. These synthetic, exploratory results support a trade-off, not a general accuracy claim.

Health Score is `round(100 × (1 − penalty / 75))`, using rounded penalties weighted 60 for critical pairs, 25 for warning pairs and 15 for EMPTY/DEAD pairs. Weights are fixed. Bands are Excellent (90+), Good (70+), Degraded (50+), Poor (30+) and Critical (below 30). Configuration audit events retain submitted values and a timestamp, not old values; configuration editing is available through the web interface, not the CLI.

`/metrics` exposes Health Score, collection/forecast duration, monitored pairs, completed cycles and collection overruns. Cycles never overlap. Retention covers lag, forecasts and recommendation matches, including those hidden by display limits.

Docker stores history at `/app/data/lag_history.db` on the `khm-data` named volume. Keep the same Compose project name to reuse it. `docker compose down` preserves the volume; `docker compose down -v` deletes it.

Outside Docker, history defaults to `lag_history.db` in the project root. Set `KHM_DB_PATH` to override it; changing the path does not migrate existing data.

## Controlled evaluation

The [benchmark guide](bench/README.md) covers isolated workloads, recording, checksummed datasets and offline replay. The benchmark dashboard uses **http://localhost:18080**, with a separate broker and data volumes.

Forecasts are linear extrapolations. Evaluation reports prediction errors and false warnings; passing recording and replay checks does not establish predictive accuracy.

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
```

## License

MIT License.
