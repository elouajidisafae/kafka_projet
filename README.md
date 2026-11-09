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
| `recommendations.confidence_gate` | [HIGH, MEDIUM] | Fit quality levels for scale/topology advice; LOW is excluded |
| `recommendations.rebalance_cycles_threshold` | 3 | Consecutive rebalancing observations before advice |
| `retention.days` | 7 | History retention; overrides `monitor.history_retention_days` |
| `retention.prune_every_cycles` / `prune_batch_size` | 60 / 5000 | Cleanup frequency and maximum rows per transaction |

Scaling advice requires rising lag, a positive critical ETA and spare partitions. Topology advice flags consumers at partition capacity. Unknown membership suppresses both; other rules cover stranded messages, stalled processing and persistent rebalancing.

Multiwindow advice also requires a rising short-window trend. Empty/dead groups receive restart advice instead of scaling advice. Interval ranges are estimates: held-out coverage was below the nominal 90%; an open upper bound appears as “or later.” Baseline remains the default. To enable responsive mode, set `forecast.method: multiwindow`; its defaults are 5 minutes / 0.5. The table and CLI always show crossing ranges; `forecast.show_range: true` also enables responsive advice text and the dashboard band. In an exploratory wave-two comparison (10 repetitions per pattern), responsive V2 reduced median post-burst 0-15-minute absolute ETA error from 345.1 to 28.2 seconds, but increased the median flat-high false-advice cycle fraction from 5.84% to 18.57%. All 10 repetitions improved on the former and worsened on the latter. These synthetic, exploratory results support a trade-off, not a general accuracy claim.

Health Score is `round(100 × (1 − penalty / 75))`, using rounded penalties weighted 60 for critical pairs, 25 for warning pairs and 15 for EMPTY/DEAD pairs. Weights are fixed. Bands are Excellent (90+), Good (70+), Degraded (50+), Poor (30+) and Critical (below 30). Configuration audit events retain submitted values and a timestamp, not old values; configuration editing is available through the web interface, not the CLI.

`/metrics` exposes Health Score, collection/forecast duration, monitored pairs, completed cycles and collection overruns. Cycles never overlap. Retention covers lag, forecasts and recommendation matches, including those hidden by display limits.

Docker stores history at `/app/data/lag_history.db` on the `khm-data` named volume. Keep the same Compose project name to reuse it. `docker compose down` preserves the volume; `docker compose down -v` deletes it.

Outside Docker, history defaults to `lag_history.db` in the project root. Set `KHM_DB_PATH` to override it; changing the path does not migrate existing data.

## Controlled evaluation

The statistics timeline sums the latest observed lag per group/topic pair in five-minute intervals. Values carry forward within the selected history window; timestamps identify actual observations, not repeated-sample totals.

The [benchmark guide](bench/README.md) is optional reproduction material covering isolated workloads, recording, checksummed datasets and offline replay. The benchmark dashboard uses **http://localhost:18080**, with a separate broker and data volumes.

Forecasts are linear extrapolations. Evaluation reports prediction errors and false warnings; passing recording and replay checks does not establish predictive accuracy.

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
```

## License

MIT License.

## Forecast ranges and fit quality

The forecast table and `python main.py --mode cli forecast` show the CRITICAL
empirically calibrated crossing range in both default and responsive modes, with
the point estimate retained alongside it. Predictions beyond 30 minutes show
"more than 30 min" instead of a range. The formula-based prediction bands remain
available through explicitly named API fields; their nominal 90% level does not
guarantee 90% crossing-time coverage. See
[archived range evidence](docs/forecast-range-evidence.md).

**Fit quality** (HIGH, MEDIUM, LOW) describes R2 fit to recent observations,
not the reliability of a projection. The API retains the `confidence` field
and the configuration retains `recommendations.confidence_gate` for compatibility.
`/api/forecast` adds `eta_warning_low_sec`, `eta_warning_high_sec`,
`eta_critical_low_sec` and `eta_critical_high_sec`; null means no bound is
available under the v2 calibration policy. Existing estimates and responsive
`eta_low_sec` / `eta_high_sec` fields remain available.

## Use KHM with an existing Prometheus

Add a scrape job to your existing Prometheus configuration (replace the target
with KHM's address reachable from Prometheus), and load the example rules:

```yaml
scrape_configs:
  - job_name: khm
    scrape_interval: 5s
    static_configs:
      - targets: ['khm:8080']
rule_files:
  - /etc/prometheus/prometheus-khm-alerts.yml
```

Copy [docs/prometheus-khm-alerts.yml](docs/prometheus-khm-alerts.yml) to that
rule-file path and reload your configuration. The example alerts on KHM's active
advice and carries `cluster`, `group`, `topic`, `rule` and `priority` labels.
Rule identifiers are `scale_consumers`, `review_partitions`, `stranded_messages`,
`stalled_processing` and `stuck_rebalance`. These are the existing displayed
recommendations after KHM's caps; the rules are not reimplemented in PromQL.
Inactive advice disappears from `khm_recommendation_active`.

`khm_forecast_critical_eta_seconds` and
`khm_forecast_critical_eta_low_seconds` / `_high_seconds` expose the point and
range in seconds when available. Missing bounds and unavailable projections
have no sample; negative point-estimate sentinel values are not exported.
Stale, ERROR and UNKNOWN pairs have no advice/forecast samples.
No Alertmanager is included: route the example alert through your existing setup.

## Recapture the bottleneck forecast (Fig. 2)

From the repository root in PowerShell:

```powershell
.\.venv-batch2\Scripts\python.exe bench/capture_forecast_demo.py --method baseline
```

Allow 5-10 minutes including Docker build/startup. This illustrative real-Kafka
scenario uses three partitions, one consumer limited to 10 messages/s and a
producer targeting 60 messages/s. It uses the existing demo stack on ports 9092
and 8080, stops its bundled demo workloads, and writes a fresh config, snapshots,
manifest and `figure-forecast.png` under a timestamped `artifacts/forecast-demo-*`
folder. It leaves the demo services available after capture. Playwright and
Microsoft Edge must be installed in the benchmark environment. Use
`--method multiwindow` for a responsive-mode capture.

The original archived `bench/results/capture/figure-forecast.png` was produced
by `bench/capture.py` at commit `88788aa57b5bbd6d698708fc2047e09748c8427d`, as
recorded by its capture manifest. That historical script required the matching
frozen benchmark runs; the new demo command does not overwrite the archive.


Crossing ranges use v2 empirical factors frozen in `bench/range_calibration_v2.yml`
and copied to packaged `core/range_calibration_v2.yml`. Selection uses first-half
inner repetitions only: OLS retains horizon-only factors (C0) because no candidate
met all coverage constraints; shipped responsive mode uses horizon × fit quality
× selected-fit history length (C3). Groups with fewer than 200 training forecasts
fall back to horizon-only factors. Only the shipped 5-minute / 0.5 responsive rule
has calibrated factors. Missing calibration yields no range.

Predicted crossings beyond 30 minutes show **more than 30 min**, with null API
range bounds and no low/high Prometheus samples; the point estimate remains intact.
The dashboard tooltip reads pooled coverage for displayed 0–30 minute ranges from
packaged `core/range_calibration_v2_results.json`, a provenance-tagged extract of
`bench/results/baseline-20261001T111043363413Z/range-calibration-v2/evaluation.json`.
Coverage is forecast-weighted on separate repetitions, not a guarantee for other
workloads. Responsive ranges are narrower with lower measured coverage; OLS retains
its original widths within 30 minutes under the specified selection fallback.

CLI, dashboard, API and Prometheus use v2 CRITICAL bounds. API
`eta_critical_formula_low_sec` / `eta_critical_formula_high_sec` retain the old
formula for transparency. WARNING calibration is unavailable; its formula is
exposed with analogous names. Formula-based recommendation wording and rules
are preserved. Restart the process after updating packaged calibration files.
