# Kafka Health Monitor (KHM)

A monitoring tool for Apache Kafka consumer groups. KHM measures lag, extrapolates linear trends, and generates operational recommendations using group state and per-topic consumer membership.

---

## 🌟 Key Features

| Feature | CLI | Web |
|---|:---:|:---:|
| **Real-time Lag Monitoring** | ✅ | ✅ |
| **Audit Trail (Governance)** | — | ✅ |
| **Proactive Recommendations** | Yes | Yes |
| **Midnight Blue Design (Dark/Light)** | — | ✅ |
| **Linear Trend Extrapolation** | — | ✅ |
| **Global Health Score (A-E)** | — | ✅ |
| **Export Data (CSV/JSON)** | — | ✅ |
| **Multi-cluster Support** | ✅ | ✅ |
| **Prometheus /metrics Export** | — | ✅ |

---

## 🚀 Quick Start

### One command (Docker)
```bash
docker compose up -d
```
Open **http://localhost:8080** to access the dashboard. The supplied Compose stack includes Kafka and demo workloads. The `dev`, `staging`, and `production` entries in the sample configuration refer to the same broker; configure distinct brokers to monitor independent clusters.

### Local Development
```bash
pip install -r requirements.txt
python main.py --mode web
```

### Configuration

The `forecast` block supports:

- `persist_every_cycle` (default `true`): persist forecast results after each monitoring cycle. Forecast API requests also persist results when enabled.
- `trend_deadband_msgs_per_sec` (default `0.05`): slopes within this deadband are stable.
- `window_hours` (default `1`): history window for linear trend extrapolation; `forecast_lag(..., window_hours=2)` overrides it for one call.

The `alerts` block controls the warning and critical lag thresholds (defaults: `1000` and `10000`).

```yaml
recommendations:
  max_per_cluster: 10
  max_per_pair: 2
  rebalance_cycles_threshold: 3
  confidence_gate: [HIGH, MEDIUM]
```

- `max_per_cluster`: maximum items per cluster, except HIGH priority items are never dropped by this cap.
- `max_per_pair`: maximum recommendations for each cluster/group/topic pair, after sorting by priority and rule order.
- `rebalance_cycles_threshold`: consecutive observations in the same rebalancing state before advice fires. A state change resets the streak to one.
- `confidence_gate`: admitted forecast confidence levels for scaling and topology advice (HIGH and MEDIUM by default; LOW is never admitted).

Missing keys receive these defaults from `core/config_loader.py`. Consumer count means members assigned at least one partition of the topic. Rebalancing, unavailable assignments, and failed descriptions produce unknown counts and suppress scaling/topology advice. Empty and dead groups have zero consumers.

Recommendations contain `priority`, `cluster_name`, and a `metrics` dictionary. Scaling requires spare partitions and a positive ETA to the critical threshold. Topology advice requires consumers at or above the partition count. Stranded messages, stalled processing, and persistent rebalance rules are evaluated independently. Multiple recommendations may appear for one pair.

### Database persistence

Docker sets `KHM_DB_PATH=/app/data/lag_history.db`; `/app/data` is mounted on the `khm-data` named volume. Keep the same Compose project name when recreating containers to retain that volume. `docker compose down` preserves it; `docker compose down -v` deletes it.

Outside Docker, the default is `lag_history.db` in the project root. Set `KHM_DB_PATH` to override it. Missing parent directories are created automatically. Changing the database path does not copy an existing database to the new location.

### CLI

```bash
python main.py --mode cli status
python main.py --mode cli status --cluster dev
```

The status command collects observations and prints recommendations with their triggering metrics. The equivalent installed command is `khm status` after `pip install -e .`.

### Supported Python versions

Supported Python range: **3.11-3.13**.

Install development dependencies and run the automated tests:

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q
```
NumPy uses `>=2.1.3,<2.2`; Kafka and PyYAML pins provide Python 3.13 wheels.

### Reproducing the slow-consumer scenario

Start Kafka, create `orders` and `logs` with three partitions each, then start the demo producer, consumers, and monitor. A single slow consumer then has spare topic partitions, allowing the `scale` rule to fire once a rising trend has sufficient confidence and a positive critical ETA. With one partition the same workload produces `topology` instead. Keep the demo running for 45 minutes and capture `/api/recommendations`, the dashboard recommendation panel, and `khm status`. Stop `consumer-slow` once pending messages exceed the warning threshold; after the group becomes EMPTY, `stranded` reports the stored pending count.

---

## 🛠 Advanced Modules

### 1. Audit Trail
The web interface provides a persisted event log through the audit page and `/api/audit`. Audit entries include an event type, severity, message, and timestamp.

### 2. Proactive Recommendations
KHM evaluates five rules and includes the triggering metrics in each result:
- **Scale consumer group**: rising lag, admitted confidence, spare partitions, and a positive critical ETA.
- **Partition count may be limiting**: rising lag with consumers at or above the partition count.
- **Stranded messages**: an EMPTY or DEAD group with lag above the warning threshold.
- **Possible processing bottleneck**: lag at or above the critical threshold with a stable trend and STABLE group.
- **Group rebalance not completing**: the same rebalancing state persists for the configured cycle count.

### 3. Dashboard appearance
The dashboard offers dark and light themes and displays recommendation priorities alongside their triggering metrics.

---

## 📂 Project Structure

```
kafka-health-monitor/
├── core/
│   ├── audit.py             # Event logging & persistence
│   ├── recommender.py       # Smart analysis engine
│   ├── forecasting.py       # Linear regression predictions
│   ├── health_score.py      # Multi-factor health scoring
│   ├── db.py                # SQLite management (History & Audit)
│   └── kafka_client.py      # High-performance offset reader
├── interfaces/
│   ├── web.py               # FastAPI server & background worker
│   └── cli.py               # Terminal UI (Rich)
├── static/
│   ├── css/                 # Theme variables
│   └── js/                  # Real-time charts & exports
└── templates/
    ├── audit.html           # Timeline activity feed
    ├── dashboard.html       # Metrics & health overview
    └── config.html          # Interactive settings
```

---

## 📝 License
Distributed under the MIT License.
### Collection performance and retention

Forecasts use one bulk history query and one fit per pair per collection cycle. Requests reuse a process-local, lock-protected cache for up to `monitor.refresh_interval` seconds; changed configuration invalidates it. Single-pair `forecast_lag()` calls still fetch fresh history and accept a window override. Forecast API requests retain their existing persistence behavior.

SQLite uses WAL mode and `synchronous=NORMAL`, with indexed history lookup and retention. The database and its `-wal` and `-shm` sidecars must share the mounted data directory. NORMAL preserves database consistency but the newest transactions can be lost after power failure.

```yaml
retention:
  days: 7
  prune_every_cycles: 60
  prune_batch_size: 5000
```

Retention applies to both lag and forecast history, in transactions deleting at most `prune_batch_size` rows. Each collection process prunes every `prune_every_cycles` cycles. `retention.days` takes precedence over the legacy `monitor.history_retention_days`; when omitted, the legacy setting supplies the default.

`/metrics` exposes the gauges `khm_collection_duration_seconds`, `khm_forecast_duration_seconds`, and `khm_monitored_pairs`, labeled by cluster. Collection duration measures Kafka collection and state tracking for that cluster; forecast duration measures bulk reads and fitting, with shared query time apportioned by pair count. Neither includes forecast persistence or retention. Pair count comes from the latest stored snapshot. Phase durations and pair counts are also logged at DEBUG level by `core.timing`.

### Controlled recordings and offline evaluation

See [the recording guide](bench/README.md) for the isolated benchmark stack, seeded workloads, immutable dataset export, exact-input replay and per-repetition evaluation. Recommendation matches are persisted before display caps in `recommendation_log`, with a `displayed` flag and run identifier; they follow the configured retention policy.

Collection cycles run serially at the configured period. An overrun starts the next cycle immediately and increments `khm_collection_overruns_total{cluster}` for participating clusters. Kafka clients are reused within collection threads. Forecast and recommendation output dictionaries are unchanged.
