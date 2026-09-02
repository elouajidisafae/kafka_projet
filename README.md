# Kafka Health Monitor (KHM) 2.0

A professional, enterprise-ready monitoring tool for Apache Kafka. KHM provides deep visibility into consumer group health, predictive lag forecasting, and a complete system audit trail.

---

## 🌟 Key Features

| Feature | CLI | Web |
|---|:---:|:---:|
| **Real-time Lag Monitoring** | ✅ | ✅ |
| **Audit Trail (Governance)** | — | ✅ |
| **Proactive Recommendations** | — | ✅ |
| **Midnight Blue Design (Dark/Light)** | — | ✅ |
| **Predictive Forecasting (AI-based)** | — | ✅ |
| **Global Health Score (A-E)** | — | ✅ |
| **Export Data (CSV/JSON)** | — | ✅ |
| **Multi-cluster Support** | ✅ | ✅ |
| **Prometheus /metrics Export** | — | ✅ |

---

## 🚀 Quick Start

### One command (Docker)
```bash
docker-compose up -d
```
Open **http://localhost:8080** to access the dashboard.

### Local Development
```bash
pip install -r requirements.txt
python main.py --mode web
```

### Configuration

The `forecast` block supports:

- `persist_every_cycle`: persist forecast results after each monitoring cycle.
- `trend_deadband_msgs_per_sec`: treat smaller trend slopes as stable.

The `alerts` block controls the warning and critical lag thresholds.

---

## 🛠 Advanced Modules

### 1. System Audit Trail (Enterprise Governance)
KHM 2.0 includes a persistent record-keeping system that logs:
- **Configuration Changes**: Track who changed thresholds and when.
- **Alert Triggers**: Historical record of every WARNING/CRITICAL transition.
- **System Events**: Service starts, data purges, and critical errors.
*All logs are exportable in CSV and JSON for compliance reporting.*

### 2. Proactive Recommendations
Beyond just monitoring, KHM analyzes trends to provide actionable advice:
- "Partition count is too low for current lag."
- "Consumer group shows signs of 'Stuck' state."
- "Growth rate suggests a critical breach in 22 minutes."

### 3. Design System
The interface uses a curated **Midnight Blue & Amber Yellow** theme, optimized for:
- Low eye strain during long monitoring sessions.
- High-contrast visual hierarchy for critical alerts.
- Full responsive support for mobile/tablet NOC views.

---

## 📂 Project Structure

```
kafka-health_monitor/
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
│   ├── css/                 # Professional Theme variables
│   └── js/                  # Real-time charts & exports
└── templates/
    ├── audit.html           # Timeline activity feed
    ├── dashboard.html       # Metrics & health overview
    └── config.html          # Interactive settings
```

---

## 📊 Comparison with Industry Tools

| Feature | KHM 2.0 | Kafdrop | Burrow | kafka-ui |
|---|:---:|:---:|:---:|:---:|
| **Language** | Python | Java | Go | Java |
| **Audit Trail** | ✅ | — | — | — |
| **Recommendations** | ✅ | — | — | — |
| **Predictive Analysis**| ✅ | — | — | — |
| **Health Score** | ✅ | — | — | — |
| **CLI & Web** | ✅ | Web only| CLI only| Web only|
| **Export (CSV/JSON)** | ✅ | — | — | — |

---

## 📝 License
Distributed under the MIT License.