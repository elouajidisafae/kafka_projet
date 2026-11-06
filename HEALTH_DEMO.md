# Revised Health Score demo and Figure 3

Run from the repository root in PowerShell with Docker Desktop and Microsoft Edge installed.
Dependency installation and the Docker build/capture can exceed five minutes; run them yourself.
This uses the existing demo stack on ports 9092/8080 and stops its demo producer/consumers.
Do not run alongside another demo or capture. Existing benchmark artifacts are untouched.

```powershell
 git switch revision-r1
 docker info
 .\.venv-batch2\Scripts\python.exe -m pip install -r requirements-bench.txt playwright
 .\.venv-batch2\Scripts\python.exe bench/capture_health_demo.py
```

The script builds the current monitor, creates three uniquely named real Kafka topics/groups,
and commits offset zero. Two members remain active without consuming; the critical member
leaves. Backlogs are 12,000 (CRITICAL/EMPTY), 1,500 (WARNING/STABLE), and zero (OK/STABLE).
It waits for score 56, final label Critical, and one stopped pair before capturing.
No forecast or recommendation configuration is changed by this script.

The unchanged equation now uses unrounded penalties 20, 25/3 and 5:
`round(100 * (1 - (20 + 25/3 + 5) / 75)) = 56`.
The numeric band is Degraded; the critical/stopped state sets the final label to Critical.
There are three active conditions: critical lag, warning lag and stopped-group availability.

Outputs are under a new `artifacts/health-demo-<UTC timestamp>/` directory:
`figure-overview.png`, `figure-active-alerts.png`, health/status JSON, config, database and manifest.
These are illustrative live demo captures, not new benchmark performance evidence.
The script closes its members after capture; subsequent live availability counts will change.
It leaves the demo Docker stack running. To stop it afterwards:

```powershell
 docker compose -f docker-compose.yml stop kafka-health-monitor kafka zookeeper
```

## Original capture provenance

`bench/results/capture/manifest.json` records commit
`88788aa57b5bbd6d698708fc2047e09748c8427d` and the original runner `bench/capture.py`
(SHA-256 `721b1e17632f05212447fe6732c317008f373897604a5a662790cd5feed10348`).
That runner is now retained locally as `internal/private-tools/capture.py`.
It produced `figure-overview.png` as part of a three-scenario session.
Its normal mode requires matching full benchmark receipts and targets the old capture
folder, so do not rerun it for this revision. The new focused runner avoids modifying
`bench/data` or `bench/results` and does not require repeating the long benchmarks.
