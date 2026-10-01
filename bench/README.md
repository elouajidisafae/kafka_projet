# Controlled forecast recording and offline evaluation

This tooling records the existing forecaster without changing its mathematics. It uses a dedicated KRaft broker, ports 19092/18080 and separate benchmark volumes. The development Compose stack is independent. Configuration follows the [Confluent Docker reference](https://docs.confluent.io/platform/7.6/installation/docker/config-reference.html).

Install `requirements-bench.txt` into the project environment. Python 3.11-3.13 is supported. Run commands from the repository root.

## Validation sequence

1. `python bench/run_forecast_validation.py --arm smoke --profile smoke --reps 1 --waves 1 --duration-min 5`
2. `python bench/measure_collection.py` (10, 20 and 40 groups, approximately ten minutes).
3. `python bench/calibrate.py` (45 minutes: crossing targets extend to 40 minutes). Review PASS/FAIL results, tune patterns.yml and repeat calibration if needed.
4. Review and commit the final configuration/tooling. Record the chosen commit/tag yourself.
5. On a quiet host with sleep/restarts disabled: `python bench/run_forecast_validation.py --arm baseline --reps 10 --waves 2`

The baseline command requires a clean tree and the full 60-minute, two-wave design. Do not repeat the baseline to select better outcomes. Smoke/calibration/timing data is excluded from publication eligibility. Forty concurrent repetitions are validated by the real operator timing and recording runs, not inferred from the small smoke test.

Each wave uses one seeded producer and one consumer per topic/group with three partitions. Processed offsets are committed synchronously after each processed batch, with half-second heartbeat commits while idle. Event logs record commit latency, uncommitted processed messages and broker offset verification. Validation receipts include the workload source hash so a workload change requires fresh validation. Rates include independent seeded arrival jitter. Topics/groups begin with bench- and include a run-specific suffix to prevent reuse. Only resources created for the wave are deleted at its end; history remains in the benchmark volume.

The benchmark configuration selects matching group/topic names, avoiding the development collector's all-groups/all-topics Cartesian product. A run-specific group allowlist prevents earlier runs from entering the new recording. The allowlist is fixed before startup and recorded in the immutable configuration snapshot.

## Dataset and integrity

`bench/data/<run-id>/` contains the manifest, complete configuration and patterns, three CSV logs, per-repetition generator JSONL logs, and SHA256SUMS. Completed export uses SQLite's backup API, so committed WAL data is included. Export never overwrites a sealed dataset. Failed/aborted captures are marked and kept for diagnosis.

The optional `forecast_log.input_ids_json` column records the exact input rows used by each fit. This resolves differences between the history query time, last observation time and later persistence time, including cached API forecasts. It changes logging only, not the forecast dictionary or mathematics. Unbounded ETAs beyond SQLite's signed integer range use tagged decimal storage and are decoded losslessly in exported CSV.

The baseline fitter uses the last observation as its ETA origin; it does not read the wall clock. Evaluation follows the specified forecast-log timestamp convention. The small difference between observation and save time is therefore part of measured live error, and is identical in the paired replay.

Verify checksums with `sha256sum -c SHA256SUMS` inside the dataset directory, or use `bench.common.verify()` from Python. Derived results are written outside the dataset, which is never modified after sealing. Local datasets/results are ignored by Git and Docker; curate the sealed dataset for the publication archive separately.

## Offline replay and scoring

```text
python bench/replay.py --dataset bench/data/<run-id> --forecaster baseline
python bench/forecast_eval.py --dataset bench/data/<run-id> --forecaster baseline
python bench/forecast_eval.py --dataset bench/data/<run-id> --forecaster baseline --source replay
```

Replay requires no running Kafka or live database. `fidelity.json` compares exact slope, intercept, R-squared and critical ETA, plus pre-cap scale/topology matches for every logged forecast. It lists all mismatches; unexplained mismatches fail validation even if the numerical fraction exceeds 99%. New models can be supplied as `module:factory`, returning the `Forecaster` protocol defined in replay.py. Use a distinct model name to keep variants separate.

Evaluation computes per-repetition metrics first, then median [Q1,Q3] across repetitions. Empty bins and absent crossings/advice use null, never zero. Positive error means a breach was predicted later than it occurred. Horizon bins are (0,5], (5,15], (15,30], and >30 minutes. The final-15-minute missed-warning denominator is logged forecast cycles; insufficient-data cycles have no persisted forecast. Recommendations use pre-cap matches for lead time; displayed does not determine eligibility.

Per-repetition scoring is limited to that workload's recorded start/end, excluding later stale forecasts after shutdown. Flat-high crossings are calibration failures. Summary output lists independent truth discrepancies, production/consumption rate drift and observed commit gaps. Inspect these checks before interpreting forecast quality. Collection resolution is five seconds, with overruns separately observable in the timing logs.

The cycle-overrun counter counts a serial global collection cycle exceeding its period and is attributed to participating clusters. Counter values are process-local. No overlapping collector cycles are scheduled.
