# Benchmark guide

Record seeded Kafka workloads and evaluate forecasts offline. The benchmark uses its own broker and volumes, with ports **19092** (Kafka) and **18080** (dashboard).

Install dependencies in a Python 3.11-3.13 environment:

```bash
pip install -r requirements-bench.txt
```

Run commands on the host from the repository root with Docker Desktop running. The recording commands start and check the benchmark containers. Run benchmarks sequentially; they share one stack.

## Validate before recording

```bash
python bench/run_forecast_validation.py --arm smoke --profile smoke --reps 1 --waves 1 --duration-min 5
python bench/calibrate.py
```

Allow about 5 minutes for smoke and 45 minutes for calibration. Inspect the reports in `bench/results/`: pattern targets, rate/commit checks, replay and crossing-time agreement must pass. Refresh validation when configuration or workload changes.

## Record the baseline

Review and commit the source and configuration first. Baseline requires a clean working tree and matching successful smoke/calibration receipts.

```bash
python bench/run_forecast_validation.py --arm baseline --reps 10 --waves 2
```

Allow 2-2.5 hours for two 60-minute waves. Use a quiet host and prevent sleep/restarts. Do not repeat the baseline to select better outcomes. Failed or interrupted recordings are retained for diagnosis.

## Replay and evaluate

The recording command exports and checks results automatically. To repeat analysis on an existing dataset, without Kafka running:

```bash
python bench/replay.py --dataset bench/data/<run-id> --forecaster baseline
python bench/forecast_eval.py --dataset bench/data/<run-id> --forecaster baseline
```

| Output | Purpose |
|---|---|
| `bench/data/<run-id>/` | Sealed manifest, configuration, CSV logs, generator logs and checksums |
| `fidelity.json` | Numerical and recommendation agreement; mismatch lists |
| `summary.json` | Acceptance checks and metrics aggregated across repetitions |
| `per_rep.csv` | Metrics for each repetition |
| `error_vs_horizon.png` | Forecast error plot |

Reports are under `bench/results/<run-id>/baseline/`. Keep sealed datasets unchanged. Data and results are ignored by Git; archive them separately for publication.

Replay checks every numerical forecast and compares recommendation evidence per exact input observation. Scoring uses the earliest forecast per identical input observation so dashboard polling does not add statistical weight.

Metrics are computed per repetition, then summarized as median [Q1, Q3]. Errors use forecast-log timestamps; positive error means a late prediction. Missing values remain null. Scoring covers only each workload's lifetime and reports errors by horizon, missed warnings, advice lead time, confidence, false alarms and crossing-time agreement.

Acceptance requires at least 99% exact replay with no unexplained mismatches, rate drift at most 5%, commit gaps at most one second, and crossing-time agreement within five seconds for at least 95% of breaching repetitions. Flat-high controls must not cross the critical threshold. Prediction errors, false warnings and retained outliers remain results to report, even when acceptance passes.

## Offline model selection

Use `python bench/forecast_eval.py --dataset <path> --secondary` for the labelled post-baseline analyses. Use `python bench/select_forecaster.py --dataset <path>` to evaluate the nine multi-window settings on wave 1. Review and commit the generated `bench/forecaster_params.yml` before held-out evaluation; candidate replay requires an explicit split. The sealed recording and baseline fitter remain unchanged.

Then run `python bench/forecast_eval.py --dataset <path> --forecaster V0,V1,V2,V3 --split heldout`. Results under `heldout/` include paired per-repetition differences, interval coverage and finite width. Missing comparisons remain null; open-ended intervals are excluded from width. Parameters and analysis source hashes freeze on first execution; repeats require identical results. Preserve that source version before further integration changes.

Validate live integration separately with `python bench/run_forecast_validation.py --arm smoke --profile smoke --reps 1 --waves 1 --duration-min 5 --method multiwindow`; this uses committed parameters and replays V3. Smoke data is not predictive-performance evidence.

Multiwindow smoke replay runs in the recording container to compare unrounded interval endpoints exactly. Cross-platform numerical libraries can differ in their last floating-point digits; `replay-environment.json` records versions. Keep the container available until smoke validation finishes.

For the separate responsive experiment, review and commit `bench/forecaster_params_responsive.yml` before running `python bench/responsive.py --dataset <path>`. This compares fixed V2 settings (5 minutes / 0.5) with V0 on wave two only, under `heldout-exploratory/`. Results are explicitly exploratory because the original held-out results have already been inspected. The original `heldout/` outputs are not overwritten.

## Scalability benchmark

Commit reviewed code first; the command requires its full SHA and a clean working tree. Run on the host with Docker, `psutil`, and the project dependencies installed. Stop unrelated containers and workloads. The benchmark uses the existing benchmark broker with a named data volume tied to the commit, preserving its previous volume. No second broker or Compose profile is added.

```bash
python bench/paper_scalability.py --commit <SHA> --smoke
python bench/paper_scalability.py --commit <SHA>
```

First validate with `--smoke` (one short repetition, separate outputs). Full runs use five repetitions with rotated scenario order and report median [Q1, Q3]. Allow roughly 60?90 minutes, depending on startup and collection time. Scalability uses 10/100/500 real committed group/topic pairs, 60-second warm-up and 120-second windows. Its workload is inactive groups with static lag, not active-consumer throughput. Memory is application-process RSS.

Outputs are under `bench/results/scalability/` (or `bench/results/scalability-smoke/` for smoke), including manifests and raw observations. Existing runs are never overwritten; failed runs remain available for diagnosis.

Reproducing the published numbers requires the sealed dataset from the Zenodo record (link to be added).
