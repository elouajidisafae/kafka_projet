# Revision r1 ? Step 3 final offline forecast comparison

Completed: first-wave selection, committed parameters, second-wave evaluation and independent output review. No live forecasting code was changed. No merge or push.

## 1. Commits and scope

- `ed6a699`: Part 1 ? offline Holt and drift ARIMA adapters and model tests.
- `e19b522`: Part 4 ? timing and exact-input experiment tooling.
- `5d39ba3`: approved adjustment ? parallel fitting and minute-sampled first-wave selection.
- `7ea3c1f`: Part 2 ? freeze selected Holt/ARIMA settings BEFORE evaluation.
- Part 3 ? this final report and retained full test output (commit message: `docs: report held-out offline forecast comparison`).

Dashboard commits on the same branch are separate. The requested cumulative diff against `32e4ba3` is recorded in the final audit section below; it includes those intervening dashboard changes.

## 2. Implementation and protocol

`bench/comparison_models.py` contains undamped additive Holt and statsmodels ARIMA(p,1,q) with drift (`trend="t"` in levels). `bench/variants.py` exposes adapters through the existing replay plugin interface. `bench/model_comparison.py` reuses exact archived input IDs, canonical forecast deduplication, Baseline, split helpers, breach interpolation, horizon bins and inclusive quartiles. It handles selection, committed-parameter guards, parallel origins, evaluation and output reporting. Dependencies are benchmark-only in `requirements-bench.txt`.

All models receive the same archived one-hour histories at each evaluated origin. Alternatives causally regularise those observations on a five-second grid anchored at the last observation, carrying forward the last observed value. A filled step is an interval with no observation; no future observation is used. Holt starts at the first regular value with the first difference as initial trend. ARIMA uses maximum likelihood. Fit exceptions, non-convergence and nonfinite forecasts are missing; there is no fallback to OLS. Alternatives search every five-second future step through 24 hours for the first threshold crossing. OLS remains the unchanged archived baseline calculation.

Selection uses wave 0 only: earliest canonical forecast per half-open 60-second interval of each repetition, anchored on generator start, ordered by timestamp then forecast ID. This is 2,400 origins across 40 first-wave repetitions. Only forecast origins are sampled; their input histories are not thinned. Held-out wave 1 keeps EVERY canonical origin: 28,440 across 40 repetitions (10 per workload). Eight process workers were used, with one numerical-library thread each. Numerical outputs match sequential execution exactly on regression samples; duration telemetry naturally differs.

The criterion is unchanged: minimum pooled median absolute crossing-time error over estimable pre-breach forecasts from creeping, burst and sawtooth. Failed/insufficient fits and successful no-crossing forecasts have no numerical crossing error, so coverage accompanies all interpretation. Grids remain 30 Holt combinations and nine ARIMA orders.

## 3. Selected settings and first-wave results

Settings frozen in `7ea3c1f`: Holt alpha=0.2, beta=0.2; ARIMA(2,1,2) with drift. The completed parameter file records the sampling rule, source hashes and selection-report hash. The held-out run passed the committed-parameter guard.

Parameter SHA-256: `bbe6d92a4dd2289049a6cee142cf6bc722fc49636c1674e162fafb1658cedd6b`.

| Selected model | Pooled crossing error, seconds: median [Q1,Q3] | Scored predictions | Fit failures / origins | Missing forecasts |
|---|---|---:|---:|---:|
| Holt | 76.674 [20.745, 1059.464] | 684 | 0 / 2400 | 40 |
| ARIMA | 72.316 [11.475, 448.287] | 700 | 523 / 2400 | 563 |

ARIMA selection was conditional on successful estimable crossings: the winning order failed at 523/2,400 origins (21.79%). ARIMA(2,1,1) had median error 84.676 seconds with 37 failures, but did not win the prescribed criterion. No criterion was changed after inspecting these results.

All 39 candidate outputs were independently checked against the exact first-wave origin IDs, objective calculations, failure counts, original grids and provenance hashes. No second-wave result was used in selection.

## 4. Held-out crossing errors

Units: **seconds**. Each cell is median [Q1,Q3] across repetition-level median absolute errors, matching the archive aggregation. Horizons refer to time remaining until the observed CRITICAL crossing. These are conditional numerical errors, not penalties assigned to missing/non-crossing forecasts.

| Workload | Horizon (min) | OLS | Holt | ARIMA |
|---|---|---|---|---|
| creeping | 0-5 | 5.106 [1.554, 7.926] | 5.765 [5.092, 6.527] | 5.731 [5.030, 7.483] |
| creeping | 5-15 | 6.802 [1.866, 13.694] | 20.811 [17.399, 23.729] | 7.115 [4.374, 10.631] |
| creeping | 15-30 | 9.683 [6.986, 21.307] | 45.263 [41.125, 47.878] | 9.594 [7.089, 16.448] |
| burst | 0-5 | 268.056 [267.913, 274.851] | 15.796 [11.696, 25.969] | 102.227 [99.125, 105.280] |
| burst | 5-15 | 354.068 [290.331, 390.961] | 160.157 [122.190, 344.738] | 475.523 [459.618, 486.371] |
| burst | 15-30 | 8340.586 [7969.728, 8594.619] | 8024.258 [7854.053, 8330.190] | 8319.710 [8047.730, 8668.138] |
| sawtooth | 0-5 | 52.049 [45.159, 80.707] | 117.546 [115.429, 118.436] | 58.196 [50.881, 86.649] |
| sawtooth | 5-15 | 55.874 [31.300, 88.532] | 400.245 [398.075, 404.285] | 64.165 [50.102, 82.879] |
| sawtooth | 15-30 | 117.889 [87.402, 146.543] | 962.463 [949.040, 965.023] | 191.898 [154.930, 212.065] |

**Coverage exception:** OLS has estimable 0?5-minute burst errors in only 3 of 10 repetitions; Holt and ARIMA each have 10. All other displayed cells have 10 repetitions. Missing numerical crossings are not model-fit failures. Do not compare that OLS burst median as if it represented all 10 repetitions.

## 5. Paired comparison per repetition

Model minus OLS on repetition-level median errors. Counts below are **better / worse / tied (paired repetitions)**; total repetitions per workload are 10. These are comparisons of per-repetition summaries; the sets of estimable forecast origins within a repetition can differ between models.

| Workload | Horizon (min) | Holt versus OLS | ARIMA versus OLS |
|---|---|---|---|
| creeping | 0-5 | 4 / 6 / 0 (10/10) | 4 / 6 / 0 (10/10) |
| creeping | 5-15 | 0 / 10 / 0 (10/10) | 4 / 6 / 0 (10/10) |
| creeping | 15-30 | 0 / 10 / 0 (10/10) | 5 / 5 / 0 (10/10) |
| burst | 0-5 | 3 / 0 / 0 (3/10) | 3 / 0 / 0 (3/10) |
| burst | 5-15 | 7 / 3 / 0 (10/10) | 0 / 10 / 0 (10/10) |
| burst | 15-30 | 8 / 2 / 0 (10/10) | 6 / 4 / 0 (10/10) |
| sawtooth | 0-5 | 1 / 9 / 0 (10/10) | 0 / 10 / 0 (10/10) |
| sawtooth | 5-15 | 0 / 10 / 0 (10/10) | 4 / 6 / 0 (10/10) |
| sawtooth | 15-30 | 0 / 10 / 0 (10/10) | 3 / 7 / 0 (10/10) |

## 6. Stable backlog, runtime, filled steps and failures

The false-advice proxy is the share of **available forecasts** predicting a CRITICAL crossing within 30 minutes (`0 < ETA <= 1800`), computed per repetition and then summarised. All models use this definition. Successful non-crossing forecasts stay in the denominator; failed/insufficient fits are missing and excluded. This is a crossing-prediction proxy, not the live recommendation rules or archived recommendation-gated false-alarm metric.

| Model | False-crossing share %, median [Q1,Q3] | Available / all flat-high origins | Missing flat-high forecasts |
|---|---|---:|---:|
| OLS | 19.831 [19.444, 20.113] | 7110 / 7110 | 0 |
| Holt | 2.676 [2.676, 2.676] | 7100 / 7110 | 10 |
| ARIMA | 64.334 [48.975, 77.968] | 4976 / 7110 | 2134 |

| Model | Median fit/projection time (ms) | Filled steps summed over fits | Fit failures / all origins | Total missing forecasts |
|---|---:|---:|---:|---:|
| OLS | 0.901 | 0 | 0 / 28,440 | 0 |
| Holt | 0.686 | 324,319 | 0 / 28,440 | 40 |
| ARIMA | 736.932 | 324,319 | 5,137 / 28,440 | 5,177 |

ARIMA had 5,137 fit failures (18.06% of all held-out origins). Including 40 insufficient regular histories gives 5,177 missing forecasts (18.20%). Holt had no fit failures and 40 insufficient histories; OLS had neither. Filled steps are cumulative across overlapping fit windows, not distinct gaps. OLS does not regularise, so its zero is not a claim that the dataset had no gaps.

Computation time is measured inside each worker around fitting/projection, including regularisation. It excludes CSV loading, worker startup and interprocess transport, and reflects execution under eight-worker concurrency on this machine. It is not total phase wall time. The new run did not persist phase start/end timestamps, so an exact total elapsed duration cannot be reconstructed from these summaries.

## 7. OLS reproduction and independent review

- Forecast-field mismatches: **0**, across all 28,440 OLS origins (slope, intercept, R-squared and CRITICAL ETA).
- Archived per-repetition crossing-error metric differences: **0**, for the same second-wave repetitions.
- Parameter hash matches the committed settings and selection report; source hashes still match.
- Independent review matched every model JSONL row to the same dense second-wave origin and exact ordered input IDs, and recomputed every model summary from its forecast rows. All matched.
- All models use the same 40 second-wave repetitions; the recorded dataset and all pre-existing result folders were read only.

## 8. Interpretation for the paper

The evidence supports OLS as a fast, robust baseline, not as a universally best predictor. It is stronger than Holt on sawtooth and most creeping horizons; ARIMA does not consistently improve those workloads, is much slower, and has substantial fit failures. However, Holt is faster in measured median fit time, improves burst errors, and substantially reduces the stable-backlog false-crossing proxy. These advantages must be reported.

Both alternatives retain very large burst errors at 15?30 minutes. OLS misses estimable short-horizon burst crossings in seven repetitions. A statement that OLS dominates Holt/ARIMA in all workloads, has the lowest false-advice rate, or is the cheapest of these implementations would contradict these results. ARIMA conditional error medians must be accompanied by its missing-fit rate.

## 9. Tests and reproduction commands

The latest full suite is **247 passed, 0 failed, 2 skipped, 26 warnings**, 29.24 seconds, from the tested parallel/minute-sampling implementation. No source code changed during selection, evaluation or this output review; a redundant suite rerun was not needed. The complete output is retained at `internal/revision-r1-step3-tests.log` (copied from the matching Step 3b full-suite log).

The owner completed both long commands. **No further run is required.** Do not rerun into the existing output folders. Commands used were:

```powershell
.\.venv-batch2\Scripts\python.exe bench/model_comparison.py select --dataset bench/data/baseline-20261001T111043363413Z --workers 8
# Review and commit completed settings before the next command (commit 7ea3c1f).
.\.venv-batch2\Scripts\python.exe bench/model_comparison.py evaluate --dataset bench/data/baseline-20261001T111043363413Z --workers 8
```

Full retained outputs: `bench/results/baseline-20261001T111043363413Z/model-comparison/selection/` and `model-comparison/heldout/`. No output file was edited during review.

## 10. Remaining limitations / disagreements

No disagreement with the specified protocol. No selection or held-out work remains. The incomplete earlier Step 3 reports are superseded by this final report. Parameter selection was fixed before held-out evaluation. Missing-fit and conditional-crossing coverage limitations above prevent an unconditional best-model claim. No manuscript, live forecasting, recommendation rule, benchmark configuration or recorded dataset was changed.

## 11. Final cumulative diff audit

The actual cumulative Git diff against `32e4ba3` is recorded below. It includes intervening dashboard changes and this report/test log.

```text
 HEALTH_DEMO.md                          |   4 +-
 bench/capture_health_demo.py            |   6 +-
 bench/comparison_models.py              |  95 ++++++++
 bench/model_comparison.py               | 403 ++++++++++++++++++++++++++++++++
 bench/model_comparison_params.yml       |  24 ++
 bench/variants.py                       |  11 +
 internal/revision-r1-step3-report.md    | 148 ++++++++++++
 internal/revision-r1-step3-tests.log    | 376 +++++++++++++++++++++++++++++
 requirements-bench.txt                  |   2 +
 static/css/dashboard.css                |  40 ++--
 static/js/dashboard.js                  | 160 +++++++------
 templates/dashboard.html                |  28 +--
 tests/test_dashboard_panels.py          | 119 ++++++++++
 tests/test_interfaces.py                |   7 +-
 tests/test_model_comparison.py          |  59 +++++
 tests/test_model_comparison_protocol.py | 127 ++++++++++
 16 files changed, 1489 insertions(+), 120 deletions(-)
```
