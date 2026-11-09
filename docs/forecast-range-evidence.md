# Archived crossing-range accuracy

Source: `bench/results/baseline-20261001T111043363413Z/heldout/summary.json`, `variants.V1/V3.patterns`. Held-out split, archived aggregates only; no replay or fitting. Coverage and relative width are medians of per-repetition statistics. Nominal band level is 90%; this does not guarantee 90% crossing-time coverage.

Archive policy: All pre-breach forecasts; missing lower bound counts as uncovered; null upper bound is open-ended. Width excludes open-ended intervals.

| Mode | Workload | Horizon (min) | Coverage | Median relative width | Open-ended count (median/rep) |
|---|---|---|---:|---:|---:|
| V1 | burst | 0-5 | 1.000000 | 1.624365 | 0.000000 |
| V1 | burst | 5-15 | 0.665231 | 1.270722 | 0.000000 |
| V1 | burst | 15-30 | 0.000000 | 0.109415 | 0.000000 |
| V1 | burst | 30+ | 0.000000 | 0.749768 | 0.000000 |
| V1 | creeping | 0-5 | 0.620480 | 0.055650 | 0.000000 |
| V1 | creeping | 5-15 | 0.273109 | 0.010356 | 0.000000 |
| V1 | creeping | 15-30 | 0.175831 | 0.005804 | 0.000000 |
| V1 | creeping | 30+ | N/A | N/A | 0.000000 |
| V1 | flat_high | 0-5 | N/A | N/A | 0.000000 |
| V1 | flat_high | 5-15 | N/A | N/A | 0.000000 |
| V1 | flat_high | 15-30 | N/A | N/A | 0.000000 |
| V1 | flat_high | 30+ | N/A | N/A | 0.000000 |
| V1 | sawtooth | 0-5 | 1.000000 | 1.390906 | 0.000000 |
| V1 | sawtooth | 5-15 | 1.000000 | 0.338385 | 0.000000 |
| V1 | sawtooth | 15-30 | 0.657303 | 0.237867 | 5.500000 |
| V1 | sawtooth | 30+ | 0.000000 | 0.003299 | 0.000000 |
| V3 | burst | 0-5 | 1.000000 | 1.624365 | 0.000000 |
| V3 | burst | 5-15 | 0.665231 | 1.270722 | 0.000000 |
| V3 | burst | 15-30 | 0.000000 | 0.109415 | 0.000000 |
| V3 | burst | 30+ | 0.000000 | 0.749768 | 0.000000 |
| V3 | creeping | 0-5 | 0.620480 | 0.055650 | 0.000000 |
| V3 | creeping | 5-15 | 0.273109 | 0.010356 | 0.000000 |
| V3 | creeping | 15-30 | 0.175831 | 0.005804 | 0.000000 |
| V3 | creeping | 30+ | N/A | N/A | 0.000000 |
| V3 | flat_high | 0-5 | N/A | N/A | 0.000000 |
| V3 | flat_high | 5-15 | N/A | N/A | 0.000000 |
| V3 | flat_high | 15-30 | N/A | N/A | 0.000000 |
| V3 | flat_high | 30+ | N/A | N/A | 0.000000 |
| V3 | sawtooth | 0-5 | 1.000000 | 1.390906 | 0.000000 |
| V3 | sawtooth | 5-15 | 1.000000 | 0.338385 | 0.000000 |
| V3 | sawtooth | 15-30 | 0.657303 | 0.237867 | 5.500000 |
| V3 | sawtooth | 30+ | 0.000000 | 0.003299 | 0.000000 |

The saved summary provides **relative width**, not absolute width in seconds, and **open-ended counts**, not shares or their denominators. Absolute median widths and open-ended shares cannot be recovered from these aggregated medians. They are not recomputed here, as requested. Flat backlog has no observed crossing, so crossing coverage is not estimable. The standalone V1/summary.json predates the interval augmentation; use the combined heldout/summary.json, which contains both V1 and V3 interval metrics.
