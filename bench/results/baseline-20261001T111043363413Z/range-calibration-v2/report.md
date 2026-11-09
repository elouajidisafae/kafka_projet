# Second-half range calibration v2 evaluation

Bins use predicted horizons. Coverage uses all eligible positive pre-crossing forecasts in each row; suppressed ranges count as uncovered. Width is the median among ranges shown. The 0-30 pooled row is the displayed-domain comparison and supplies the v2 tooltip coverage. No range is shown by v2 above 30 minutes.

| Mode | Workload | Bin (min) | n | Version | Ranges shown | Covered | Coverage | Median width (s) |
|---|---|---|---:|---|---:|---:|---:|---:|
| baseline | burst | 0-5 | 466 | v1 | 466 | 393 | 0.843 | 1381.556 |
| baseline | burst | 0-5 | 466 | v2 | 466 | 393 | 0.843 | 1381.556 |
| baseline | burst | 5-15 | 311 | v1 | 311 | 262 | 0.842 | 521.977 |
| baseline | burst | 5-15 | 311 | v2 | 311 | 262 | 0.842 | 521.977 |
| baseline | burst | 15-30 | 132 | v1 | 132 | 10 | 0.076 | 236.097 |
| baseline | burst | 15-30 | 132 | v2 | 132 | 10 | 0.076 | 236.097 |
| baseline | burst | 30+ | 1922 | v1 | 1922 | 1705 | 0.887 | 6319.911 |
| baseline | burst | 30+ | 1922 | v2 | 0 | 0 | 0.000 | N/A |
| baseline | burst | 0-30 | 909 | v1 | 909 | 665 | 0.732 | 624.301 |
| baseline | burst | 0-30 | 909 | v2 | 909 | 665 | 0.732 | 624.301 |
| baseline | burst | all | 2831 | v1 | 2831 | 2370 | 0.837 | 6086.237 |
| baseline | burst | all | 2831 | v2 | 909 | 665 | 0.235 | 624.301 |
| baseline | creeping | 0-5 | 592 | v1 | 592 | 590 | 0.997 | 1757.293 |
| baseline | creeping | 0-5 | 592 | v2 | 592 | 590 | 0.997 | 1757.293 |
| baseline | creeping | 5-15 | 1194 | v1 | 1194 | 1194 | 1.000 | 618.620 |
| baseline | creeping | 5-15 | 1194 | v2 | 1194 | 1194 | 1.000 | 618.620 |
| baseline | creeping | 15-30 | 1678 | v1 | 1678 | 1621 | 0.966 | 253.600 |
| baseline | creeping | 15-30 | 1678 | v2 | 1678 | 1621 | 0.966 | 253.600 |
| baseline | creeping | 30+ | 31 | v1 | 31 | 0 | 0.000 | 1238.004 |
| baseline | creeping | 30+ | 31 | v2 | 0 | 0 | 0.000 | N/A |
| baseline | creeping | 0-30 | 3464 | v1 | 3464 | 3405 | 0.983 | 334.479 |
| baseline | creeping | 0-30 | 3464 | v2 | 3464 | 3405 | 0.983 | 334.479 |
| baseline | creeping | all | 3495 | v1 | 3495 | 3405 | 0.974 | 336.210 |
| baseline | creeping | all | 3495 | v2 | 3464 | 3405 | 0.974 | 334.479 |
| baseline | sawtooth | 0-5 | 524 | v1 | 524 | 447 | 0.853 | 2231.299 |
| baseline | sawtooth | 0-5 | 524 | v2 | 524 | 447 | 0.853 | 2231.299 |
| baseline | sawtooth | 5-15 | 1272 | v1 | 1272 | 996 | 0.783 | 622.237 |
| baseline | sawtooth | 5-15 | 1272 | v2 | 1272 | 996 | 0.783 | 622.237 |
| baseline | sawtooth | 15-30 | 1436 | v1 | 1436 | 1295 | 0.902 | 242.348 |
| baseline | sawtooth | 15-30 | 1436 | v2 | 1436 | 1295 | 0.902 | 242.348 |
| baseline | sawtooth | 30+ | 348 | v1 | 348 | 264 | 0.759 | 1640.405 |
| baseline | sawtooth | 30+ | 348 | v2 | 0 | 0 | 0.000 | N/A |
| baseline | sawtooth | 0-30 | 3232 | v1 | 3232 | 2738 | 0.847 | 397.943 |
| baseline | sawtooth | 0-30 | 3232 | v2 | 3232 | 2738 | 0.847 | 397.943 |
| baseline | sawtooth | all | 3580 | v1 | 3580 | 3002 | 0.839 | 479.598 |
| baseline | sawtooth | all | 3580 | v2 | 3232 | 2738 | 0.765 | 397.943 |
| baseline | pooled | 0-5 | 1582 | v1 | 1582 | 1430 | 0.904 | 1815.098 |
| baseline | pooled | 0-5 | 1582 | v2 | 1582 | 1430 | 0.904 | 1815.098 |
| baseline | pooled | 5-15 | 2777 | v1 | 2777 | 2452 | 0.883 | 608.800 |
| baseline | pooled | 5-15 | 2777 | v2 | 2777 | 2452 | 0.883 | 608.800 |
| baseline | pooled | 15-30 | 3246 | v1 | 3246 | 2926 | 0.901 | 248.214 |
| baseline | pooled | 15-30 | 3246 | v2 | 3246 | 2926 | 0.901 | 248.214 |
| baseline | pooled | 30+ | 2301 | v1 | 2301 | 1969 | 0.856 | 6233.539 |
| baseline | pooled | 30+ | 2301 | v2 | 0 | 0 | 0.000 | N/A |
| baseline | pooled | 0-30 | 7605 | v1 | 7605 | 6808 | 0.895 | 396.909 |
| baseline | pooled | 0-30 | 7605 | v2 | 7605 | 6808 | 0.895 | 396.909 |
| baseline | pooled | all | 9906 | v1 | 9906 | 8777 | 0.886 | 629.473 |
| baseline | pooled | all | 9906 | v2 | 7605 | 6808 | 0.687 | 396.909 |
| multiwindow | burst | 0-5 | 832 | v1 | 832 | 728 | 0.875 | 1355.633 |
| multiwindow | burst | 0-5 | 832 | v2 | 832 | 701 | 0.843 | 968.566 |
| multiwindow | burst | 5-15 | 570 | v1 | 570 | 449 | 0.788 | 151.566 |
| multiwindow | burst | 5-15 | 570 | v2 | 570 | 442 | 0.775 | 139.634 |
| multiwindow | burst | 15-30 | 22 | v1 | 22 | 0 | 0.000 | 202.619 |
| multiwindow | burst | 15-30 | 22 | v2 | 22 | 0 | 0.000 | 202.619 |
| multiwindow | burst | 30+ | 1800 | v1 | 1800 | 1590 | 0.883 | 6421.325 |
| multiwindow | burst | 30+ | 1800 | v2 | 0 | 0 | 0.000 | N/A |
| multiwindow | burst | 0-30 | 1424 | v1 | 1424 | 1177 | 0.827 | 347.946 |
| multiwindow | burst | 0-30 | 1424 | v2 | 1424 | 1143 | 0.803 | 279.726 |
| multiwindow | burst | all | 3224 | v1 | 3224 | 2767 | 0.858 | 5989.352 |
| multiwindow | burst | all | 3224 | v2 | 1424 | 1143 | 0.355 | 279.726 |
| multiwindow | creeping | 0-5 | 592 | v1 | 592 | 590 | 0.997 | 1373.709 |
| multiwindow | creeping | 0-5 | 592 | v2 | 592 | 555 | 0.938 | 105.979 |
| multiwindow | creeping | 5-15 | 1194 | v1 | 1194 | 1194 | 1.000 | 210.959 |
| multiwindow | creeping | 5-15 | 1194 | v2 | 1194 | 1194 | 1.000 | 141.914 |
| multiwindow | creeping | 15-30 | 1678 | v1 | 1678 | 1621 | 0.966 | 228.531 |
| multiwindow | creeping | 15-30 | 1678 | v2 | 1678 | 1592 | 0.949 | 177.373 |
| multiwindow | creeping | 30+ | 31 | v1 | 31 | 0 | 0.000 | 1252.892 |
| multiwindow | creeping | 30+ | 31 | v2 | 0 | 0 | 0.000 | N/A |
| multiwindow | creeping | 0-30 | 3464 | v1 | 3464 | 3405 | 0.983 | 237.218 |
| multiwindow | creeping | 0-30 | 3464 | v2 | 3464 | 3341 | 0.964 | 155.807 |
| multiwindow | creeping | all | 3495 | v1 | 3495 | 3405 | 0.974 | 238.151 |
| multiwindow | creeping | all | 3495 | v2 | 3464 | 3341 | 0.956 | 155.807 |
| multiwindow | sawtooth | 0-5 | 524 | v1 | 524 | 441 | 0.842 | 1744.248 |
| multiwindow | sawtooth | 0-5 | 524 | v2 | 524 | 395 | 0.754 | 149.905 |
| multiwindow | sawtooth | 5-15 | 1272 | v1 | 1272 | 995 | 0.782 | 212.192 |
| multiwindow | sawtooth | 5-15 | 1272 | v2 | 1272 | 1017 | 0.800 | 145.827 |
| multiwindow | sawtooth | 15-30 | 1436 | v1 | 1436 | 1225 | 0.853 | 218.392 |
| multiwindow | sawtooth | 15-30 | 1436 | v2 | 1436 | 1189 | 0.828 | 206.576 |
| multiwindow | sawtooth | 30+ | 348 | v1 | 348 | 283 | 0.813 | 1660.133 |
| multiwindow | sawtooth | 30+ | 348 | v2 | 0 | 0 | 0.000 | N/A |
| multiwindow | sawtooth | 0-30 | 3232 | v1 | 3232 | 2661 | 0.823 | 233.991 |
| multiwindow | sawtooth | 0-30 | 3232 | v2 | 3232 | 2601 | 0.805 | 179.117 |
| multiwindow | sawtooth | all | 3580 | v1 | 3580 | 2944 | 0.822 | 243.871 |
| multiwindow | sawtooth | all | 3580 | v2 | 3232 | 2601 | 0.727 | 179.117 |
| multiwindow | pooled | 0-5 | 1948 | v1 | 1948 | 1759 | 0.903 | 1482.159 |
| multiwindow | pooled | 0-5 | 1948 | v2 | 1948 | 1651 | 0.848 | 187.206 |
| multiwindow | pooled | 5-15 | 3036 | v1 | 3036 | 2638 | 0.869 | 193.511 |
| multiwindow | pooled | 5-15 | 3036 | v2 | 3036 | 2653 | 0.874 | 142.863 |
| multiwindow | pooled | 15-30 | 3136 | v1 | 3136 | 2846 | 0.908 | 224.111 |
| multiwindow | pooled | 15-30 | 3136 | v2 | 3136 | 2781 | 0.887 | 194.509 |
| multiwindow | pooled | 30+ | 2179 | v1 | 2179 | 1873 | 0.860 | 6341.706 |
| multiwindow | pooled | 30+ | 2179 | v2 | 0 | 0 | 0.000 | N/A |
| multiwindow | pooled | 0-30 | 8120 | v1 | 8120 | 7243 | 0.892 | 237.284 |
| multiwindow | pooled | 0-30 | 8120 | v2 | 8120 | 7085 | 0.873 | 171.188 |
| multiwindow | pooled | all | 10299 | v1 | 10299 | 9116 | 0.885 | 270.909 |
| multiwindow | pooled | all | 10299 | v2 | 8120 | 7085 | 0.688 | 171.188 |

Stable backlog, canonical origins (share showing a range, not crossing coverage):

| Mode | Predicted bin / version | n | Range n | Share |
|---|---|---:|---:|---:|
| baseline | 0-30/v1 | 1408 | 1408 | 1.000 |
| baseline | 0-30/v2 | 1408 | 1408 | 1.000 |
| baseline | 30+/v1 | 5722 | 5722 | 1.000 |
| baseline | 30+/v2 | 5722 | 0 | 0.000 |
| baseline | nonpositive/v1 | 0 | 0 | N/A |
| baseline | nonpositive/v2 | 0 | 0 | N/A |
| baseline | all/v1 | 7130 | 7130 | 1.000 |
| baseline | all/v2 | 7130 | 1408 | 0.197 |
| multiwindow | 0-30/v1 | 610 | 610 | 1.000 |
| multiwindow | 0-30/v2 | 610 | 610 | 1.000 |
| multiwindow | 30+/v1 | 3291 | 3291 | 1.000 |
| multiwindow | 30+/v2 | 3291 | 0 | 0.000 |
| multiwindow | nonpositive/v1 | 3229 | 0 | 0.000 |
| multiwindow | nonpositive/v2 | 3229 | 0 | 0.000 |
| multiwindow | all/v1 | 7130 | 3901 | 0.547 |
| multiwindow | all/v2 | 7130 | 610 | 0.086 |
