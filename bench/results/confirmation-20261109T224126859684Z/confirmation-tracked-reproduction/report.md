# Frozen confirmation results

Table 5 errors: median [Q1,Q3] of per-repetition medians; nulls excluded and counted.

| Model | Workload | Horizon | Median error (s) | Q1 | Q3 | Repetitions |
|---|---|---|---:|---:|---:|---:|
| OLS | burst | after-burst 0-15 | 374.354879 | 343.756781 | 403.67414 | 10 |
| OLS | creeping | 0-5 | 4.717681 | 2.723038 | 6.814854 | 10 |
| OLS | creeping | 15-30 | 10.573116 | 8.632332 | 13.342327 | 10 |
| OLS | creeping | 5-15 | 6.389903 | 5.581373 | 10.068301 | 10 |
| OLS | sawtooth | 0-5 | 67.860608 | 54.482176 | 86.804868 | 10 |
| OLS | sawtooth | 15-30 | 149.27258 | 123.907649 | 183.46015 | 10 |
| OLS | sawtooth | 5-15 | 68.724692 | 56.862166 | 90.083267 | 10 |
| Responsive | burst | after-burst 0-15 | 22.030434 | 13.927127 | 26.103846 | 10 |
| Responsive | creeping | 0-5 | 4.717681 | 2.723038 | 6.814854 | 10 |
| Responsive | creeping | 15-30 | 10.573116 | 8.632332 | 13.342327 | 10 |
| Responsive | creeping | 5-15 | 6.389903 | 5.581373 | 10.068301 | 10 |
| Responsive | sawtooth | 0-5 | 67.860608 | 54.482176 | 86.804868 | 10 |
| Responsive | sawtooth | 15-30 | 149.27258 | 123.907649 | 183.46015 | 10 |
| Responsive | sawtooth | 5-15 | 68.724692 | 56.862166 | 90.083267 | 10 |
| Holt | burst | after-burst 0-15 | 51.768432 | 43.754257 | 56.901361 | 10 |
| Holt | creeping | 0-5 | 3.681766 | 2.769675 | 4.967277 | 10 |
| Holt | creeping | 15-30 | 61.030051 | 52.690526 | 62.106984 | 10 |
| Holt | creeping | 5-15 | 22.453801 | 17.221784 | 26.350197 | 10 |
| Holt | sawtooth | 0-5 | 112.586922 | 110.074399 | 113.245761 | 10 |
| Holt | sawtooth | 15-30 | 923.23318 | 919.682526 | 930.465616 | 10 |
| Holt | sawtooth | 5-15 | 404.949753 | 403.266922 | 405.570043 | 10 |
| ARIMA_212 | burst | after-burst 0-15 | 350.45434 | 340.716157 | 368.452983 | 10 |
| ARIMA_212 | creeping | 0-5 | 2.445361 | 1.866155 | 4.033203 | 10 |
| ARIMA_212 | creeping | 15-30 | 10.319395 | 8.233122 | 13.063069 | 10 |
| ARIMA_212 | creeping | 5-15 | 6.229859 | 4.2858 | 7.739227 | 10 |
| ARIMA_212 | sawtooth | 0-5 | 85.547913 | 71.429442 | 91.149384 | 10 |
| ARIMA_212 | sawtooth | 15-30 | 210.421905 | 201.56245 | 215.455873 | 10 |
| ARIMA_212 | sawtooth | 5-15 | 73.018804 | 70.835221 | 78.835843 | 10 |
| ARIMA_211_exploratory | burst | after-burst 0-15 | 345.15875 | 329.942537 | 353.210761 | 10 |
| ARIMA_211_exploratory | creeping | 0-5 | 2.444794 | 1.867252 | 4.030988 | 10 |
| ARIMA_211_exploratory | creeping | 15-30 | 10.330981 | 8.348002 | 14.992049 | 10 |
| ARIMA_211_exploratory | creeping | 5-15 | 6.235068 | 4.31134 | 7.71799 | 10 |
| ARIMA_211_exploratory | sawtooth | 0-5 | 94.695828 | 67.498808 | 105.366385 | 10 |
| ARIMA_211_exploratory | sawtooth | 15-30 | 248.692213 | 222.608168 | 257.37505 | 10 |
| ARIMA_211_exploratory | sawtooth | 5-15 | 74.196552 | 62.764927 | 85.062565 | 10 |

## Positive crossing coverage

| Model | Workload | Horizon | Origins | Positive | Pooled fraction | Median per-rep fraction | Q1 | Q3 |
|---|---|---|---:|---:|---:|---:|---:|---:|
| OLS | burst | after-burst 0-15 | 1725 | 919 | 0.532754 | 0.551054 | 0.515278 | 0.575015 |
| OLS | creeping | 0-5 | 600 | 594 | 0.99 | 0.983333 | 0.983333 | 1.0 |
| OLS | creeping | 15-30 | 1726 | 1726 | 1.0 | 1.0 | 1.0 | 1.0 |
| OLS | creeping | 5-15 | 1200 | 1200 | 1.0 | 1.0 | 1.0 | 1.0 |
| OLS | sawtooth | 0-5 | 600 | 600 | 1.0 | 1.0 | 1.0 | 1.0 |
| OLS | sawtooth | 15-30 | 1803 | 1803 | 1.0 | 1.0 | 1.0 | 1.0 |
| OLS | sawtooth | 5-15 | 1200 | 1200 | 1.0 | 1.0 | 1.0 | 1.0 |
| Responsive | burst | after-burst 0-15 | 1725 | 1421 | 0.823768 | 0.816092 | 0.81155 | 0.822222 |
| Responsive | creeping | 0-5 | 600 | 594 | 0.99 | 0.983333 | 0.983333 | 1.0 |
| Responsive | creeping | 15-30 | 1726 | 1726 | 1.0 | 1.0 | 1.0 | 1.0 |
| Responsive | creeping | 5-15 | 1200 | 1200 | 1.0 | 1.0 | 1.0 | 1.0 |
| Responsive | sawtooth | 0-5 | 600 | 600 | 1.0 | 1.0 | 1.0 | 1.0 |
| Responsive | sawtooth | 15-30 | 1803 | 1803 | 1.0 | 1.0 | 1.0 | 1.0 |
| Responsive | sawtooth | 5-15 | 1200 | 1200 | 1.0 | 1.0 | 1.0 | 1.0 |
| Holt | burst | after-burst 0-15 | 1725 | 1577 | 0.914203 | 0.903927 | 0.901041 | 0.909722 |
| Holt | creeping | 0-5 | 600 | 600 | 1.0 | 1.0 | 1.0 | 1.0 |
| Holt | creeping | 15-30 | 1726 | 1716 | 0.994206 | 0.99422 | 0.994169 | 0.994253 |
| Holt | creeping | 5-15 | 1200 | 1200 | 1.0 | 1.0 | 1.0 | 1.0 |
| Holt | sawtooth | 0-5 | 600 | 242 | 0.403333 | 0.4 | 0.4 | 0.4 |
| Holt | sawtooth | 15-30 | 1803 | 887 | 0.491958 | 0.491713 | 0.489595 | 0.494444 |
| Holt | sawtooth | 5-15 | 1200 | 670 | 0.558333 | 0.558333 | 0.552083 | 0.564583 |
| ARIMA_212 | burst | after-burst 0-15 | 1725 | 1551 | 0.89913 | 0.988889 | 0.956609 | 1.0 |
| ARIMA_212 | creeping | 0-5 | 600 | 562 | 0.936667 | 0.933333 | 0.933333 | 0.979167 |
| ARIMA_212 | creeping | 15-30 | 1726 | 1377 | 0.797798 | 0.797059 | 0.783575 | 0.823002 |
| ARIMA_212 | creeping | 5-15 | 1200 | 1140 | 0.95 | 0.945833 | 0.925 | 0.979167 |
| ARIMA_212 | sawtooth | 0-5 | 600 | 449 | 0.748333 | 0.758333 | 0.7 | 0.825 |
| ARIMA_212 | sawtooth | 15-30 | 1803 | 1222 | 0.677759 | 0.687001 | 0.643056 | 0.7 |
| ARIMA_212 | sawtooth | 5-15 | 1200 | 995 | 0.829167 | 0.825 | 0.802083 | 0.864583 |
| ARIMA_211_exploratory | burst | after-burst 0-15 | 1725 | 1725 | 1.0 | 1.0 | 1.0 | 1.0 |
| ARIMA_211_exploratory | creeping | 0-5 | 600 | 598 | 0.996667 | 1.0 | 1.0 | 1.0 |
| ARIMA_211_exploratory | creeping | 15-30 | 1726 | 1650 | 0.955968 | 0.959885 | 0.940616 | 0.965314 |
| ARIMA_211_exploratory | creeping | 5-15 | 1200 | 1189 | 0.990833 | 0.995833 | 0.985417 | 1.0 |
| ARIMA_211_exploratory | sawtooth | 0-5 | 600 | 571 | 0.951667 | 0.966667 | 0.925 | 0.966667 |
| ARIMA_211_exploratory | sawtooth | 15-30 | 1803 | 1668 | 0.925125 | 0.927993 | 0.912615 | 0.9375 |
| ARIMA_211_exploratory | sawtooth | 5-15 | 1200 | 1164 | 0.97 | 0.966667 | 0.952083 | 0.983333 |

## Stable false crossings within 30 minutes

| Model | Median per-rep fraction | Q1 | Q3 | Repetitions |
|---|---:|---:|---:|---:|
| OLS | 0.187849 | 0.184707 | 0.189944 | 10 |
| Responsive | 0.087989 | 0.087989 | 0.087989 | 10 |
| Holt | 0.025175 | 0.025175 | 0.026573 | 10 |
| ARIMA_212 | 0.509703 | 0.456529 | 0.55937 | 10 |
| ARIMA_211_exploratory | 0.812237 | 0.796472 | 0.827794 | 10 |

## Gated advice and lead time

Holt/ARIMA advice: N/A (no archived R2 gate for these models).

| Model | Workload | Measure | Median | Q1 | Q3 | Repetitions |
|---|---|---|---:|---:|---:|---:|
| OLS | burst | M4_advice_lead_seconds | 1826.456104 | 1794.085029 | 1861.345045 | 10 |
| OLS | burst | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS | creeping | M4_advice_lead_seconds | 1761.7991 | 1753.511534 | 1765.704854 | 10 |
| OLS | creeping | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS | sawtooth | M4_advice_lead_seconds | 1799.426622 | 1798.767126 | 1800.385934 | 10 |
| OLS | sawtooth | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS | flat_high | M4_advice_lead_seconds | N/A | N/A | N/A | 0 |
| OLS | flat_high | M6_false_alarm_cycle_fraction | 0.055866 | 0.055866 | 0.055866 | 10 |
| OLS | burst | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | burst | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | burst | S2_critical_minus_advice | 1826.456104 | 1794.085029 | 1861.345045 | 10 |
| OLS | creeping | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | creeping | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | creeping | S2_critical_minus_advice | 1761.7991 | 1753.511534 | 1765.704854 | 10 |
| OLS | flat_high | S1_after_fill_advice_fraction | 0.045198 | 0.045198 | 0.045198 | 10 |
| OLS | flat_high | S1_fill_advice_fraction | 1.0 | 1.0 | 1.0 | 10 |
| OLS | flat_high | S2_critical_minus_advice | N/A | N/A | N/A | 0 |
| OLS | sawtooth | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | sawtooth | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS | sawtooth | S2_critical_minus_advice | 1799.426622 | 1798.767126 | 1800.385934 | 10 |
| Responsive | burst | M4_advice_lead_seconds | 1826.456104 | 1794.085029 | 1861.345045 | 10 |
| Responsive | burst | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| Responsive | creeping | M4_advice_lead_seconds | 1761.7991 | 1753.511534 | 1765.704854 | 10 |
| Responsive | creeping | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| Responsive | sawtooth | M4_advice_lead_seconds | 1799.426622 | 1798.767126 | 1800.385934 | 10 |
| Responsive | sawtooth | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| Responsive | flat_high | M4_advice_lead_seconds | N/A | N/A | N/A | 0 |
| Responsive | flat_high | M6_false_alarm_cycle_fraction | 0.186453 | 0.120112 | 0.233589 | 10 |
| Responsive | burst | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | burst | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | burst | S2_critical_minus_advice | 1826.456104 | 1794.085029 | 1861.345045 | 10 |
| Responsive | creeping | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | creeping | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | creeping | S2_critical_minus_advice | 1761.7991 | 1753.511534 | 1765.704854 | 10 |
| Responsive | flat_high | S1_after_fill_advice_fraction | 0.17726 | 0.110169 | 0.224929 | 10 |
| Responsive | flat_high | S1_fill_advice_fraction | 1.0 | 1.0 | 1.0 | 10 |
| Responsive | flat_high | S2_critical_minus_advice | N/A | N/A | N/A | 0 |
| Responsive | sawtooth | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | sawtooth | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| Responsive | sawtooth | S2_critical_minus_advice | 1799.426622 | 1798.767126 | 1800.385934 | 10 |
| OLS recorded advice | burst | M4_advice_lead_seconds | 1826.407897 | 1794.048331 | 1861.305462 | 10 |
| OLS recorded advice | burst | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | creeping | M4_advice_lead_seconds | 1761.71393 | 1753.429882 | 1765.61695 | 10 |
| OLS recorded advice | creeping | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | sawtooth | M4_advice_lead_seconds | 1799.265882 | 1798.608296 | 1800.236203 | 10 |
| OLS recorded advice | sawtooth | M6_false_alarm_cycle_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | flat_high | M4_advice_lead_seconds | N/A | N/A | N/A | 0 |
| OLS recorded advice | flat_high | M6_false_alarm_cycle_fraction | 0.055866 | 0.055866 | 0.055866 | 10 |
| OLS recorded advice | burst | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | burst | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | burst | S2_critical_minus_advice | 1826.407897 | 1794.048331 | 1861.305462 | 10 |
| OLS recorded advice | creeping | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | creeping | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | creeping | S2_critical_minus_advice | 1761.71393 | 1753.429882 | 1765.61695 | 10 |
| OLS recorded advice | flat_high | S1_after_fill_advice_fraction | 0.045198 | 0.045198 | 0.045198 | 10 |
| OLS recorded advice | flat_high | S1_fill_advice_fraction | 1.0 | 1.0 | 1.0 | 10 |
| OLS recorded advice | flat_high | S2_critical_minus_advice | N/A | N/A | N/A | 0 |
| OLS recorded advice | sawtooth | S1_after_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | sawtooth | S1_fill_advice_fraction | N/A | N/A | N/A | 0 |
| OLS recorded advice | sawtooth | S2_critical_minus_advice | 1799.265882 | 1798.608296 | 1800.236203 | 10 |

## Calibrated ranges (predicted horizons up to 30 minutes)

| Model | Workload | Bin | Version | n | Ranges | Covered | Coverage | Median width (s) |
|---|---|---|---|---:|---:|---:|---:|---:|
| OLS | burst | 0-5 | v1 | 473 | 473 | 391 | 0.826638 | 1387.336197 |
| OLS | burst | 0-5 | v2 | 473 | 473 | 391 | 0.826638 | 1387.336197 |
| OLS | burst | 5-15 | v1 | 317 | 317 | 253 | 0.798107 | 519.909369 |
| OLS | burst | 5-15 | v2 | 317 | 317 | 253 | 0.798107 | 519.909369 |
| OLS | burst | 15-30 | v1 | 131 | 131 | 18 | 0.137405 | 234.462048 |
| OLS | burst | 15-30 | v2 | 131 | 131 | 18 | 0.137405 | 234.462048 |
| OLS | burst | 0-30 | v1 | 921 | 921 | 662 | 0.718784 | 628.439158 |
| OLS | burst | 0-30 | v2 | 921 | 921 | 662 | 0.718784 | 628.439158 |
| OLS | creeping | 0-5 | v1 | 602 | 602 | 600 | 0.996678 | 1734.170246 |
| OLS | creeping | 0-5 | v2 | 602 | 602 | 600 | 0.996678 | 1734.170246 |
| OLS | creeping | 5-15 | v1 | 1206 | 1206 | 1206 | 1.0 | 619.136605 |
| OLS | creeping | 5-15 | v2 | 1206 | 1206 | 1206 | 1.0 | 619.136605 |
| OLS | creeping | 15-30 | v1 | 1669 | 1669 | 1587 | 0.950869 | 254.657712 |
| OLS | creeping | 15-30 | v2 | 1669 | 1669 | 1587 | 0.950869 | 254.657712 |
| OLS | creeping | 0-30 | v1 | 3477 | 3477 | 3393 | 0.975841 | 336.594408 |
| OLS | creeping | 0-30 | v2 | 3477 | 3477 | 3393 | 0.975841 | 336.594408 |
| OLS | sawtooth | 0-5 | v1 | 500 | 500 | 418 | 0.836 | 2202.396213 |
| OLS | sawtooth | 0-5 | v2 | 500 | 500 | 418 | 0.836 | 2202.396213 |
| OLS | sawtooth | 5-15 | v1 | 1278 | 1278 | 1001 | 0.783255 | 624.30469 |
| OLS | sawtooth | 5-15 | v2 | 1278 | 1278 | 1001 | 0.783255 | 624.30469 |
| OLS | sawtooth | 15-30 | v1 | 1411 | 1411 | 1276 | 0.904323 | 244.848389 |
| OLS | sawtooth | 15-30 | v2 | 1411 | 1411 | 1276 | 0.904323 | 244.848389 |
| OLS | sawtooth | 0-30 | v1 | 3189 | 3189 | 2695 | 0.845093 | 390.70724 |
| OLS | sawtooth | 0-30 | v2 | 3189 | 3189 | 2695 | 0.845093 | 390.70724 |
| OLS | pooled | 0-5 | v1 | 1575 | 1575 | 1409 | 0.894603 | 1803.537056 |
| OLS | pooled | 0-5 | v2 | 1575 | 1575 | 1409 | 0.894603 | 1803.537056 |
| OLS | pooled | 5-15 | v1 | 2801 | 2801 | 2460 | 0.878258 | 607.766817 |
| OLS | pooled | 5-15 | v2 | 2801 | 2801 | 2460 | 0.878258 | 607.766817 |
| OLS | pooled | 15-30 | v1 | 3211 | 3211 | 2881 | 0.897228 | 249.079862 |
| OLS | pooled | 15-30 | v2 | 3211 | 3211 | 2881 | 0.897228 | 249.079862 |
| OLS | pooled | 0-30 | v1 | 7587 | 7587 | 6750 | 0.88968 | 396.908942 |
| OLS | pooled | 0-30 | v2 | 7587 | 7587 | 6750 | 0.88968 | 396.908942 |
| Responsive | burst | 0-5 | v1 | 831 | 831 | 735 | 0.884477 | 1328.520796 |
| Responsive | burst | 0-5 | v2 | 831 | 831 | 731 | 0.879663 | 958.209375 |
| Responsive | burst | 5-15 | v1 | 674 | 674 | 542 | 0.804154 | 159.496634 |
| Responsive | burst | 5-15 | v2 | 674 | 674 | 526 | 0.780415 | 146.07629 |
| Responsive | burst | 15-30 | v1 | 30 | 30 | 8 | 0.266667 | 212.411739 |
| Responsive | burst | 15-30 | v2 | 30 | 30 | 8 | 0.266667 | 212.411739 |
| Responsive | burst | 0-30 | v1 | 1535 | 1535 | 1285 | 0.837134 | 260.834274 |
| Responsive | burst | 0-30 | v2 | 1535 | 1535 | 1265 | 0.824104 | 230.629821 |
| Responsive | creeping | 0-5 | v1 | 602 | 602 | 600 | 0.996678 | 1355.633465 |
| Responsive | creeping | 0-5 | v2 | 602 | 602 | 537 | 0.892027 | 104.584559 |
| Responsive | creeping | 5-15 | v1 | 1206 | 1206 | 1206 | 1.0 | 211.13477 |
| Responsive | creeping | 5-15 | v2 | 1206 | 1206 | 1199 | 0.994196 | 142.033041 |
| Responsive | creeping | 15-30 | v1 | 1669 | 1669 | 1587 | 0.950869 | 229.484409 |
| Responsive | creeping | 15-30 | v2 | 1669 | 1669 | 1529 | 0.916117 | 176.370034 |
| Responsive | creeping | 0-30 | v1 | 3477 | 3477 | 3393 | 0.975841 | 238.150738 |
| Responsive | creeping | 0-30 | v2 | 3477 | 3477 | 3265 | 0.939028 | 154.971584 |
| Responsive | sawtooth | 0-5 | v1 | 500 | 500 | 410 | 0.82 | 1721.6545 |
| Responsive | sawtooth | 0-5 | v2 | 500 | 500 | 369 | 0.738 | 146.766998 |
| Responsive | sawtooth | 5-15 | v1 | 1278 | 1278 | 1001 | 0.783255 | 212.897164 |
| Responsive | sawtooth | 5-15 | v2 | 1278 | 1278 | 1028 | 0.804382 | 147.605289 |
| Responsive | sawtooth | 15-30 | v1 | 1411 | 1411 | 1176 | 0.833451 | 220.644753 |
| Responsive | sawtooth | 15-30 | v2 | 1411 | 1411 | 1187 | 0.841247 | 208.634884 |
| Responsive | sawtooth | 0-30 | v1 | 3189 | 3189 | 2587 | 0.811226 | 233.340932 |
| Responsive | sawtooth | 0-30 | v2 | 3189 | 3189 | 2584 | 0.810285 | 178.543314 |
| Responsive | pooled | 0-5 | v1 | 1933 | 1933 | 1745 | 0.902742 | 1455.046586 |
| Responsive | pooled | 0-5 | v2 | 1933 | 1933 | 1637 | 0.84687 | 185.463285 |
| Responsive | pooled | 5-15 | v1 | 3158 | 3158 | 2749 | 0.870488 | 198.093057 |
| Responsive | pooled | 5-15 | v2 | 3158 | 3158 | 2753 | 0.871754 | 144.94891 |
| Responsive | pooled | 15-30 | v1 | 3110 | 3110 | 2771 | 0.890997 | 224.804591 |
| Responsive | pooled | 15-30 | v2 | 3110 | 3110 | 2724 | 0.875884 | 196.096729 |
| Responsive | pooled | 0-30 | v1 | 8201 | 8201 | 7265 | 0.885868 | 237.977412 |
| Responsive | pooled | 0-30 | v2 | 8201 | 8201 | 7114 | 0.867455 | 172.357824 |

## Stable backlog ranges

| Model | Predicted bin | Version | n | Range n | Share |
|---|---|---|---:|---:|---:|
| OLS | 0-30 | v1 | 1341 | 1341 | 1.0 |
| OLS | 0-30 | v2 | 1341 | 1341 | 1.0 |
| OLS | 30+ | v1 | 5819 | 5819 | 1.0 |
| OLS | 30+ | v2 | 5819 | 0 | 0.0 |
| OLS | all | v1 | 7160 | 7160 | 1.0 |
| OLS | all | v2 | 7160 | 1341 | 0.187291 |
| Responsive | 0-30 | v1 | 630 | 630 | 1.0 |
| Responsive | 0-30 | v2 | 630 | 630 | 1.0 |
| Responsive | 30+ | v1 | 3439 | 3439 | 1.0 |
| Responsive | 30+ | v2 | 3439 | 0 | 0.0 |
| Responsive | all | v1 | 7160 | 4069 | 0.568296 |
| Responsive | all | v2 | 7160 | 630 | 0.087989 |

## Availability / failures

| Model | Missing forecasts | Fit failures | OLS field mismatches |
|---|---:|---:|---:|
| OLS | 0 | 0 | 0 |
| Responsive | 0 | 0 | 0 |
| Holt | 40 | 0 | 0 |
| ARIMA_212 | 5691 | 5651 | 0 |
| ARIMA_211_exploratory | 896 | 856 | 0 |

Recording quality checks: {"advice_mismatches": 0, "ols_field_mismatches": 0, "passed": true, "truth_match_fraction": 1.0}
