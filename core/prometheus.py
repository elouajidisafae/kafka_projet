"""Prometheus presentation of existing forecast and recommendation outputs."""
import json
import math

RULES = dict(scale='scale_consumers', topology='review_partitions',
             stranded='stranded_messages', stalled='stalled_processing',
             rebalance='stuck_rebalance')


def _labels(cluster, group, topic, **extra):
    values = dict(cluster=cluster, group=group, topic=topic, **extra)
    return ','.join(f'{key}={json.dumps(value, ensure_ascii=False)}' for key,value in values.items())


def forecast_advice_lines(recommendations, forecasts, eligible):
    lines = ['# HELP khm_recommendation_active Existing active KHM advice (1); absent when inactive',
             '# TYPE khm_recommendation_active gauge']
    seen = set()
    for rec in recommendations:
        key = rec['cluster_name'], rec['group_id'], rec['topic']
        rule = RULES.get(rec['id'].rsplit('-', 1)[-1])
        if key not in eligible or rule is None:
            continue
        labels = _labels(*key, rule=rule, priority=rec['priority'])
        if labels not in seen:
            lines.append(f'khm_recommendation_active{{{labels}}} 1')
            seen.add(labels)
    for suffix, field in [('', 'eta_critical_sec'), ('_low', 'eta_critical_low_sec'),
                          ('_high', 'eta_critical_high_sec')]:
        name = f'khm_forecast_critical_eta{suffix}_seconds'
        lines.extend([f'# HELP {name} Projected CRITICAL crossing seconds; absent when unavailable',
                      f'# TYPE {name} gauge'])
        for key, forecast in forecasts.items():
            value = forecast.get(field)
            if (key in eligible and forecast.get('enough_data') and value is not None
                    and math.isfinite(value) and value >= 0):
                lines.append(f'{name}{{{_labels(*key)}}} {value}')
    return lines
