from bench.capture_forecast_demo import SCENARIOS, capture_ready


def test_established_capture_requires_selected_history_and_range():
    topic = 'figure'
    advice = dict(recommendations=[dict(topic=topic, id=topic+'-scale')])
    fitted = dict(enough_data=True, eta_critical_low_sec=500, eta_critical_high_sec=900,
                  window_seconds=599)
    assert SCENARIOS['established']['produce'] == 15
    assert not capture_ready(fitted, advice, topic, 'established')
    assert capture_ready(fitted, advice, topic, 'fast')
    fitted['window_seconds'] = 600
    assert capture_ready(fitted, advice, topic, 'established')
    fitted['eta_critical_high_sec'] = None
    assert not capture_ready(fitted, advice, topic, 'established')
