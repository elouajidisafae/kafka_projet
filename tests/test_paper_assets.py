import json
from bench.capture_audit_demo import has_alert

def test_audit_alert_requires_real_matching_topic_severity_and_lag():
    logs=[dict(event_type='ALERT',severity='WARNING',message='Alert triggered on demo:g:t: WARNING',
               details=json.dumps({'lag':5900}))]
    assert has_alert(logs,'demo:g:t','WARNING',5900)
    assert not has_alert(logs,'different','WARNING',5900)
    assert not has_alert(logs,'demo:g:t','CRITICAL',5900)
    assert not has_alert(logs,'demo:g:t','WARNING',10900)
