"""Short checks of provenance guards, aggregation, and capture evidence."""
import json
from copy import deepcopy
import yaml

import pytest
from fastapi.testclient import TestClient

from bench import paper_common as protocol
from bench.capture import audit_excerpt
from core import timing
from interfaces import web


@pytest.mark.parametrize('head,dirty', [('other', ''), ('frozen', ' M README.md')])
def test_freeze_rejects_mismatched_or_dirty_source(monkeypatch, head, dirty):
    monkeypatch.setattr(protocol, 'command', lambda *args: head if args[1]=='rev-parse' else dirty)
    with pytest.raises(ValueError, match='clean working tree'):
        protocol.frozen_commit('frozen')


def test_completed_cycle_metrics_are_not_mixed_with_next_collection(monkeypatch):
    monkeypatch.setattr(timing, '_values', {})
    timing.record_collection('bench', 2, 10)
    timing.record_forecast('bench', .5, 10)
    timing.record_cycle('bench')
    timing.record_collection('bench', 99, 100)
    text='\n'.join(timing.prometheus_lines())
    assert protocol.metric(text, 'khm_collection_duration_seconds')==2
    assert protocol.metric(text, 'khm_forecast_duration_seconds')==.5
    assert protocol.metric(text, 'khm_collection_cycles_total')==1
    assert protocol.metric(text, 'khm_monitored_pairs')==10
    with pytest.raises(ValueError):
        protocol.metric(text, 'khm_collection_duration_seconds', 'other')


def test_metrics_exports_health_only_after_collection(monkeypatch):
    monkeypatch.setattr(web, 'get_latest_per_group', lambda: [])
    monkeypatch.setattr(web, '_last_health_score', {})
    client=TestClient(web.app)
    assert '\nkhm_health_score ' not in client.get('/metrics').text
    monkeypatch.setattr(web, '_last_health_score', {'score':20})
    assert protocol.metric(client.get('/metrics').text,'khm_health_score')==20


def test_repetition_summary_keeps_missing_values_and_rotation():
    rows=[{'tool':'a','value':v} for v in [None,1,2,3,4]]
    result=protocol.summarise(rows,['tool'],['value'])[0]['value']
    assert result['n']==4 and result['median']==2.5
    assert protocol.summarise([{'tool':'a'}],['tool'],['value'])[0]['value']['median'] is None
    assert [protocol.rotated([10,100,500],i) for i in range(3)]==[
        [10,100,500],[100,500,10],[500,10,100]]
    assert protocol.byte_quantity('1.5GiB')==1.5*1024**3


def test_audit_excerpt_requires_real_ordered_transitions():
    start='2026-10-05T00:00:00+00:00'
    rows=[dict(id=i,recorded_at='2026-10-05T00:00:01+00:00',event_type=kind,
               severity=severity,message=message,details=details)
          for i,kind,severity,message,details in [
              (1,'CONFIG_CHANGE','INFO','Configuration updated',json.dumps({'alerts':{'warning_threshold':5000}})),
              (2,'ALERT','INFO','s3 back to OK',None),
              (3,'ALERT','WARNING','s3 WARNING',None),
              (4,'ALERT','CRITICAL','s3 CRITICAL',None)]]
    assert audit_excerpt(list(reversed(rows)),'s3',start)==rows
    with pytest.raises(ValueError,match='Expected resolved'):
        audit_excerpt(rows[:3],'s3',start)
    with pytest.raises(ValueError,match='predates'):
        audit_excerpt(rows,'s3','2026-10-06T00:00:00+00:00')


def test_threshold_form_preserves_unexposed_monitor_options(tmp_path, monkeypatch):
    config=deepcopy(web.CONFIG)
    config['monitor']['group_topic_match']=True
    monkeypatch.setattr(web,'CONFIG',config)
    monkeypatch.chdir(tmp_path)
    path=tmp_path/'config.yml';path.write_text(yaml.safe_dump(config),encoding='utf-8')
    events=[]
    monkeypatch.setattr(web.audit,'log_event',lambda **event:events.append(event))
    body={'alerts':{'warning_threshold':5000,'critical_threshold':10000},
          'monitor':{'refresh_interval':5,'history_retention_days':7}}
    response=TestClient(web.app).post('/api/config',json=body)
    assert response.json()=={'success':True}
    assert web.CONFIG['monitor']['group_topic_match'] is True
    assert yaml.safe_load(path.read_text())['monitor']['group_topic_match'] is True
    assert events[0]['details']==body
    assert events[0]['event_type']=='CONFIG_CHANGE'
