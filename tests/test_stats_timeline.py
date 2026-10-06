"""Hand-computed backlog histories: sample count must not change backlog."""
from datetime import datetime, timezone

import pytest

from core import db, stats


@pytest.fixture
def history(tmp_path, monkeypatch):
    monkeypatch.setattr(db, 'DB_PATH', tmp_path/'history.db')
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026,10,6,8,0,tzinfo=timezone.utc)
    monkeypatch.setattr(stats,'datetime',Clock)
    db.init_db()
    def add(time, lag, group='g1', topic='t1', cluster='demo'):
        with db._get_connection() as conn:
            conn.execute('INSERT INTO lag_history(cluster_name,group_id,topic,total_lag,status,recorded_at) VALUES (?,?,?,?,?,?)',
                         (cluster,group,topic,lag,'OK',time))
    return add


def test_repeated_samples_are_not_summed(history):
    for second in range(10):
        history(f'2026-10-06T07:27:{second:02}+00:00',100)
    history('2026-10-06T07:28:00+00:00',200,group='g2')
    history('2026-10-06T07:31:00+00:00',100)
    history('2026-10-06T07:31:01+00:00',200,group='g2')
    assert stats.get_lag_timeline()==[
        dict(cluster_name='demo',bucket='2026-10-06T07:28:00+00:00',total_lag=300),
        dict(cluster_name='demo',bucket='2026-10-06T07:31:01+00:00',total_lag=300)]


def test_carry_forward_and_real_reduction_without_filling_empty_intervals(history):
    history('2026-10-06T07:26:00+00:00',100)
    history('2026-10-06T07:27:00+00:00',200,group='g2')
    history('2026-10-06T07:31:00+00:00',50)
    history('2026-10-06T07:41:00+00:00',0,group='g2')
    points=stats.get_lag_timeline()
    assert [p['total_lag'] for p in points]==[300,250,50]
    assert [p['bucket'] for p in points]==['2026-10-06T07:27:00+00:00','2026-10-06T07:31:00+00:00','2026-10-06T07:41:00+00:00']


def test_latest_sample_ties_and_pair_cluster_identity(history):
    time='2026-10-06T07:29:59+00:00'
    history(time,1000)
    history(time,10)  # latest inserted row wins equal timestamps
    history(time,20,topic='t2')
    history(time,30,group='g2')
    history(time,70,cluster='other')
    assert {p['cluster_name']:p['total_lag'] for p in stats.get_lag_timeline()}=={'demo':60,'other':70}


def test_empty_expired_and_invalid_measurements(history):
    assert stats.get_lag_timeline()==[]
    history('2026-10-05T07:59:59+00:00',5000)
    history('2026-10-06T07:28:00+00:00',-1)
    assert stats.get_lag_timeline()==[]
    history('2026-10-06T07:29:59+00:00',25)
    history('2026-10-06T07:30:00+00:00',30)
    assert [p['total_lag'] for p in stats.get_lag_timeline()]==[25,30]
