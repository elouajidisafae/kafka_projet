"""Collection-cycle persistence for forecast-driven rules, shared with replay."""
from threading import RLock

FORECAST_RULES = {'scale', 'topology'}


class AdvicePersistence:
    def __init__(self):
        self.counts = {}
        self.last_cycle = None
        self.lock = RLock()

    @staticmethod
    def key(row):
        rule = row.get('rule') or row['id'].rsplit('-',1)[-1]
        return row.get('cluster_name',''), row['group_id'], row['topic'], rule

    def apply(self, matches, minimum=1, *, advance=False, cycle=None, eligible_pairs=None):
        minimum = int(minimum)
        if minimum < 1:
            raise ValueError('forecast_persistence_cycles must be positive')
        with self.lock:
            active = {self.key(r) for r in matches if self.key(r)[-1] in FORECAST_RULES
                      and (eligible_pairs is None or self.key(r)[:3] in eligible_pairs)}
            if advance and (cycle is None or cycle != self.last_cycle):
                self.counts = {key:self.counts.get(key,0)+1 for key in active}
                self.last_cycle = cycle
            # N=1 preserves exact historical matches, even before the first cycle.
            return [r for r in matches if minimum == 1 or self.key(r)[-1] not in FORECAST_RULES
                    or self.key(r) in active and self.counts.get(self.key(r),0) >= minimum]


def persist_stream(rows, minimum):
    """Independent per-pair cycles; API duplicates must be removed by the caller."""
    states = {}
    output = []
    for row in sorted(rows,key=lambda r:(r['recorded_at'],int(r['id']))):
        pair=(row.get('cluster_name',''),row['group_id'],row['topic'])
        state=states.setdefault(pair,AdvicePersistence())
        copied=dict(row)
        copied['advice']=state.apply(row['advice'],minimum,advance=True,cycle=row['id'])
        output.append(copied)
    return output
