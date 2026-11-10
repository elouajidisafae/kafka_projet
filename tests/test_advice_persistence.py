from core.advice_persistence import AdvicePersistence


def match(rule='scale'):
    return dict(id='c-g-t-'+rule,cluster_name='c',group_id='g',topic='t')


def test_one_is_exact_and_polls_do_not_advance():
    state=AdvicePersistence();rows=[match(),match('stalled')]
    assert state.apply(rows,1)==rows
    assert state.apply(rows,3,advance=True,cycle=1)==[rows[1]]
    for _ in range(20):assert state.apply(rows,3)==[rows[1]]
    assert state.apply(rows,3,advance=True,cycle=1)==[rows[1]]
    assert state.apply(rows,3,advance=True,cycle=2)==[rows[1]]
    assert state.apply(rows,3,advance=True,cycle=3)==rows
    state.apply([],3,advance=True,cycle=4)
    assert state.apply(rows,3,advance=True,cycle=5)==[rows[1]]


def test_rules_and_pairs_count_independently_and_missing_pair_resets():
    state=AdvicePersistence();rows=[match(),match('topology')]
    state.apply(rows,2,advance=True,cycle=1)
    assert state.apply(rows,2,advance=True,cycle=2)==rows
    assert state.apply(rows,2,advance=True,cycle=3,eligible_pairs=set())==[]
    assert state.apply(rows,2,advance=True,cycle=4)==[]
