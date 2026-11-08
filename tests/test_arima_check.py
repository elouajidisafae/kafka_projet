import pytest
from bench.arima_check import choose_from_summary, DISCLOSURE


def candidate(p,q,failures,error):
    return dict(model="ARIMA",settings={"p":p,"q":q},fit_failures=failures,forecasts=100,
                pooled_crossing_error={"median":error})


def test_exploratory_choice_uses_failure_limit_and_original_criterion():
    rows=[candidate(2,2,22,10),candidate(2,1,2,20),candidate(0,0,0,30)]
    winner=choose_from_summary(dict(split="wave_1",candidates=rows))
    assert winner["settings"]=={"p":2,"q":1}
    assert "after" in DISCLOSURE and "held-out" in DISCLOSURE


def test_exploratory_limit_inclusive_and_heldout_selection_forbidden():
    row=candidate(2,1,5,20)
    assert choose_from_summary(dict(split="wave_1",candidates=[row]))==row
    with pytest.raises(ValueError,match="first-half"):
        choose_from_summary(dict(split="wave_2",candidates=[row]))
    row["fit_failures"]=6
    with pytest.raises(ValueError,match="No order"):
        choose_from_summary(dict(split="wave_1",candidates=[row]))
