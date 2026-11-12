from pathlib import Path
import subprocess
import sys

import pytest
from scipy.stats import t

from core.student_t import student_t_95


def test_quantiles_against_independent_scipy_reference():
    # Every live-window sample count, small/heavy-tailed cases and large inputs.
    for degrees in list(range(1,725))+[1000,4096,10000,100000,1000000,1000001,10000000]:
        assert student_t_95(degrees)==pytest.approx(t.ppf(.95,degrees),rel=2e-10,abs=2e-10)
    assert student_t_95(1)==pytest.approx(6.313751514675,abs=1e-10)
    assert student_t_95(2)==pytest.approx(2.919985580354,abs=1e-10)


@pytest.mark.parametrize('degrees',[0,-1,True,1.5])
def test_invalid_degrees(degrees):
    with pytest.raises(ValueError): student_t_95(degrees)


def test_runtime_forecast_without_any_scipy_import():
    code='''
import sys,importlib.abc
class NoScipy(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,path=None,target=None):
        if fullname=='scipy' or fullname.startswith('scipy.'):
            raise AssertionError('Runtime tried to import scipy: '+fullname)
sys.meta_path.insert(0,NoScipy())
import interfaces.web
from core.forecasting import fit_configured_forecast
from datetime import datetime,timedelta,timezone
start=datetime(2026,1,1,tzinfo=timezone.utc)
records=[dict(recorded_at=(start+timedelta(seconds=i*5)).isoformat(),total_lag=100+i*30+(-1)**i) for i in range(100)]
forecast=fit_configured_forecast(records,'c','g','t')
assert forecast['enough_data']
assert 'eta_critical_formula_low_sec' in forecast
assert forecast['range_status']=='calibrated'
assert not any(k=='scipy' or k.startswith('scipy.') for k in sys.modules)
'''
    subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],
                   check=True,capture_output=True,text=True,timeout=30)
