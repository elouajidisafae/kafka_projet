"""Fixed 95th Student-t percentile using only the Python standard library.

Invert the positive-tail identity P(T>x)=I_(v/(v+x*x))(v/2,1/2)/2.
The regularized beta uses its continued fraction and log-domain prefactor.
Only the fixed 90% two-sided formula interval needs this percentile; calibrated
ranges do not use it. Cache by integer residual degrees of freedom.
"""
from functools import lru_cache
import math
from statistics import NormalDist


def _beta_fraction(a, b, x):
    tiny=1e-300
    def nonzero(value):
        return value if abs(value)>tiny else math.copysign(tiny,value)
    c=1.0
    d=1.0/nonzero(1.0-(a+b)*x/(a+1.0))
    result=d
    for m in range(1,401):
        aa=m*(b-m)*x/((a+2*m-1)*(a+2*m))
        d=1.0/nonzero(1.0+aa*d);c=nonzero(1.0+aa/c)
        result*=d*c
        aa=-(a+m)*(a+b+m)*x/((a+2*m)*(a+2*m+1))
        d=1.0/nonzero(1.0+aa*d);c=nonzero(1.0+aa/c)
        change=d*c;result*=change
        if abs(change-1.0)<3e-14:
            return result
    raise ArithmeticError('Student-t beta fraction did not converge')


def _regularized_beta(x, a, b):
    if x<=0: return 0.0
    if x>=1: return 1.0
    prefactor=math.exp(math.lgamma(a+b)-math.lgamma(a)-math.lgamma(b)
                       +a*math.log(x)+b*math.log1p(-x))
    if x<(a+1)/(a+b+2):
        return prefactor*_beta_fraction(a,b,x)/a
    return 1-prefactor*_beta_fraction(b,a,1-x)/b


@lru_cache(maxsize=1024)
def student_t_95(degrees):
    """95th percentile for positive integer degrees of freedom."""
    if isinstance(degrees,bool) or not isinstance(degrees,int) or degrees<1:
        raise ValueError('Degrees of freedom must be a positive integer')
    # Avoid lgamma cancellation for degrees far beyond a live fit window.
    if degrees>10_000:
        z=NormalDist().inv_cdf(.95)
        return z+(z**3+z)/(4*degrees)+(5*z**5+16*z**3+3*z)/(96*degrees**2)
    lower,upper=0.0,8.0
    for _ in range(64):
        middle=(lower+upper)/2
        tail=.5*_regularized_beta(degrees/(degrees+middle*middle),degrees/2,.5)
        if tail>.05: lower=middle
        else: upper=middle
    return (lower+upper)/2
