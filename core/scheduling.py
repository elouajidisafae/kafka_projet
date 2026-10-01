"""Serial fixed-period scheduling: overruns start the next cycle immediately."""
from time import monotonic, sleep


def run_cycles(cycle, interval, stopped=lambda: False, on_overrun=lambda elapsed: None,
               clock=monotonic, wait=sleep):
    while not stopped():
        start = clock()
        cycle()
        elapsed = clock() - start
        if elapsed > interval:
            on_overrun(elapsed)
        else:
            wait(max(0.0, interval - elapsed))
