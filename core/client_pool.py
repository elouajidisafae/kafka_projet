"""Thread-local Kafka clients; aliases retain independent clients and queries."""
import atexit
from threading import local, Lock

_local = local()
_lock = Lock()
_consumers = []


def client(factory, config, alias=""):
    pool = getattr(_local, "pool", None)
    if pool is None:
        _local.pool = pool = {}
    key = (factory, alias, tuple(sorted(config.items())))
    if key not in pool:
        pool[key] = factory(config)
        if "group.id" in config:
            with _lock:
                _consumers.append(pool[key])
    return pool[key]


def close_clients():
    with _lock:
        for consumer in _consumers:
            consumer.close()
        _consumers.clear()


atexit.register(close_clients)
