from bench.stackcmp_functional import first_firings


def test_first_firings_only_uses_target_group_and_firing_alerts():
    snapshot={"khm":[{"id":"c-g-t-scale","group_id":"g"},{"id":"c-other-t-scale","group_id":"other"}],
              "prometheus":[{"state":"pending","labels":{"consumergroup":"g","alertname":"Pending"}},
                            {"state":"firing","labels":{"consumergroup":"g","alertname":"ScaleConsumerGroup"},"activeAt":"now"}]}
    khm={};prom={}
    first_firings(snapshot,"g",5,khm,prom)
    first_firings(snapshot,"g",10,khm,prom)
    assert khm=={"scale":5}
    assert set(prom)=={"ScaleConsumerGroup"} and prom["ScaleConsumerGroup"]["observed_after_seconds"]==5
