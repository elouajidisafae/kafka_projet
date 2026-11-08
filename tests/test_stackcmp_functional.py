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



def test_topic_futures_keep_admin_client_alive_until_completion(monkeypatch):
    import gc
    import weakref
    import confluent_kafka.admin
    from bench.stackcmp_functional import create_scenario_topics
    completed=[]
    class Future:
        def __init__(self,owner,name): self.owner,self.name=weakref.ref(owner),name
        def result(self,timeout):
            gc.collect()
            assert self.owner() is not None, "AdminClient destroyed while its futures are pending"
            assert timeout==30
            completed.append(self.name)
    class Admin:
        def __init__(self,config): pass
        def create_topics(self,topics): return {topic.topic:Future(self,topic.topic) for topic in topics}
    monkeypatch.setattr(confluent_kafka.admin,"AdminClient",Admin)
    create_scenario_topics(["bottleneck","stranded"])
    assert completed==["bottleneck","stranded"]
