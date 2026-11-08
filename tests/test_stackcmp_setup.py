from pathlib import Path
import yaml
from bench.stackcmp_common import IMAGES,effort,stats


def test_stack_images_are_exact_versions_and_scrape_matches_khm():
    assert all(":" in image and not image.endswith(":latest") for image in IMAGES.values())
    cfg=yaml.safe_load(Path("bench/stackcmp/prometheus.yml").read_text())
    assert cfg["global"]["scrape_interval"]==cfg["global"]["evaluation_interval"]=="5s"
    services=yaml.safe_load(Path("docker-compose.stackcmp.yml").read_text())["services"]
    assert "--storage.tsdb.retention.time=7d" in services["prometheus"]["command"]
    rules=yaml.safe_load(Path("bench/stackcmp/alerts.yml").read_text())["groups"][0]["rules"]
    assert not any("rebalance" in rule.get("alert","").lower() for rule in rules)


def test_configuration_effort_lists_every_counted_source_file():
    for setup,count in [("khm",2),("prometheus",3),("grafana",6)]:
        value=effort(setup)
        assert value["files"]==count==len(value["paths"])
        assert value["lines"]>=value["nonblank_noncomment_lines"]>0


def test_resource_sample_sums_the_same_container_stats_fields(monkeypatch):
    import json
    from bench import stackcmp_common as common
    raw="\n".join(json.dumps(dict(Name=name,MemUsage=ram+" / 2GiB",CPUPerc=cpu))
                   for name,ram,cpu in [("p","10MiB","2%"),("e","20MiB","3%")])
    monkeypatch.setattr(common,"command",lambda *a,**kw:raw)
    result=stats({"prometheus":"a","exporter":"b"})
    assert result["memory_bytes"]==30*1024**2 and result["cpu_percent"]==5



def test_named_retry_preserves_old_results_and_selects_matching_prepare(tmp_path,monkeypatch):
    import pytest
    from bench import stackcmp_common as common
    monkeypatch.setattr(common,"RESULTS",tmp_path)
    previous=tmp_path/"functional"
    previous.mkdir()
    (previous/"manifest.json").write_text("failed evidence")
    monkeypatch.setenv("STACKCMP_ATTEMPT","retry1")
    assert common.phase("functional")==tmp_path/"functional-retry1"
    assert common.phase_name("prepare")=="prepare-retry1"
    assert (previous/"manifest.json").read_text()=="failed evidence"
    with pytest.raises(FileExistsError): common.phase("functional")
    monkeypatch.setenv("STACKCMP_ATTEMPT","../escape")
    with pytest.raises(ValueError): common.phase("resources")



def test_host_broker_advertises_ipv4_explicitly():
    cfg=yaml.safe_load(Path("docker-compose.stackcmp.yml").read_text())
    assert "EXTERNAL://127.0.0.1:39092" in cfg["services"]["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"]


def test_offset_seed_error_checks_code_not_error_object_truthiness():
    import pytest
    from types import SimpleNamespace as NS
    from bench.stackcmp_common import check_committed_partitions
    class Error:
        def __init__(self,code): self.value=code
        def code(self): return self.value
        def __str__(self): return "test broker error"
    check_committed_partitions("g",[NS(partition=0,error=None),NS(partition=1,error=Error(0))])
    with pytest.raises(RuntimeError,match="g: partition 2: code=-197"):
        check_committed_partitions("g",[NS(partition=2,error=Error(-197))])



def test_offset_seeding_retries_only_transient_coordinator_errors():
    import pytest
    from types import SimpleNamespace as NS
    from confluent_kafka import KafkaError
    from bench.stackcmp_common import seed_committed_offsets
    class Consumer:
        calls=0
        def commit(self,**kwargs):
            self.calls+=1
            return [NS(partition=p,error=KafkaError(KafkaError.NOT_COORDINATOR) if self.calls==1 else None) for p in range(3)]
        def committed(self,*args,**kwargs): return [NS(partition=p,error=None,offset=0) for p in range(3)]
        def poll(self,timeout): pass
    consumer=Consumer()
    seed_committed_offsets(consumer,"g","t",retry_delay=0)
    assert consumer.calls==2
    class Denied(Consumer):
        def commit(self,**kwargs): return [NS(partition=0,error=KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))]
    with pytest.raises(RuntimeError,match="AUTHORIZATION"):
        seed_committed_offsets(Denied(),"g","t",retry_delay=0)
