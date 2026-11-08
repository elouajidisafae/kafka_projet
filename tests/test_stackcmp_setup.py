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
