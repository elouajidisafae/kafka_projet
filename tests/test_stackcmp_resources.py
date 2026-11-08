from bench.stackcmp_resources import schedule,setup_metadata
from bench.stackcmp_common import SETUPS


def test_resource_schedule_has_five_repetitions_and_rotates_sizes_and_setups():
    rows=schedule()
    assert len(rows)==15
    assert [size for rep,size,_ in rows if rep==0]==[10,100,500]
    assert [size for rep,size,_ in rows if rep==1]==[100,500,10]
    for size in [10,100,500]: assert sum(n==size for _,n,_ in rows)==5
    assert all(set(setups)==set(SETUPS) for _,_,setups in rows)
    assert len({tuple(order) for _,_,order in rows})==3


def test_image_accounting_deduplicates_image_ids():
    prep={"images":{name:{"id":"same-id","logical_size_bytes":100} for name in SETUPS["prometheus"]}}
    result=setup_metadata("prometheus",prep)
    assert result["containers"]==2 and result["logical_image_bytes"]==100
    assert "not exclusive physical" in result["image_size_definition"]
