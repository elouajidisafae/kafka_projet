"""Browser presentation checks; no Kafka, network or live database required.

Optional Playwright and installed Microsoft Edge are used when available.
"""
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import pytest
from core.health_score import compute_health_score
from core.lag_calculator import ConsumerGroupStatus, PartitionLag


@pytest.fixture(scope="module")
def browser():
    sync = pytest.importorskip("playwright.sync_api")
    with sync.sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch(channel="msedge", headless=True, timeout=20000)
        except sync.Error as exc:
            pytest.skip(f"Installed Edge required for browser presentation checks: {exc}")
        yield browser
        browser.close()


def pair(group,lag=0,status=None):
    return ConsumerGroupStatus("demo",group,"orders",[PartitionLag(0,lag,0,lag)] if status is None else [],
                               status=status or "OK",group_state="EMPTY" if group=="payments" else "STABLE",
                               partition_count=1)


@pytest.mark.parametrize("theme", ["light","dark"])
@pytest.mark.parametrize("cluster_count", [1,2])
def test_overview_panel_counts_filters_and_themes(browser,theme,cluster_count):
    root=Path(__file__).resolve().parents[1]
    values=[pair("payments",12000),pair("warning",1500),pair("healthy"),pair("broken",status="ERROR"),pair("new",status="UNKNOWN")]
    states={"demo":{"reachable":True}}
    rows=[dict(asdict(r),timestamp=r.timestamp.isoformat(),stale=False,recorded_at=r.timestamp.isoformat()) for r in values]
    if cluster_count==2:
        states["offline"]={"reachable":False,"last_successful_collection":"2026-11-06T12:00:00+00:00"}
        rows.append(dict(rows[0],cluster_name="offline",group_id="old-pair",stale=True))
    payload={"data":rows,"cluster_status":states,"clusters":list(states)}
    health={"value":compute_health_score(values,states)}
    def handle(route):
        parsed=urlparse(route.request.url)
        path=parsed.path
        if path=="/":
            route.fulfill(body=(root/"templates/dashboard.html").read_bytes(),content_type="text/html; charset=utf-8")
        elif path.startswith("/static/"):
            route.fulfill(body=(root/path.lstrip("/")).read_bytes(),content_type="text/css" if path.endswith(".css") else "application/javascript")
        elif path=="/api/status":
            selected=parse_qs(parsed.query).get("cluster",[None])[0]
            route.fulfill(json=dict(payload,data=[r for r in payload["data"] if selected is None or r["cluster_name"]==selected]))
        elif path=="/api/health-score": route.fulfill(json=health["value"])
        elif path=="/api/forecast": route.fulfill(json={"forecasts":[]})
        elif path=="/api/recommendations": route.fulfill(json={"recommendations":[]})
        else: route.fulfill(body="",content_type="application/javascript")
    context=browser.new_context(viewport={"width":1440,"height":1100})
    context.add_init_script(f"localStorage.setItem('khm-theme', '{theme}')")
    page=context.new_page()
    page.set_default_timeout(5000)
    page.set_default_navigation_timeout(10000)
    errors=[]
    page.on("pageerror",lambda error:errors.append(str(error)))
    page.route("**/*",handle)
    try:
        page.goto("http://dashboard.test/",wait_until="networkidle")
        page.wait_for_function("document.getElementById('score-value').textContent === '56'")
        assert page.locator(".cards .card").count()==5
        assert [page.locator("#"+id).inner_text() for id in ["c-total","c-crit","c-warn","c-stopped","c-monitoring"]]==[str(4+cluster_count),"1","1","1",str(1+cluster_count)]
        assert page.locator("#score-detail").inner_text()==f"1 stopped group with 12,000 waiting messages + {cluster_count + 2} more"
        assert page.locator(".cluster-monitor-count").inner_text()==f"{cluster_count} {'cluster' if cluster_count==1 else 'clusters'} monitored"
        assert page.locator(".cluster-summary-line").count()==(2 if cluster_count==2 else 0)
        assert page.locator("#active-alert-total, #active-alert-dialog").count()==0
        expected={"critical":["payments"],"warning":["warning"],"stopped":["payments"],
                  "monitoring":["broken","new"]+(["old-pair"] if cluster_count==2 else [])}
        for panel,names in expected.items():
            button=page.locator(f"[data-filter='{panel}']")
            button.click()
            assert button.get_attribute("aria-pressed")=="true"
            assert "active-filter" in button.get_attribute("class")
            assert page.locator("#tbody tr").count()==len(names)
            for name in names: assert name in page.locator("#tbody").inner_text()
            button.click()
            assert page.locator("[data-filter='all']").get_attribute("aria-pressed")=="true"
            assert page.locator("#tbody tr").count()==len(rows)
        page.locator("[data-filter='critical']").click()
        page.locator("[data-filter='all']").click()
        assert page.locator("#tbody tr").count()==len(rows)
        if cluster_count==2:
            page.locator("[data-filter='monitoring']").click()
            page.locator(".tab").filter(has_text="offline").click()
            page.wait_for_function("document.getElementById('c-total').textContent === '1'")
            assert page.locator("#c-monitoring").inner_text()=="1"
            assert "old-pair" in page.locator("#tbody").inner_text()
            assert "STALE" in page.locator("#tbody").inner_text()
            page.locator("[data-filter='critical']").click()
            assert "No pairs match" in page.locator("#tbody").inner_text()
            page.locator(".tab").filter(has_text="All clusters").click()
            page.wait_for_function("document.getElementById('c-total').textContent === '6'")
            page.locator("[data-filter='all']").click()
        output=root/"artifacts/dashboard-simplification"
        output.mkdir(parents=True,exist_ok=True)
        page.screenshot(path=str(output/f"{theme}-{cluster_count}-clusters.png"),full_page=True,animations="disabled")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        # One critical/stopped pair has two reasons but no other problem item.
        health["value"]=compute_health_score([values[0]],{"demo":{"reachable":True}})
        page.evaluate("refreshHealthScore()")
        assert page.locator("#score-detail").inner_text()=="1 stopped group with 12,000 waiting messages"
        # Every panel remains visible at zero; clearing data must not retain old counts.
        payload.update(data=[],cluster_status={name:{"reachable":True} for name in states})
        health["value"]=compute_health_score([],{name:{"reachable":True} for name in states})
        page.evaluate("async () => { await refresh(); await refreshHealthScore(); }")
        assert all(page.locator('#'+id).inner_text()=="0" for id in ["c-total","c-crit","c-warn","c-stopped","c-monitoring"])
        assert all(page.locator(f"[data-filter='{name}']").is_visible() for name in ["all","critical","warning","stopped","monitoring"])
        assert page.locator("#score-detail").inner_text()=="No active problems"
        assert page.locator("#score-value").inner_text()=="No data"
        assert not errors
    finally:
        context.close()
