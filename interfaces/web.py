"""
Interface Web — FastAPI avec support multi-cluster.
"""
import threading
import time
import yaml

from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from core.lag_calculator import compute_all_lags
from core.forecasting import forecast_all, cached_forecast_all
from core.db import init_db, save_lag, get_lag_history, get_latest_per_group, maybe_purge_old_records
from core.config_loader import CONFIG
from core.stats import get_global_stats
from core.health_score import compute_health_score
from core.recommender import get_recommendations
from core import audit
from core import __version__

app = FastAPI(title="Kafka Health Monitor", version=__version__)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
_last_health_score = {}
_last_statuses = {} # Track status per group to log transitions

def _background_collector():
    interval = CONFIG["monitor"]["refresh_interval"]
    global _last_statuses
    def cycle():
        try:
            results = compute_all_lags()
            for r in results:
                if r.total_lag >= 0:
                    save_lag(
                        r.cluster_name, r.group_id, r.topic, r.total_lag, r.status,
                        r.group_state, partition_count=r.partition_count,
                        partitions_counted=r.partitions_counted, consumer_count=r.consumer_count,
                    )
                    
                    # Audit Trail for alerts
                    key = f"{r.cluster_name}:{r.group_id}:{r.topic}"
                    old_status = _last_statuses.get(key)
                    if r.status != old_status:
                        if r.status in ["WARNING", "CRITICAL"]:
                            audit.log_event(
                                event_type="ALERT",
                                severity=r.status,
                                message=f"Alert triggered on {key}: {r.status}",
                                details={"lag": r.total_lag, "state": r.group_state}
                            )
                        elif old_status in ["WARNING", "CRITICAL"] and r.status == "OK":
                            audit.log_event(
                                event_type="ALERT",
                                severity="INFO",
                                message=f"Alert resolved on {key} (back to OK).",
                                details={"lag": r.total_lag}
                            )
                        _last_statuses[key] = r.status

            maybe_purge_old_records()
            forecast_all()
            get_recommendations(persist=True)
            # Calcul du Health Score global
            global _last_health_score
            _last_health_score = compute_health_score(results)
            from core.timing import record_cycle
            for cluster in CONFIG["clusters"]:
                record_cycle(cluster["name"])
            print(f"[health] Global score: {_last_health_score['score']}/100 ({_last_health_score['grade']})")
        except Exception as e:
            print(f"[collector] Error: {e}")
    from core.scheduling import run_cycles
    from core.timing import record_overrun
    run_cycles(cycle, interval, on_overrun=lambda elapsed: [record_overrun(c["name"]) for c in CONFIG["clusters"]])



@app.get("/api/status")
def api_status(request: Request):
    cluster = request.query_params.get("cluster")
    rows = get_latest_per_group(cluster_name=cluster)
    clusters = [c["name"] for c in CONFIG["clusters"]]
    return {
        "data": rows,
        "clusters": clusters,
        "thresholds": {
            "warning":  CONFIG["alerts"]["warning_threshold"],
            "critical": CONFIG["alerts"]["critical_threshold"],
        }
    }


@app.get("/api/history")
def api_history(
    cluster: str = Query(...),
    group: str = Query(...),
    topic: str = Query(...),
    hours: int = Query(1),
):
    records = get_lag_history(cluster, group, topic, last_hours=hours)
    return {"cluster": cluster, "group_id": group, "topic": topic, "hours": hours, "points": records}

@app.get("/api/forecast")
def api_forecast():
    """
    Retourne les prédictions de lag pour tous les groupes/topics.
    Basé sur une régression linéaire sur l'historique SQLite.
    """
    results = cached_forecast_all()
    return {"forecasts": results, "show_range": CONFIG.get("forecast", {}).get("show_range", False)}

@app.get("/api/recommendations")
def api_recommendations():
    """
    Retourne des recommandations intelligentes basées sur la tendance et l'état.
    """
    recs = get_recommendations()
    if recs:
        audit.log_event(
            event_type="RECOMMENDATION",
            severity="INFO",
            message=f"Analysis completed: {len(recs)} recommendation(s) generated.",
            details={"count": len(recs)}
        )
    return {"recommendations": recs}

@app.get("/api/config")
def get_config():
    """Retourne la configuration actuelle."""
    return {
        "clusters":    CONFIG.get("clusters", []),
        "alerts":      CONFIG.get("alerts", {}),
        "monitor":     CONFIG.get("monitor", {}),
        "exclude_topics": CONFIG.get("exclude_topics", []),
        "exclude_groups": CONFIG.get("exclude_groups", []),
    }


@app.post("/api/config")
async def save_config(request: Request):
    """
    Sauvegarde la configuration dans config.yml
    et recharge CONFIG en mémoire immédiatement.
    """
    try:
        body = await request.json()

        config_path = Path("config.yml")
        if config_path.exists():
            with open(config_path, "r") as f:
                current = yaml.safe_load(f) or {}
        else:
            current = {}

        # Met à jour uniquement les sections modifiables
        # Preserve options not exposed by the form, including group_topic_match.
        current["alerts"] = {**current.get("alerts", {}), **body.get("alerts", {})}
        current["monitor"] = {**current.get("monitor", {}), **body.get("monitor", {})}
        current["exclude_topics"] = body.get("exclude_topics", [])
        current["exclude_groups"] = body.get("exclude_groups", [])

        with open(config_path, "w") as f:
            yaml.dump(current, f, default_flow_style=False, allow_unicode=True)

        # Recharge CONFIG en mémoire sans redémarrer
        CONFIG["alerts"]  = current["alerts"]
        CONFIG["monitor"] = current["monitor"]
        CONFIG["exclude_topics"] = current["exclude_topics"]
        CONFIG["exclude_groups"] = current["exclude_groups"]

        audit.log_event(
            event_type="CONFIG_CHANGE",
            severity="INFO",
            message="Configuration updated through the web interface.",
            details=body
        )

        print(f"[config] Configuration mise a jour : {body}")
        return {"success": True}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/stats")
def api_stats():
    """Retourne les statistiques globales agrégées."""
    return get_global_stats()

@app.get("/api/health-score")
def api_health_score():
    """
    Retourne le Health Score global (0-100).
    Calculé à chaque cycle de collecte — pas de recalcul à la demande.
    """
    return _last_health_score if _last_health_score else {"score": None, "grade": "Chargement..."}


@app.get("/stats", response_class=HTMLResponse)
def stats_page(request: Request):
    """Page de statistiques globales."""
    return templates.TemplateResponse("stats.html", {"request": request})


@app.get("/config", response_class=HTMLResponse)
def config_page(request: Request):
    """Page de configuration."""
    return templates.TemplateResponse("config.html", {"request": request})

@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request):
    """Page de piste d'audit."""
    return templates.TemplateResponse("audit.html", {"request": request})

@app.get("/api/audit")
def api_audit(limit: int = 100):
    """API pour récupérer les logs d'audit."""
    return audit.fetch_logs(limit)

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    rows = get_latest_per_group()
    lines = []
    lines.append("# HELP kafka_consumer_lag Pending messages")
    lines.append("# TYPE kafka_consumer_lag gauge")
    for row in rows:
        if row["total_lag"] >= 0:
            label = f'cluster="{row["cluster_name"]}",group="{row["group_id"]}",topic="{row["topic"]}"'
            lines.append(f"kafka_consumer_lag{{{label}}} {row['total_lag']}")
    lines.append("# HELP kafka_consumer_status Status (0=OK 1=WARNING 2=CRITICAL)")
    lines.append("# TYPE kafka_consumer_status gauge")
    status_map = {"OK": 0, "WARNING": 1, "CRITICAL": 2, "ERROR": -1}
    for row in rows:
        label = f'cluster="{row["cluster_name"]}",group="{row["group_id"]}",topic="{row["topic"]}"'
        lines.append(f"kafka_consumer_status{{{label}}} {status_map.get(row['status'], -1)}")
    from core.timing import prometheus_lines
    lines.extend(prometheus_lines())
    lines.append("# HELP khm_health_score Normalised health score; absent before first collection")
    lines.append("# TYPE khm_health_score gauge")
    if _last_health_score:
        lines.append(f'khm_health_score {_last_health_score["score"]}')
    return "\n".join(lines)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.on_event("startup")
def startup():
    init_db()
    t = threading.Thread(target=_background_collector, daemon=True)
    t.start()


def run_web():
    host = CONFIG["web"]["host"]
    port = CONFIG["web"]["port"]
    print(f"Dashboard disponible sur http://localhost:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
