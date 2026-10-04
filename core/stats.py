"""
Statistiques globales — agrège les données SQLite pour la page /stats.

Calcule :
- Résumé par cluster (total lag, nb groupes, nb CRITICAL)
- Top 5 topics les plus problématiques
- Top 5 consumer groups les plus lents
- Evolution du lag sur les dernières 24h (série temporelle)
- Comparaison inter-clusters
"""
from datetime import datetime, timedelta, timezone
from .db import utc_now_iso, _get_connection


def get_cluster_summary() -> list[dict]:
    """
    Résumé par cluster :
    - total_lag       : somme des lags actuels
    - nb_groups       : nombre de groupes suivis
    - nb_critical     : groupes en CRITICAL
    - nb_warning      : groupes en WARNING
    - nb_ok           : groupes en OK
    - max_lag         : lag maximum observé
    - avg_lag         : lag moyen
    """
    with _get_connection() as conn:
        rows = conn.execute("""
            SELECT
                cluster_name,
                SUM(total_lag)                                    as total_lag,
                COUNT(DISTINCT group_id)                          as nb_groups,
                SUM(CASE WHEN status='CRITICAL' THEN 1 ELSE 0 END) as nb_critical,
                SUM(CASE WHEN status='WARNING'  THEN 1 ELSE 0 END) as nb_warning,
                SUM(CASE WHEN status='OK'       THEN 1 ELSE 0 END) as nb_ok,
                MAX(total_lag)                                    as max_lag,
                AVG(total_lag)                                    as avg_lag
            FROM (
                SELECT cluster_name, group_id, topic, total_lag, status,
                       MAX(recorded_at) as recorded_at
                FROM lag_history
                GROUP BY cluster_name, group_id, topic
            )
            GROUP BY cluster_name
            ORDER BY total_lag DESC
        """).fetchall()
    return [dict(row) for row in rows]


def get_top_topics(limit: int = 5) -> list[dict]:
    """
    Top N topics avec le lag cumulé le plus élevé
    (toutes partitions et tous groupes confondus).
    """
    with _get_connection() as conn:
        rows = conn.execute("""
            SELECT
                topic,
                SUM(total_lag)           as total_lag,
                COUNT(DISTINCT group_id) as nb_groups,
                MAX(total_lag)           as max_lag,
                AVG(total_lag)           as avg_lag
            FROM (
                SELECT topic, group_id, total_lag,
                       MAX(recorded_at) as recorded_at
                FROM lag_history
                GROUP BY topic, group_id
            )
            GROUP BY topic
            ORDER BY total_lag DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(row) for row in rows]


def get_top_groups(limit: int = 5) -> list[dict]:
    """
    Top N consumer groups avec le lag cumulé le plus élevé.
    """
    with _get_connection() as conn:
        rows = conn.execute("""
            SELECT
                cluster_name,
                group_id,
                SUM(total_lag)       as total_lag,
                COUNT(topic)         as nb_topics,
                MAX(total_lag)       as max_lag,
                MAX(status)          as worst_status
            FROM (
                SELECT cluster_name, group_id, topic, total_lag, status,
                       MAX(recorded_at) as recorded_at
                FROM lag_history
                GROUP BY cluster_name, group_id, topic
            )
            GROUP BY cluster_name, group_id
            ORDER BY total_lag DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(row) for row in rows]


def get_lag_timeline(hours: int = 24) -> list[dict]:
    """
    Evolution du lag total par cluster sur les N dernières heures.
    Agrège les données par tranches de 5 minutes pour lisibilité.

    Retourne : [{cluster_name, bucket, total_lag}, ...]
    """
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    with _get_connection() as conn:
        rows = conn.execute("""
            SELECT
                cluster_name,
                SUBSTR(recorded_at, 1, 15) || '0:00' as bucket,
                SUM(total_lag)                        as total_lag
            FROM lag_history
            WHERE recorded_at >= ?
            GROUP BY cluster_name, bucket
            ORDER BY cluster_name, bucket ASC
        """, (since,)).fetchall()
    return [dict(row) for row in rows]


def get_global_stats() -> dict:
    """
    Statistiques globales agrégées sur tous les clusters.
    Point d'entrée principal pour l'endpoint /api/stats.
    """
    with _get_connection() as conn:
        total_records = conn.execute(
            "SELECT COUNT(*) as n FROM lag_history"
        ).fetchone()["n"]

        oldest = conn.execute(
            "SELECT MIN(recorded_at) as ts FROM lag_history"
        ).fetchone()["ts"]

        newest = conn.execute(
            "SELECT MAX(recorded_at) as ts FROM lag_history"
        ).fetchone()["ts"]

    return {
        "cluster_summary": get_cluster_summary(),
        "top_topics":      get_top_topics(5),
        "top_groups":      get_top_groups(5),
        "lag_timeline":    get_lag_timeline(24),
        "meta": {
            "total_records": total_records,
            "oldest_record": oldest,
            "newest_record": newest,
            "generated_at":  utc_now_iso(),
        }
    }