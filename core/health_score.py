"""Health scores normalised by the maximum attainable penalty (75).

Fixed weights: critical 60, warning 25, empty/dead 15. Critical and
warning statuses are mutually exclusive. Counts refer to group-topic pairs.
"""


def _components(results):
    total = len(results)
    critical = sum(r.status == "CRITICAL" for r in results)
    warning = sum(r.status == "WARNING" for r in results)
    empty = sum(str(getattr(r, "group_state", "")).upper() in {"EMPTY", "DEAD"} for r in results)
    penalties = (round(60 * critical / total), round(25 * warning / total),
                 round(15 * empty / total)) if total else (0, 0, 0)
    score = max(0, min(100, round(100 * (1 - sum(penalties) / 75))))
    return critical, warning, empty, penalties, score


def _compute_single_score(results: list) -> int:
    return _components(results)[-1]


def _grade(score: int) -> tuple[str, str]:
    """Retourne le grade et la couleur associés au score."""
    if score >= 90: return "Excellent", "#22c55e"
    if score >= 70: return "Bon",       "#4ade80"
    if score >= 50: return "Moyen",     "#facc15"
    if score >= 30: return "Mauvais",   "#fb923c"
    return             "Critique",  "#f87171"


def _empty_score() -> dict:
    return {
        "score": 100, "grade": "Aucune donnee", "color": "#64748b",
        "total_groups": 0, "n_critical": 0, "n_warning": 0,
        "n_ok": 0, "n_empty": 0,
        "details": {
            "penalty_critical": 0, "penalty_warning": 0, "penalty_state": 0,
            "pct_critical": 0.0, "pct_warning": 0.0,
        },
        "by_cluster": [],
    }


def compute_health_score(results: list) -> dict:
    """
    Calcule le Health Score global à partir des résultats de lag.
    Utilise _compute_single_score pour les clusters — pas de recursion.
    """
    if not results:
        return _empty_score()

    total = len(results)
    n_crit, n_warn, n_empty, penalties, score = _components(results)
    penalty_critical, penalty_warning, penalty_state = penalties
    pct_crit = n_crit / total
    pct_warn = n_warn / total

    grade, color = _grade(score)

    # Scores par cluster — utilise _compute_single_score sans recursion
    cluster_map: dict[str, list] = {}
    for r in results:
        cluster_map.setdefault(r.cluster_name, []).append(r)

    by_cluster = []
    for cname, clist in cluster_map.items():
        by_cluster.append({
            "cluster_name": cname,
            "score":      _compute_single_score(clist),
            "n_critical": sum(1 for r in clist if r.status == "CRITICAL"),
            "n_warning":  sum(1 for r in clist if r.status == "WARNING"),
            "n_ok":       sum(1 for r in clist if r.status == "OK"),
            "total":      len(clist),
        })

    return {
        "score":        score,
        "grade":        grade,
        "color":        color,
        "total_groups": total,
        "n_critical":   n_crit,
        "n_warning":    n_warn,
        "n_ok":         total - n_crit - n_warn,
        "n_empty":      n_empty,
        "details": {
            "penalty_critical": penalty_critical,
            "penalty_warning":  penalty_warning,
            "penalty_state":    penalty_state,
            "pct_critical":     round(pct_crit  * 100, 1),
            "pct_warning":      round(pct_warn  * 100, 1),
        },
        "by_cluster": by_cluster,
    }