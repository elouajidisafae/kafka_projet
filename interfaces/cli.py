"""
Interface CLI — affichage terminal avec Rich.
Support multi-cluster avec option --cluster.
"""
import time
import click
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich import box
from datetime import datetime, timezone

from core.lag_calculator import compute_all_lags, ConsumerGroupStatus
from core.db import init_db, save_lag, get_lag_history, purge_old_records
from core.config_loader import CONFIG

console = Console()

STATUS_STYLE = {
    "OK":       ("[green]● OK[/green]",        "green"),
    "WARNING":  ("[yellow]▲ WARNING[/yellow]",  "yellow"),
    "CRITICAL": ("[red]✖ CRITICAL[/red]",      "red"),
    "ERROR":    ("[dim]? ERROR[/dim]",          "dim"),
}


def _build_table(results: list[ConsumerGroupStatus], cluster_filter=None) -> Table:
    title = f"Kafka Health Monitor  •  {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
    if cluster_filter:
        title += f"  •  cluster: {cluster_filter}"

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan", show_lines=True)
    table.add_column("Cluster",        style="cyan",  min_width=12)
    table.add_column("Consumer Group", style="bold",  min_width=20)
    table.add_column("Topic",                         min_width=12)
    table.add_column("Partitions",     justify="center")
    table.add_column("Total Lag",      justify="right", min_width=10)
    table.add_column("Status",         justify="center", min_width=12)

    if not results:
        table.add_row("[dim]Aucun résultat[/dim]", "", "", "", "", "")
        return table

    for r in results:
        status_text, row_style = STATUS_STYLE.get(r.status, ("?", ""))
        lag_display = f"{r.total_lag:,}" if r.total_lag >= 0 else "N/A"
        table.add_row(
            r.cluster_name,
            r.group_id,
            r.topic,
            str(len(r.partitions)),
            lag_display,
            status_text,
            style=row_style if r.status == "CRITICAL" else "",
        )
    return table


def _collect_and_save() -> list[ConsumerGroupStatus]:
    results = compute_all_lags()
    for r in results:
        if r.total_lag >= 0:
            save_lag(r.cluster_name, r.group_id, r.topic, r.total_lag, r.status,
                     r.group_state, partition_count=r.partition_count,
                     partitions_counted=r.partitions_counted, consumer_count=r.consumer_count)
    purge_old_records()
    from core.forecasting import forecast_all
    forecast_all()
    return results


@click.group()
def cli():
    """Kafka Health Monitor — surveillance légère des consumer groups."""
    init_db()


@cli.command()
@click.option("--cluster", "-c", default=None, help="Filtrer par cluster (ex: production)")
def status(cluster):
    """Snapshot instantané du lag. Optionnel : --cluster nom"""
    console.print("\n[cyan]Collecting Kafka metrics...[/cyan]")
    results = _collect_and_save()
    if cluster:
        results = [r for r in results if r.cluster_name == cluster]
    console.print(_build_table(results, cluster_filter=cluster))
    from core.recommender import get_recommendations
    for rec in get_recommendations():
        if cluster and rec["cluster_name"] != cluster:
            continue
        console.print(Panel(
            f"{rec['advice']}\n{rec['action']}\nMetrics: {rec['metrics']}",
            title=f"{rec['priority']} | {rec['title']} | {rec['cluster_name']}/{rec['group_id']}/{rec['topic']}",
            expand=False))
    console.print()


@cli.command()
@click.option("--interval", "-i", default=CONFIG["monitor"]["refresh_interval"],
              show_default=True, help="Secondes entre chaque rafraîchissement.")
@click.option("--cluster", "-c", default=None, help="Filtrer par cluster")
def watch(interval: int, cluster: str):
    """Surveillance continue avec rafraîchissement automatique. Ctrl+C pour quitter."""
    console.print(
        Panel(
            f"[cyan]Mode WATCH activé[/cyan] — toutes les [bold]{interval}s[/bold]"
            + (f" — cluster: [bold]{cluster}[/bold]" if cluster else "") +
            "\n[dim]Ctrl+C pour quitter[/dim]",
            expand=False
        )
    )
    try:
        with Live(console=console, refresh_per_second=1, screen=False) as live:
            while True:
                results = _collect_and_save()
                if cluster:
                    results = [r for r in results if r.cluster_name == cluster]
                live.update(_build_table(results, cluster_filter=cluster))
                time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[dim]Surveillance arrêtée.[/dim]")


@cli.command()
@click.option("--cluster", "-c", required=True, help="Nom du cluster")
@click.option("--group",   "-g", required=True, help="ID du consumer group")
@click.option("--topic",   "-t", required=True, help="Nom du topic")
@click.option("--hours",   "-H", default=1, show_default=True, help="Heures d'historique")
def history(cluster: str, group: str, topic: str, hours: int):
    """Historique du lag pour un cluster/groupe/topic donné."""
    records = get_lag_history(cluster, group, topic, last_hours=hours)

    if not records:
        console.print(
            f"[yellow]Aucun historique pour[/yellow] "
            f"[bold]{cluster}[/bold] / [bold]{group}[/bold] / [bold]{topic}[/bold]"
        )
        return

    table = Table(
        title=f"Historique : {cluster} → {group} / {topic} (dernières {hours}h)",
        box=box.SIMPLE_HEAVY, header_style="bold magenta",
    )
    table.add_column("Timestamp (UTC)", min_width=20)
    table.add_column("Total Lag",       justify="right")
    table.add_column("Status",          justify="center")

    for row in records:
        status_text, _ = STATUS_STYLE.get(row["status"], ("?", ""))
        table.add_row(
            row["recorded_at"][:19].replace("T", " "),
            f"{row['total_lag']:,}",
            status_text,
        )
    console.print(table)


def run_cli():
    """Point d'entrée appelé par main.py en mode CLI."""
    cli(standalone_mode=False)