"""
Interfaz de usuario con Rich
"""
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.layout import Layout
from rich import box

console = Console()

from dataclasses import dataclass
from typing import Optional

@dataclass
class QueryMetrics:
    tenant_id: str
    dataset: str
    rows: int = 0
    bytes: int = 0
    metadata_latency_ms: float = 0.0
    transfer_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    status: str = "Pending"
    error: Optional[str] = None

class CLIInterface:
    def print_header(self):
        console.print(Panel.fit(
            "[bold cyan]Arrow Flight over WebSocket PoC[/bold cyan]\n"
            "[yellow]Multi-tenant Load Tester[/yellow]",
            border_style="blue"
        ))

    def display_single_query_results(self, metrics: QueryMetrics):
        """Muestra resultado de una query individual"""
        if metrics.status == "Success":
            table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
            table.add_column("Metric", style="dim")
            table.add_column("Value")
            
            table.add_row("Status", "[green]Success[/green]")
            table.add_row("Tenant", metrics.tenant_id)
            table.add_row("Rows", f"{metrics.rows:,}")
            table.add_row("Bytes", f"{metrics.bytes/1024/1024:.2f} MB")
            table.add_row("Metadata Latency", f"{metrics.metadata_latency_ms:.2f} ms")
            table.add_row("Transfer Latency", f"{metrics.transfer_latency_ms:.2f} ms")
            table.add_row("Total Latency", f"[bold]{metrics.total_latency_ms:.2f} ms[/bold]")
            
            console.print(table)
        else:
            console.print(f"[bold red]Error doing query:[/bold red] {metrics.error}")

    def show_load_test_results(self, metrics):
        """Muestra resumen de load test"""
        console.print("\n[bold]Load Test Results[/bold]")
        
        # Tabla resumen
        table = Table(title="Summary Metrics", box=box.ROUNDED)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        table.add_row("Duration", f"{metrics.duration_s:.2f} s")
        table.add_row("Total Requests", str(metrics.total_requests))
        table.add_row("Successful", f"[green]{metrics.successful}[/green]")
        table.add_row("Failed", f"[red]{metrics.failed}[/red]")
        table.add_row("Throughput", f"{metrics.total_requests / metrics.duration_s:.2f} req/s")
        table.add_row("Total Data", f"{metrics.total_bytes / 1024 / 1024:.2f} MB")
        
        console.print(table)
        
        # Tabla latencias
        lat_table = Table(title="Latency Statistics", box=box.ROUNDED)
        lat_table.add_column("Statistic", style="yellow")
        lat_table.add_column("Time (ms)", style="bold white")
        
        lat_table.add_row("Average", f"{metrics.avg_latency_ms:.2f}")
        lat_table.add_row("P95", f"{metrics.p95_latency_ms:.2f}")
        
        console.print(lat_table)

ui = CLIInterface()
