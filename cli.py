"""
Arrow Flight sobre WebSocket PoC - CLI
"""
import argparse
import sys
import logging
import platform

from ui import ui
from flight_client import ArrowFlightClient
from load_tester import LoadTester
from rich.console import Console

# Configurar logging silencioso para librerías, solo errores
logging.basicConfig(level=logging.ERROR)

console = Console()

def cmd_query(args):
    """Ejecuta una consulta individual"""
    ui.print_header()
    console.print(f"\nRunning single query for tenant: [bold cyan]{args.tenant}[/], dataset: [bold green]{args.dataset}[/], rows: [bold yellow]{args.rows or 'Default'}[/]")
    
    client = ArrowFlightClient(args.gateway)
    
    # Verificar salud
    if not client.check_health():
        print(f"Error: Could not connect to Gateway at {args.gateway}")
        return
    
    # Ejecutar query
    try:
        metrics = client.query_dataset(args.tenant, args.dataset, rows=args.rows)
        ui.display_single_query_results(metrics)
    except Exception as e:
        console.print(f"[bold red]Error executing query:[/bold red] {e}")


def cmd_load_test(args):
    """Ejecuta prueba de carga"""
    ui.print_header()
    
    # Generar lista de tenants
    tenants = args.tenants_list.split(",") if args.tenants_list else \
              [f"tenant_{i:03d}" for i in range(1, args.tenants_count + 1)]
    
    # Si el usuario paso un solo tenant especifico en la lista, usar ese
    # Para la PoC con 1 solo connector, usamos el ID del connector real si es posible
    # O simulamos multiples queries al mismo connector
    
    # Nota: Si solo tenemos 1 connector conectado (ej: tenant_sergio), 
    # todos los requests a otros tenants fallaran.
    # El usuario debe pasar --tenants-list tenant_sergio para probar con el connector real.
    
    print(f"\nStarting Load Test:")
    print(f"  Requests:    {args.requests}")
    print(f"  Concurrency: {args.concurrency}")
    print(f"  Tenants:     {len(tenants)} ({', '.join(tenants[:3])}...)") 
    print(f"  Gateway:     {args.gateway}")
    print("-" * 40)
    
    tester = LoadTester(args.gateway, concurrency=args.concurrency)
    metrics = tester.run_load_test(args.requests, tenants, dataset=args.dataset, rows=args.rows)
    
    ui.show_load_test_results(metrics)
    
    # Exportar JSON si se pide
    if args.json:
        import json
        with open(args.json, "w") as f:
            # Convertir metrics a dict serializable
            data = {
                "summary": {
                    "duration": metrics.duration_s,
                    "requests": metrics.total_requests,
                    "successful": metrics.successful,
                    "failed": metrics.failed,
                },
                "latency": {
                    "avg": metrics.avg_latency_ms,
                    "p95": metrics.p95_latency_ms
                }
            }
            json.dump(data, f, indent=2)
        print(f"\nResults exported to {args.json}")

def main():
    parser = argparse.ArgumentParser(description="Arrow Flight WebSocket PoC Client")
    parser.add_argument("--gateway", default="grpc://localhost:8815", help="Gateway URI")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Comando Query
    query_parser = subparsers.add_parser("query", help="Ejecuta una consulta individual")
    query_parser.add_argument("--tenant", default="tenant_default", help="ID del tenant")
    query_parser.add_argument("--dataset", default="sales", help="Nombre del dataset")
    query_parser.add_argument("--rows", type=int, default=None, help="Número de filas a solicitar (opcional)")
    query_parser.set_defaults(func=cmd_query)
    
    # Subcomando: load-test
    p_load = subparsers.add_parser("load-test", help="Run load test")
    p_load.add_argument("--requests", type=int, default=100, help="Total requests")
    p_load.add_argument("--concurrency", type=int, default=10, help="Concurrent workers")
    p_load.add_argument("--tenants-count", type=int, default=5, help="Number of simulated tenants")
    p_load.add_argument("--tenants-list", help="Comma-sep list of tenant IDs")
    p_load.add_argument("--dataset", default="sales", help="Dataset name (file in datasets/)")
    p_load.add_argument("--rows", type=int, default=None, help="Num rows per request (synthetic)")
    p_load.add_argument("--json", help="Export results to JSON file")
    p_load.set_defaults(func=cmd_load_test)
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
