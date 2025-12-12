"""
Generador de carga concurrente
"""
import asyncio
import time
import threading
import concurrent.futures
from typing import List, Dict
from dataclasses import dataclass

from flight_client import ArrowFlightClient

@dataclass
class LoadTestResult:
    total_requests: int
    successful: int
    failed: int
    total_rows: int
    total_bytes: int
    duration_s: float
    avg_latency_ms: float
    p95_latency_ms: float
    results: List[Dict]

class LoadTester:
    def __init__(self, gateway_uri: str, concurrency: int = 10):
        self.gateway_uri = gateway_uri
        self.concurrency = concurrency
        self.results = []
        self._lock = threading.Lock()
        
    def _single_request(self, tenant_id: str, dataset: str, rows: int = None):
        """
        Ejecuta UNA sola request con su PROPIA conexión gRPC.
        
        IGUAL QUE SSE: Cada request abre su propia conexión independiente.
        Esto permite paralelismo real a nivel de red (no multiplexación).
        """
        # Crear nuevo cliente con conexión independiente para cada request
        client = ArrowFlightClient(self.gateway_uri)
        res = client.query_dataset(tenant_id, dataset, rows=rows)
        with self._lock:
            self.results.append(res)
        return res
                
    def run_load_test(self, 
                      total_requests: int, 
                      tenants: List[str], 
                      dataset: str = "sales",
                      rows: int = None) -> LoadTestResult:
        """
        Ejecuta prueba de carga usando ThreadPoolExecutor.
        IMPORTANTE: Cada request es un task independiente para máxima concurrencia.
        """
        start_time = time.time()
        self.results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            
            # CLAVE: Cada request individual es un task independiente
            # Esto permite que el ThreadPoolExecutor maneje la concurrencia real
            for i in range(total_requests):
                tenant = tenants[i % len(tenants)]  # Round-robin entre tenants
                futures.append(
                    executor.submit(self._single_request, tenant, dataset, rows)
                )
            
            # Esperar a que terminen todas las requests
            concurrent.futures.wait(futures)
            
        duration = time.time() - start_time
        
        return self._calculate_metrics(duration)

    def _calculate_metrics(self, duration: float) -> LoadTestResult:
        """Calcula estadísticas finales"""
        # Resultados son objetos QueryMetrics
        successful = [r for r in self.results if r.status == "Success"]
        failed = [r for r in self.results if r.status != "Success"]
        
        latencies = sorted([r.total_latency_ms for r in successful])
        total_rows = sum(r.rows for r in successful)
        total_bytes = sum(r.bytes for r in successful)
        
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        p95_latency = latencies[int(len(latencies) * 0.95)] if latencies else 0
        
        return LoadTestResult(
            total_requests=len(self.results),
            successful=len(successful),
            failed=len(failed),
            total_rows=total_rows,
            total_bytes=total_bytes,
            duration_s=duration,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            results=[r.__dict__ for r in self.results] # Convert to dict for JSON export compatibility
        )
