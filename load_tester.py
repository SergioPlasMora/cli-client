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
        
    def _worker(self, tenant_id: str, dataset: str, count: int, rows: int = None):
        """Worker thread function"""
        client = ArrowFlightClient(self.gateway_uri)
        for _ in range(count):
            res = client.query_dataset(tenant_id, dataset, rows=rows)
            with self._lock:
                self.results.append(res)
                
    def run_load_test(self, 
                      total_requests: int, 
                      tenants: List[str], 
                      dataset: str = "sales",
                      rows: int = None) -> LoadTestResult:
        """
        Ejecuta prueba de carga usando ThreadPoolExecutor.
        (Arrow Flight client es sincrono bloqueante, por eso threads)
        """
        start_time = time.time()
        self.results = []
        
        reqs_per_tenant = total_requests // len(tenants)
        remainder = total_requests % len(tenants)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            
            # Distribuir carga entre tenants
            for i, tenant in enumerate(tenants):
                count = reqs_per_tenant + (1 if i < remainder else 0)
                if count > 0:
                    futures.append(
                        executor.submit(self._worker, tenant, dataset, count, rows)
                    )
            
            # Esperar a que terminen
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
