#!/usr/bin/env python3
"""
Script de pruebas de carga para el Enrutador.
Simula múltiples usuarios concurrentes ejecutando requests de los 3 patrones.
"""
import argparse
import asyncio
import time
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict
import statistics

# Importar del app_cli existente
sys.path.append(str(Path(__file__).parent.parent))

try:
    from api_client import APIClient, DatasetResponse
    from metrics import MetricsCollector
    from logger import setup_logger
except ImportError:
    print("Error: No se puede importar desde app_cli")
    print("Verifica que ../app_cli esté disponible")
    sys.exit(1)

# cc-28-aa-cd-5c-74
@dataclass
class LoadTestConfig:
    """Configuración de la prueba de carga."""
    enrutador_url: str = "http://localhost:8000"
    mac_address: str = "cc-28-aa-cd-5c-74"
    concurrent_users: int = 10
    requests_per_user: int = 1
    pattern: str = "A"  # A, B, C, or "all"
    dataset_name: str = "dataset_1kb.json"
    timeout: int = 60
    ramp_up_seconds: int = 0  # Tiempo para escalonar usuarios


@dataclass
class LoadTestResult:
    """Resultado agregado de la prueba de carga."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_duration_seconds: float = 0.0
    
    # Métricas de TTFB (Time To First Byte)
    ttfb_values: List[float] = field(default_factory=list)
    ttfb_min: float = 0.0
    ttfb_max: float = 0.0
    ttfb_avg: float = 0.0
    ttfb_p50: float = 0.0
    ttfb_p90: float = 0.0
    ttfb_p95: float = 0.0
    ttfb_p99: float = 0.0
    
    # Throughput
    throughput_values: List[float] = field(default_factory=list)
    throughput_avg: float = 0.0
    
    # Bytes transferidos
    total_bytes: int = 0
    
    # Requests por segundo
    requests_per_second: float = 0.0
    
    def calculate_statistics(self):
        """Calcula estadísticas agregadas."""
        if self.ttfb_values:
            self.ttfb_min = min(self.ttfb_values)
            self.ttfb_max = max(self.ttfb_values)
            self.ttfb_avg = statistics.mean(self.ttfb_values)
            self.ttfb_p50 = statistics.median(self.ttfb_values)
            
            sorted_ttfb = sorted(self.ttfb_values)
            self.ttfb_p90 = sorted_ttfb[int(len(sorted_ttfb) * 0.90)] if len(sorted_ttfb) > 0 else 0
            self.ttfb_p95 = sorted_ttfb[int(len(sorted_ttfb) * 0.95)] if len(sorted_ttfb) > 0 else 0
            self.ttfb_p99 = sorted_ttfb[int(len(sorted_ttfb) * 0.99)] if len(sorted_ttfb) > 0 else 0
        
        if self.throughput_values:
            self.throughput_avg = statistics.mean(self.throughput_values)
        
        if self.total_duration_seconds > 0:
            self.requests_per_second = self.total_requests / self.total_duration_seconds


def execute_request_pattern_a(client: APIClient, config: LoadTestConfig) -> DatasetResponse:
    """Ejecuta una request del Patrón A (Buffering)."""
    return client.request_dataset_sync(
        mac_address=config.mac_address,
        dataset_name=config.dataset_name,
        timeout=config.timeout
    )


def execute_request_pattern_b(client: APIClient, config: LoadTestConfig) -> DatasetResponse:
    """Ejecuta una request del Patrón B (Streaming)."""
    return client.request_dataset_stream(
        mac_address=config.mac_address,
        dataset_name=config.dataset_name
    )


def execute_request_pattern_c(client: APIClient, config: LoadTestConfig) -> DatasetResponse:
    """Ejecuta una request del Patrón C (Offloading)."""
    return client.request_dataset_offload(
        mac_address=config.mac_address,
        dataset_name=config.dataset_name,
        timeout=config.timeout
    )


def worker_thread(user_id: int, config: LoadTestConfig, start_delay: float = 0) -> List[DatasetResponse]:
    """
    Thread worker que simula un usuario ejecutando N requests.
    
    Args:
        user_id: ID del usuario simulado
        config: Configuración de la prueba
        start_delay: Delay antes de comenzar (para ramp-up)
    
    Returns:
        Lista de respuestas
    """
    if start_delay > 0:
        time.sleep(start_delay)
    
    client = APIClient(
        base_url=config.enrutador_url,
        timeout=config.timeout
    )
    
    results = []
    
    for i in range(config.requests_per_user):
        try:
            if config.pattern == "A":
                response = execute_request_pattern_a(client, config)
            elif config.pattern == "B":
                response = execute_request_pattern_b(client, config)
            elif config.pattern == "C":
                response = execute_request_pattern_c(client, config)
            else:
                # Rotar entre patrones
                pattern_index = (user_id + i) % 3
                if pattern_index == 0:
                    response = execute_request_pattern_a(client, config)
                elif pattern_index == 1:
                    response = execute_request_pattern_b(client, config)
                else:
                    response = execute_request_pattern_c(client, config)
            
            results.append(response)
            
        except Exception as e:
            print(f"[User {user_id}] Error: {e}")
            results.append(DatasetResponse(
                request_id="",
                status="error",
                error_message=str(e)
            ))
    
    return results


def run_load_test(config: LoadTestConfig, logger) -> LoadTestResult:
    """
    Ejecuta la prueba de carga con múltiples usuarios concurrentes.
    
    Args:
        config: Configuración de la prueba
        logger: Logger
    
    Returns:
        Resultado agregado
    """
    logger.info(f"🚀 Iniciando prueba de carga")
    logger.info(f"   Usuarios concurrentes: {config.concurrent_users}")
    logger.info(f"   Requests por usuario: {config.requests_per_user}")
    logger.info(f"   Patrón: {config.pattern}")
    logger.info(f"   Dataset: {config.dataset_name}")
    logger.info(f"   Ramp-up: {config.ramp_up_seconds}s")
    
    result = LoadTestResult()
    all_responses: List[DatasetResponse] = []
    
    # Calcular delay entre usuarios para ramp-up
    if config.ramp_up_seconds > 0 and config.concurrent_users > 1:
        delay_per_user = config.ramp_up_seconds / config.concurrent_users
    else:
        delay_per_user = 0
    
    # Timestamp de inicio
    start_time = time.perf_counter()
    
    # Ejecutar usuarios en paralelo con ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=config.concurrent_users) as executor:
        futures = []
        
        for user_id in range(config.concurrent_users):
            start_delay = user_id * delay_per_user
            future = executor.submit(worker_thread, user_id, config, start_delay)
            futures.append(future)
        
        # Recolectar resultados conforme completan
        completed_users = 0
        for future in as_completed(futures):
            try:
                responses = future.result()
                all_responses.extend(responses)
                completed_users += 1
                
                # Mostrar progreso
                if completed_users % max(1, config.concurrent_users // 10) == 0:
                    logger.info(f"   Progreso: {completed_users}/{config.concurrent_users} usuarios completados")
                
            except Exception as e:
                logger.error(f"   Error en worker: {e}")
    
    # Timestamp de fin
    end_time = time.perf_counter()
    result.total_duration_seconds = end_time - start_time
    
    # Procesar resultados
    result.total_requests = len(all_responses)
    
    for response in all_responses:
        if response.status == "completed":
            result.successful_requests += 1
            
            # TTFB
            if response.t0_sent and response.t4_received:
                ttfb = response.t4_received - response.t0_sent
                result.ttfb_values.append(ttfb)
            
            # Bytes
            if response.data_size_bytes:
                result.total_bytes += response.data_size_bytes
                
                # Throughput
                if response.t0_sent and response.t4_received:
                    duration = response.t4_received - response.t0_sent
                    if duration > 0:
                        throughput = response.data_size_bytes / duration
                        result.throughput_values.append(throughput)
        else:
            result.failed_requests += 1
    
    # Calcular estadísticas
    result.calculate_statistics()
    
    return result


def print_results(result: LoadTestResult, config: LoadTestConfig):
    """Imprime resultados de la prueba."""
    print("\n" + "=" * 80)
    print("📊 RESULTADOS DE LA PRUEBA DE CARGA")
    print("=" * 80)
    
    print(f"\n🔧 Configuración:")
    print(f"   Usuarios concurrentes: {config.concurrent_users}")
    print(f"   Requests por usuario:  {config.requests_per_user}")
    print(f"   Patrón:                {config.pattern}")
    print(f"   Dataset:               {config.dataset_name}")
    
    print(f"\n📈 Resultados Generales:")
    print(f"   Total requests:        {result.total_requests}")
    print(f"   Exitosos:              {result.successful_requests} ({result.successful_requests/result.total_requests*100:.1f}%)")
    print(f"   Fallidos:              {result.failed_requests} ({result.failed_requests/result.total_requests*100:.1f}%)")
    print(f"   Duración total:        {result.total_duration_seconds:.2f}s")
    print(f"   Requests/segundo:      {result.requests_per_second:.2f}")
    
    if result.ttfb_values:
        print(f"\n⏱️  TTFB (Time To First Byte):")
        print(f"   Min:  {result.ttfb_min:.3f}s")
        print(f"   Max:  {result.ttfb_max:.3f}s")
        print(f"   Avg:  {result.ttfb_avg:.3f}s")
        print(f"   P50:  {result.ttfb_p50:.3f}s")
        print(f"   P90:  {result.ttfb_p90:.3f}s")
        print(f"   P95:  {result.ttfb_p95:.3f}s")
        print(f"   P99:  {result.ttfb_p99:.3f}s")
    
    if result.throughput_values:
        print(f"\n📦 Throughput:")
        print(f"   Avg:         {result.throughput_avg:,.0f} bytes/s")
        print(f"   Avg (MB/s):  {result.throughput_avg/1024/1024:.2f} MB/s")
    
    if result.total_bytes > 0:
        print(f"\n💾 Datos Transferidos:")
        print(f"   Total:  {result.total_bytes:,} bytes ({result.total_bytes/1024/1024:.2f} MB)")
    
    print("\n" + "=" * 80 + "\n")


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description="Prueba de carga para el Enrutador",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # 100 usuarios concurrentes, Patrón A
  python load_test.py --users 100 --pattern A --dataset dataset_1mb.csv
  
  # 1000 usuarios, todos los patrones mezclados
  python load_test.py --users 1000 --pattern all --dataset dataset_10mb.csv
  
  # 500 usuarios, con ramp-up de 10 segundos
  python load_test.py --users 500 --pattern B --ramp-up 10
        """
    )
    
    parser.add_argument("--url", default="http://localhost:8000", help="URL del Enrutador")
    parser.add_argument("--mac", default="cc-28-aa-cd-5c-74", help="MAC address del Conector")
    parser.add_argument("--users", "-u", type=int, default=10, help="Número de usuarios concurrentes")
    parser.add_argument("--requests", "-r", type=int, default=1, help="Requests por usuario")
    parser.add_argument("--pattern", "-p", choices=["A", "B", "C", "all"], default="A", 
                        help="Patrón a probar (A=Buffering, B=Streaming, C=Offloading, all=mezclado)")
    parser.add_argument("--dataset", "-d", default="dataset_1kb.json", help="Nombre del dataset")
    parser.add_argument("--timeout", "-t", type=int, default=60, help="Timeout en segundos")
    parser.add_argument("--ramp-up", type=int, default=0, help="Tiempo de ramp-up en segundos")
    
    args = parser.parse_args()
    
    # Configuración
    config = LoadTestConfig(
        enrutador_url=args.url,
        mac_address=args.mac,
        concurrent_users=args.users,
        requests_per_user=args.requests,
        pattern=args.pattern,
        dataset_name=args.dataset,
        timeout=args.timeout,
        ramp_up_seconds=args.ramp_up
    )
    
    # Logger
    logger = setup_logger(level="INFO", format_type="text")
    
    # Verificar conexión al Enrutador
    client = APIClient(base_url=config.enrutador_url)
    if not client.health_check():
        logger.error("❌ No se puede conectar al Enrutador")
        logger.error(f"   URL: {config.enrutador_url}")
        sys.exit(1)
    
    logger.info(f"✅ Conectado al Enrutador: {config.enrutador_url}")
    
    # Ejecutar prueba
    try:
        result = run_load_test(config, logger)
        print_results(result, config)
        
        # Guardar resultados en CSV para análisis
        metrics_collector = MetricsCollector(output_file="load_test_results.csv")
        # Aquí podrías guardar los resultados agregados si lo necesitas
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Prueba interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
