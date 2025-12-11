"""
Cliente Arrow Flight
Maneja la comunicación gRPC con el Gateway
"""
import json
import time
import logging
from typing import Dict, Any, Generator

import pyarrow.flight as flight
import pyarrow as pa
from ui import QueryMetrics

logger = logging.getLogger("FlightClient")

class ArrowFlightClient:
    def __init__(self, uri: str = "grpc://localhost:8815"):
        self.uri = uri
        self.client = flight.FlightClient(uri)
        
    def check_health(self) -> bool:
        """Verifica conectividad básica"""
        try:
            # Listar flights es una operación liviana
            list(self.client.list_flights())
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
            
    def get_dataset_info(self, tenant_id: str, dataset_name: str, rows: int = None) -> flight.FlightInfo:
        """Obtiene metadata del dataset"""
        # Descriptor path: [tenant_id, dataset_name, rows?]
        path_args = [tenant_id.encode(), dataset_name.encode()]
        if rows:
            path_args.append(str(rows).encode())
            
        descriptor = flight.FlightDescriptor.for_path(*path_args)
        
        try:
            info = self.client.get_flight_info(descriptor)
            return info
        except flight.FlightError as e:
            logger.error(f"Error getting flight info: {e}")
            raise
            
    def get_dataset_stream(self, ticket: flight.Ticket) -> flight.FlightStreamReader:
        """Obtiene el stream de datos"""
        try:
            reader = self.client.do_get(ticket)
            return reader
        except flight.FlightError as e:
            logger.error(f"Error reading stream: {e}")
            raise

    def query_dataset(self, tenant_id: str, dataset: str, rows: int = None) -> QueryMetrics:
        """
        Ejecuta flujo completo: GetInfo + DoGet
        Retorna métricas.
        """
        metrics = QueryMetrics(tenant_id=tenant_id, dataset=dataset)
        
        try:
            # 1. Get Flight Info
            t0 = time.perf_counter()
            info = self.get_dataset_info(tenant_id, dataset, rows=rows)
            t1 = time.perf_counter()
            metrics.metadata_latency_ms = (t1 - t0) * 1000
            
            # Unpack info
            endpoint = info.endpoints[0]
            ticket = endpoint.ticket
            
            # 2. Do Get (Stream)
            t2 = time.perf_counter()
            reader = self.get_dataset_stream(ticket)
            
            # Leer toda la tabla
            table = reader.read_all()
            t3 = time.perf_counter()
            
            metrics.transfer_latency_ms = (t3 - t2) * 1000
            metrics.rows = table.num_rows
            metrics.bytes = table.nbytes
            metrics.status = "Success"
            
        except Exception as e:
            metrics.status = "Error"
            metrics.error = str(e)
            logger.error(f"Query failed: {e}")
            
        metrics.total_latency_ms = metrics.metadata_latency_ms + metrics.transfer_latency_ms
        return metrics
