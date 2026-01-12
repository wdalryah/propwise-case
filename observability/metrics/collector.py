"""
Metrics Collector
=================

Prometheus-compatible metrics collection for the Propwise data platform.

Supports:
- Prometheus pushgateway integration
- PostgreSQL persistence
- In-memory metrics for testing
"""

import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from functools import wraps
import threading

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        CollectorRegistry, push_to_gateway, REGISTRY,
        generate_latest
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects and exports metrics for the data platform.
    
    Supports multiple backends:
    - prometheus: Push to Prometheus Pushgateway
    - postgres: Store in PostgreSQL
    - memory: In-memory storage (for testing)
    """
    
    # Predefined metric definitions
    METRIC_DEFINITIONS = {
        # Pipeline metrics
        "pipeline_run_duration_seconds": {
            "type": "histogram",
            "description": "Duration of pipeline runs",
            "labels": ["pipeline_name", "status"]
        },
        "pipeline_rows_processed": {
            "type": "counter",
            "description": "Total rows processed by pipeline",
            "labels": ["pipeline_name", "table_name", "stage"]
        },
        "pipeline_rows_rejected": {
            "type": "counter",
            "description": "Total rows rejected by pipeline",
            "labels": ["pipeline_name", "table_name", "reason"]
        },
        "pipeline_errors_total": {
            "type": "counter",
            "description": "Total pipeline errors",
            "labels": ["pipeline_name", "error_type"]
        },
        
        # Data quality metrics
        "dq_check_passed": {
            "type": "counter",
            "description": "Data quality checks passed",
            "labels": ["table_name", "check_name"]
        },
        "dq_check_failed": {
            "type": "counter",
            "description": "Data quality checks failed",
            "labels": ["table_name", "check_name", "severity"]
        },
        "dq_null_rate": {
            "type": "gauge",
            "description": "Null rate for column",
            "labels": ["table_name", "column_name"]
        },
        "dq_duplicate_rate": {
            "type": "gauge",
            "description": "Duplicate rate for column",
            "labels": ["table_name", "column_name"]
        },
        "dq_completeness_score": {
            "type": "gauge",
            "description": "Completeness score (0-100)",
            "labels": ["table_name"]
        },
        
        # Data freshness metrics
        "data_freshness_hours": {
            "type": "gauge",
            "description": "Hours since last data update",
            "labels": ["table_name", "stage"]
        },
        "data_row_count": {
            "type": "gauge",
            "description": "Current row count",
            "labels": ["table_name", "stage"]
        },
        
        # Ingestion metrics
        "ingestion_records_total": {
            "type": "counter",
            "description": "Total records ingested",
            "labels": ["source", "table_name", "mode"]
        },
        "ingestion_duration_seconds": {
            "type": "histogram",
            "description": "Duration of ingestion jobs",
            "labels": ["source", "table_name"]
        },
        "cdc_lag_seconds": {
            "type": "gauge",
            "description": "CDC replication lag in seconds",
            "labels": ["table_name"]
        },
        
        # DWH metrics
        "dwh_fact_row_count": {
            "type": "gauge",
            "description": "Fact table row count",
            "labels": ["fact_table"]
        },
        "dwh_dimension_row_count": {
            "type": "gauge",
            "description": "Dimension table row count",
            "labels": ["dimension_table"]
        },
        "dwh_load_duration_seconds": {
            "type": "histogram",
            "description": "DWH load duration",
            "labels": ["table_name", "load_type"]
        }
    }
    
    def __init__(
        self,
        backend: str = "postgres",
        postgres_config: Optional[Dict] = None,
        pushgateway_url: Optional[str] = None,
        job_name: str = "propwise_etl"
    ):
        """
        Initialize metrics collector.
        
        Args:
            backend: 'prometheus', 'postgres', or 'memory'
            postgres_config: PostgreSQL connection config
            pushgateway_url: Prometheus Pushgateway URL
            job_name: Job name for Prometheus
        """
        self.backend = backend
        self.job_name = job_name
        self.pushgateway_url = pushgateway_url
        
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        
        self._db_conn = None
        self._memory_store: List[Dict] = []
        self._prometheus_metrics: Dict = {}
        self._lock = threading.Lock()
        
        # Initialize Prometheus metrics if available
        if backend == "prometheus" and PROMETHEUS_AVAILABLE:
            self._init_prometheus_metrics()
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metric objects."""
        self._registry = CollectorRegistry()
        
        for name, definition in self.METRIC_DEFINITIONS.items():
            metric_type = definition["type"]
            description = definition["description"]
            labels = definition.get("labels", [])
            
            if metric_type == "counter":
                self._prometheus_metrics[name] = Counter(
                    name, description, labels, registry=self._registry
                )
            elif metric_type == "gauge":
                self._prometheus_metrics[name] = Gauge(
                    name, description, labels, registry=self._registry
                )
            elif metric_type == "histogram":
                self._prometheus_metrics[name] = Histogram(
                    name, description, labels, registry=self._registry
                )
            elif metric_type == "summary":
                self._prometheus_metrics[name] = Summary(
                    name, description, labels, registry=self._registry
                )
    
    @property
    def db_conn(self):
        """Get database connection."""
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    def close(self):
        """Close connections."""
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
    
    # =========================================
    # METRIC RECORDING METHODS
    # =========================================
    
    def record_counter(
        self,
        metric_name: str,
        value: float = 1,
        labels: Optional[Dict] = None
    ):
        """
        Increment a counter metric.
        
        Args:
            metric_name: Name of the metric
            value: Value to increment by
            labels: Label key-value pairs
        """
        labels = labels or {}
        
        with self._lock:
            if self.backend == "prometheus" and PROMETHEUS_AVAILABLE:
                if metric_name in self._prometheus_metrics:
                    self._prometheus_metrics[metric_name].labels(**labels).inc(value)
            
            elif self.backend == "postgres":
                self._persist_to_postgres(metric_name, "counter", value, labels)
            
            else:  # memory
                self._memory_store.append({
                    "metric_name": metric_name,
                    "metric_type": "counter",
                    "value": value,
                    "labels": labels,
                    "timestamp": datetime.now().isoformat()
                })
    
    def record_gauge(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict] = None
    ):
        """
        Set a gauge metric value.
        
        Args:
            metric_name: Name of the metric
            value: Current value
            labels: Label key-value pairs
        """
        labels = labels or {}
        
        with self._lock:
            if self.backend == "prometheus" and PROMETHEUS_AVAILABLE:
                if metric_name in self._prometheus_metrics:
                    self._prometheus_metrics[metric_name].labels(**labels).set(value)
            
            elif self.backend == "postgres":
                self._persist_to_postgres(metric_name, "gauge", value, labels)
            
            else:  # memory
                self._memory_store.append({
                    "metric_name": metric_name,
                    "metric_type": "gauge",
                    "value": value,
                    "labels": labels,
                    "timestamp": datetime.now().isoformat()
                })
    
    def record_histogram(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict] = None
    ):
        """
        Record a histogram observation.
        
        Args:
            metric_name: Name of the metric
            value: Observed value
            labels: Label key-value pairs
        """
        labels = labels or {}
        
        with self._lock:
            if self.backend == "prometheus" and PROMETHEUS_AVAILABLE:
                if metric_name in self._prometheus_metrics:
                    self._prometheus_metrics[metric_name].labels(**labels).observe(value)
            
            elif self.backend == "postgres":
                self._persist_to_postgres(metric_name, "histogram", value, labels)
            
            else:  # memory
                self._memory_store.append({
                    "metric_name": metric_name,
                    "metric_type": "histogram",
                    "value": value,
                    "labels": labels,
                    "timestamp": datetime.now().isoformat()
                })
    
    def _persist_to_postgres(
        self,
        metric_name: str,
        metric_type: str,
        value: float,
        labels: Dict
    ):
        """Persist metric to PostgreSQL."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES (%s, %s, %s, %s)
                """, (metric_name, metric_type, value, Json(labels)))
                self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to persist metric: {e}")
            self.db_conn.rollback()
    
    # =========================================
    # CONVENIENCE METHODS
    # =========================================
    
    def record_pipeline_run(
        self,
        pipeline_name: str,
        duration_seconds: float,
        status: str,
        rows_processed: int = 0,
        rows_rejected: int = 0,
        table_name: Optional[str] = None
    ):
        """Record metrics for a pipeline run."""
        self.record_histogram(
            "pipeline_run_duration_seconds",
            duration_seconds,
            {"pipeline_name": pipeline_name, "status": status}
        )
        
        if rows_processed > 0:
            self.record_counter(
                "pipeline_rows_processed",
                rows_processed,
                {"pipeline_name": pipeline_name, "table_name": table_name or "all", "stage": "complete"}
            )
        
        if rows_rejected > 0:
            self.record_counter(
                "pipeline_rows_rejected",
                rows_rejected,
                {"pipeline_name": pipeline_name, "table_name": table_name or "all", "reason": "quality_check"}
            )
    
    def record_dq_result(
        self,
        table_name: str,
        check_name: str,
        passed: bool,
        severity: str = "warning"
    ):
        """Record a data quality check result."""
        if passed:
            self.record_counter(
                "dq_check_passed",
                1,
                {"table_name": table_name, "check_name": check_name}
            )
        else:
            self.record_counter(
                "dq_check_failed",
                1,
                {"table_name": table_name, "check_name": check_name, "severity": severity}
            )
    
    def record_data_freshness(
        self,
        table_name: str,
        stage: str,
        hours_since_update: float
    ):
        """Record data freshness metric."""
        self.record_gauge(
            "data_freshness_hours",
            hours_since_update,
            {"table_name": table_name, "stage": stage}
        )
    
    def record_row_count(
        self,
        table_name: str,
        stage: str,
        row_count: int
    ):
        """Record table row count."""
        self.record_gauge(
            "data_row_count",
            row_count,
            {"table_name": table_name, "stage": stage}
        )
    
    # =========================================
    # TIMING DECORATOR
    # =========================================
    
    def timed(self, metric_name: str, labels: Optional[Dict] = None):
        """
        Decorator to time function execution.
        
        Usage:
            @metrics.timed("pipeline_run_duration_seconds", {"pipeline": "etl"})
            def run_pipeline():
                ...
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start
                    self.record_histogram(metric_name, duration, labels or {})
            return wrapper
        return decorator
    
    # =========================================
    # EXPORT METHODS
    # =========================================
    
    def push_to_prometheus(self):
        """Push metrics to Prometheus Pushgateway."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not available")
            return False
        
        if not self.pushgateway_url:
            logger.warning("Pushgateway URL not configured")
            return False
        
        try:
            push_to_gateway(
                self.pushgateway_url,
                job=self.job_name,
                registry=self._registry
            )
            logger.info("Metrics pushed to Prometheus Pushgateway")
            return True
        except Exception as e:
            logger.error(f"Failed to push metrics: {e}")
            return False
    
    def get_prometheus_metrics(self) -> str:
        """Get metrics in Prometheus exposition format."""
        if PROMETHEUS_AVAILABLE:
            return generate_latest(self._registry).decode('utf-8')
        return ""
    
    def get_memory_metrics(self) -> List[Dict]:
        """Get in-memory metrics store."""
        return self._memory_store.copy()
    
    def clear_memory_metrics(self):
        """Clear in-memory metrics store."""
        with self._lock:
            self._memory_store.clear()

