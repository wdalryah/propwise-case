"""
Propwise Observability Module
=============================

Provides monitoring, alerting, and logging for the data platform.

Components:
- metrics: Prometheus-compatible metrics collection
- alerts: Alert management and notification
- logging: Structured logging with centralized storage

Usage:
    from observability import MetricsCollector, AlertManager, StructuredLogger
    
    # Metrics
    metrics = MetricsCollector()
    metrics.record_gauge("pipeline_rows_processed", 1000, {"table": "leads"})
    
    # Alerts
    alerts = AlertManager()
    alerts.send_alert("critical", "Pipeline Failed", "ETL job failed")
    
    # Logging
    logger = StructuredLogger("etl_pipeline")
    logger.info("Pipeline started", extra={"run_id": "abc123"})
"""

from .metrics.collector import MetricsCollector
from .alerts.manager import AlertManager
from .logging.structured_logger import StructuredLogger

__version__ = "1.0.0"
__all__ = ["MetricsCollector", "AlertManager", "StructuredLogger"]

