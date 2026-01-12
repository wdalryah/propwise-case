"""
Propwise Quality Framework
==========================

Comprehensive quality framework with:
- Schema Registry (Glue-like catalog for staging data lake)
- Schema contract validation (soft contract)
- Dimension metrics (completeness, integrity, uniqueness, validity, timeliness)
- Rejection handling and quarantine
- PII detection and data classification
- Alert integration

Usage:
    from quality_framework import (
        SchemaRegistry,
        SchemaContract,
        DimensionMetrics,
        RejectionHandler,
        PIIDetector,
        QualityAlertManager
    )
    
    # Schema Registry (like Glue Catalog)
    registry = SchemaRegistry(minio_endpoint='localhost:9000')
    results = registry.run_crawler(bucket="staging")  # Auto-discover schemas
    schema = registry.get_schema("processed_leads")    # Get table schema
    tables = registry.list_tables()                    # List all tables
    
    # Schema validation
    contract = SchemaContract(staging_config_dir="processing/common_code/staging/configs")
    result = contract.validate_schema(df, "leads")
    
    # Dimension metrics
    metrics = DimensionMetrics()
    scores = metrics.calculate_all_dimensions(df, "leads", "raw")
    
    # Rejection handling
    handler = RejectionHandler()
    clean_df, rejected_df, summary = handler.process_dataframe(df, "leads")
    
    # PII detection
    detector = PIIDetector()
    classification = detector.scan_dataframe(df, "leads")
"""

from .schema_registry import SchemaRegistry
from .schema_contract import SchemaContract, DimensionMetrics
from .rejection_handler import RejectionHandler, PIIDetector
from .quality_alerts import QualityAlertManager

__version__ = "1.0.0"
__all__ = [
    "SchemaRegistry",
    "SchemaContract",
    "DimensionMetrics", 
    "RejectionHandler",
    "PIIDetector",
    "QualityAlertManager"
]
