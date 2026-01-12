"""
Propwise Ingestion Engine
========================

A decoupled data ingestion engine that supports:
- Full Load: Complete table extraction
- CDC (Change Data Capture): Incremental extraction based on timestamp

This engine is responsible ONLY for moving data from source to data lake.
No transformations or cleaning - data is ingested exactly as-is.
"""

__version__ = "1.0.0"

