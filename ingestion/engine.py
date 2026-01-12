"""
Ingestion Engine Core
=====================

Main orchestrator for data ingestion from source to data lake.
Supports Full Load and CDC modes.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from .connectors.mysql_connector import MySQLConnector
from .connectors.minio_connector import MinIOConnector

logger = logging.getLogger(__name__)


class IngestionEngine:
    """
    Data Ingestion Engine for moving data from source databases to data lake.
    
    Supports:
    - Full Load: Extract entire table
    - CDC: Extract incremental changes based on timestamp column
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the ingestion engine.
        
        Args:
            config_path: Path to the configs directory
        """
        self.config_path = config_path or self._get_default_config_path()
        self.task_settings = self._load_task_settings()
        self.table_mappings = self._load_table_mappings()
        self.source_connector = None
        self.target_connector = None
        self.batch_id = self._generate_batch_id()
        self.extraction_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        self._setup_logging()
        
    def _get_default_config_path(self) -> str:
        """Get default config path."""
        return str(Path(__file__).parent / "configs")
    
    def _load_task_settings(self) -> Dict:
        """Load task settings from JSON file."""
        settings_path = os.path.join(self.config_path, "task_settings.json")
        with open(settings_path, 'r') as f:
            return json.load(f)
    
    def _load_table_mappings(self) -> Dict:
        """Load table mappings from JSON file."""
        mappings_path = os.path.join(self.config_path, "table_mappings.json")
        with open(mappings_path, 'r') as f:
            return json.load(f)
    
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for this run."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_settings = self.task_settings.get("task_settings", {}).get("logging", {})
        log_level = getattr(logging, log_settings.get("level", "INFO"))
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        if log_settings.get("log_to_file"):
            log_path = log_settings.get("log_path", "logs/ingestion.log")
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            logging.getLogger().addHandler(file_handler)
    
    def connect(self, use_docker_hosts: bool = False):
        """
        Establish connections to source and target.
        
        Args:
            use_docker_hosts: Use Docker internal hostnames (for running inside containers)
        """
        logger.info("Connecting to source and target systems...")
        
        # Source connection
        source_config = self.task_settings["source"]["connection"].copy()
        if use_docker_hosts:
            source_config["host"] = self.task_settings["source"].get("docker_host", source_config["host"])
            source_config["port"] = self.task_settings["source"].get("docker_port", source_config["port"])
        
        self.source_connector = MySQLConnector(source_config)
        self.source_connector.connect()
        logger.info("✓ Connected to MySQL source")
        
        # Target connection
        target_config = self.task_settings["target"]["connection"].copy()
        if use_docker_hosts:
            target_config["endpoint"] = self.task_settings["target"].get("docker_endpoint", target_config["endpoint"])
        
        self.target_connector = MinIOConnector(target_config)
        self.target_connector.connect()
        logger.info("✓ Connected to MinIO target")
    
    def disconnect(self):
        """Close all connections."""
        if self.source_connector:
            self.source_connector.disconnect()
        logger.info("Connections closed")
    
    def get_tables_to_ingest(self) -> List[Dict]:
        """
        Get list of tables to ingest based on table mappings.
        
        Returns:
            List of table configurations
        """
        tables = []
        for rule in self.table_mappings.get("rules", []):
            if rule.get("rule-type") == "selection" and rule.get("rule-action") == "explicit":
                cdc_config = rule.get("cdc-config", {})
                tables.append({
                    "rule_id": rule.get("rule-id"),
                    "rule_name": rule.get("rule-name"),
                    "schema": rule["object-locator"]["schema-name"],
                    "table": rule["object-locator"]["table-name"],
                    "cdc_config": cdc_config
                })
        return tables
    
    def get_table_prefix(self) -> str:
        """Get the table prefix from configuration."""
        # Check transformation rules for add-prefix action
        for rule in self.table_mappings.get("rules", []):
            if (rule.get("rule-type") == "transformation" and 
                rule.get("rule-action") == "add-prefix" and
                rule.get("rule-target") == "table"):
                return rule.get("value", "")
        
        # Fallback to task settings
        return self.task_settings.get("task_settings", {}).get("table_prefix", "")
    
    def get_target_schema(self, source_schema: str) -> str:
        """Get the target schema name after transformations."""
        for rule in self.table_mappings.get("rules", []):
            if (rule.get("rule-type") == "transformation" and 
                rule.get("rule-action") == "rename" and
                rule.get("rule-target") == "schema"):
                locator = rule.get("object-locator", {})
                if locator.get("schema-name") == source_schema:
                    return rule.get("value", source_schema)
        return source_schema
    
    def apply_audit_columns(self, df, table_name: str, id_column: str = "id", 
                            operation: str = "I", delete_config: Dict = None) -> Any:
        """
        Apply audit columns to dataframe based on transformation rules.
        
        Args:
            df: Pandas DataFrame
            table_name: Name of the source table
            id_column: Column to use for GPK generation
            operation: Default operation type (I=Insert, U=Update, D=Delete)
            delete_config: Config for detecting soft deletes
            
        Returns:
            DataFrame with audit columns added
        """
        import pandas as pd
        
        df = df.copy()
        audit_config = self.task_settings.get("task_settings", {}).get("audit_columns", {})
        extraction_time = datetime.now().isoformat()
        
        # Add extraction_time
        if audit_config.get("extraction_time", True):
            df["extraction_time"] = extraction_time
        
        # Add global primary key (gpk) - combination of id and extraction timestamp
        if audit_config.get("gpk", True):
            if id_column in df.columns:
                df["gpk"] = df[id_column].astype(str) + "~" + self.extraction_timestamp
            else:
                # Use row index if id column not found
                df["gpk"] = df.index.astype(str) + "~" + self.extraction_timestamp
        
        # Add env
        env_value = audit_config.get("env", "test")
        df["env"] = env_value
        
        # Add region
        region_value = audit_config.get("region", "local")
        df["region"] = region_value
        
        # Add operation type (op) - I, U, D
        df["op"] = operation
        
        # Detect soft deletes if delete_config is provided
        if delete_config:
            delete_column = delete_config.get("column")
            delete_value = delete_config.get("delete_value")
            if delete_column and delete_value and delete_column in df.columns:
                # Mark rows as Delete where delete condition is met
                df.loc[df[delete_column] == delete_value, "op"] = "D"
        
        return df
    
    def full_load(self, table_config: Dict) -> Dict:
        """
        Perform full load for a single table.
        
        Args:
            table_config: Table configuration dict
            
        Returns:
            Result dictionary with status and metrics
        """
        schema = table_config["schema"]
        table = table_config["table"]
        
        # Get target names with transformations applied
        table_prefix = self.get_table_prefix()
        target_schema = self.get_target_schema(schema)
        target_table = f"{table_prefix}{table}"
        
        logger.info(f"Starting FULL LOAD for {schema}.{table} -> {target_schema}/{target_table}")
        
        result = {
            "source_table": f"{schema}.{table}",
            "target_table": f"{target_schema}/{target_table}",
            "mode": "full_load",
            "status": "pending",
            "rows_extracted": 0,
            "rows_loaded": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error": None
        }
        
        try:
            # Extract data from source
            settings = self.task_settings["task_settings"]["full_load"]
            batch_size = settings.get("batch_size", 10000)
            
            logger.info(f"  Extracting data from {schema}.{table}...")
            df = self.source_connector.extract_table(schema, table)
            result["rows_extracted"] = len(df)
            logger.info(f"  Extracted {len(df)} rows")
            
            if len(df) == 0:
                logger.warning(f"  No data found in {schema}.{table}")
                result["status"] = "success"
                result["end_time"] = datetime.now().isoformat()
                return result
            
            # Apply audit columns (full load = Insert)
            df = self.apply_audit_columns(df, table_name=table, id_column="id", operation="I")
            
            # Write to target
            file_format = settings.get("file_format", "csv")
            target_path = f"{target_schema}/{target_table}/full_load/{self.batch_id}"
            
            logger.info(f"  Writing to MinIO: {target_path} (format: {file_format})")
            self.target_connector.write_dataframe(
                df=df,
                path=target_path,
                file_format=file_format
            )
            
            result["rows_loaded"] = len(df)
            result["status"] = "success"
            result["target_path"] = target_path
            result["file_format"] = file_format
            result["audit_columns"] = ["extraction_time", "gpk", "env", "region", "op"]
            logger.info(f"  ✓ Full load complete: {len(df)} rows written to {target_path}")
            
        except Exception as e:
            logger.error(f"  ✗ Full load failed: {str(e)}")
            result["status"] = "failed"
            result["error"] = str(e)
        
        result["end_time"] = datetime.now().isoformat()
        return result
    
    def cdc_load(self, table_config: Dict, watermark: Optional[str] = None) -> Dict:
        """
        Perform CDC (incremental) load for a single table.
        
        Args:
            table_config: Table configuration dict
            watermark: Last processed timestamp (ISO format)
            
        Returns:
            Result dictionary with status and metrics
        """
        schema = table_config["schema"]
        table = table_config["table"]
        cdc_config = table_config.get("cdc_config", {})
        
        # Get target names with transformations applied
        table_prefix = self.get_table_prefix()
        target_schema = self.get_target_schema(schema)
        target_table = f"{table_prefix}{table}"
        
        logger.info(f"Starting CDC LOAD for {schema}.{table} -> {target_schema}/{target_table}")
        
        result = {
            "source_table": f"{schema}.{table}",
            "target_table": f"{target_schema}/{target_table}",
            "mode": "cdc",
            "status": "pending",
            "rows_extracted": 0,
            "rows_loaded": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "previous_watermark": watermark,
            "new_watermark": None,
            "error": None
        }
        
        try:
            settings = self.task_settings["task_settings"]["cdc"]
            # Use table-specific tracking column if available, else use default
            tracking_column = cdc_config.get("tracking_column", settings.get("tracking_column", "updated_at"))
            delete_detection = cdc_config.get("delete_detection")
            
            # Extract incremental data
            logger.info(f"  Extracting changes since: {watermark or 'beginning'}")
            logger.info(f"  Tracking column: {tracking_column}")
            
            df = self.source_connector.extract_incremental(
                schema=schema,
                table=table,
                tracking_column=tracking_column,
                watermark=watermark
            )
            result["rows_extracted"] = len(df)
            logger.info(f"  Extracted {len(df)} changed rows")
            
            if len(df) == 0:
                logger.info(f"  No changes found in {schema}.{table}")
                result["status"] = "success"
                result["new_watermark"] = watermark
                result["end_time"] = datetime.now().isoformat()
                return result
            
            # Apply audit columns with operation detection
            # Default operation is U (Update) for CDC, unless it's detected as Delete
            df = self.apply_audit_columns(
                df, 
                table_name=table, 
                id_column="id",
                operation="U",  # Default to Update for CDC
                delete_config=delete_detection
            )
            
            # Count operation types
            if "op" in df.columns:
                op_counts = df["op"].value_counts().to_dict()
                result["inserts"] = op_counts.get("I", 0)
                result["updates"] = op_counts.get("U", 0)
                result["deletes"] = op_counts.get("D", 0)
                logger.info(f"  Operations: I={result['inserts']}, U={result['updates']}, D={result['deletes']}")
            
            # Get new watermark
            if tracking_column in df.columns:
                result["new_watermark"] = str(df[tracking_column].max())
            
            # Write to target
            file_format = settings.get("file_format", "csv")
            target_path = f"{target_schema}/{target_table}/cdc/{self.batch_id}"
            
            logger.info(f"  Writing to MinIO: {target_path} (format: {file_format})")
            self.target_connector.write_dataframe(
                df=df,
                path=target_path,
                file_format=file_format
            )
            
            result["rows_loaded"] = len(df)
            result["status"] = "success"
            result["target_path"] = target_path
            result["file_format"] = file_format
            result["audit_columns"] = ["extraction_time", "gpk", "env", "region", "op"]
            logger.info(f"  ✓ CDC load complete: {len(df)} rows written to {target_path}")
            
        except Exception as e:
            logger.error(f"  ✗ CDC load failed: {str(e)}")
            result["status"] = "failed"
            result["error"] = str(e)
        
        result["end_time"] = datetime.now().isoformat()
        return result
    
    def run_full_load(self) -> List[Dict]:
        """
        Run full load for all configured tables.
        
        Returns:
            List of result dictionaries
        """
        logger.info("=" * 60)
        logger.info("STARTING FULL LOAD INGESTION")
        logger.info(f"Batch ID: {self.batch_id}")
        logger.info(f"Extraction Timestamp: {self.extraction_timestamp}")
        logger.info("=" * 60)
        
        tables = self.get_tables_to_ingest()
        logger.info(f"Tables to ingest: {len(tables)}")
        
        results = []
        for table_config in tables:
            result = self.full_load(table_config)
            results.append(result)
        
        # Summary
        success = sum(1 for r in results if r["status"] == "success")
        failed = sum(1 for r in results if r["status"] == "failed")
        total_rows = sum(r["rows_loaded"] for r in results)
        
        logger.info("=" * 60)
        logger.info("FULL LOAD COMPLETE")
        logger.info(f"  Tables: {success} success, {failed} failed")
        logger.info(f"  Total rows: {total_rows}")
        logger.info("=" * 60)
        
        return results
    
    def run_cdc(self, watermarks: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        Run CDC for all configured tables.
        
        Args:
            watermarks: Dict of table_name -> last_watermark
            
        Returns:
            List of result dictionaries
        """
        watermarks = watermarks or {}
        
        logger.info("=" * 60)
        logger.info("STARTING CDC INGESTION")
        logger.info(f"Batch ID: {self.batch_id}")
        logger.info(f"Extraction Timestamp: {self.extraction_timestamp}")
        logger.info("=" * 60)
        
        tables = self.get_tables_to_ingest()
        logger.info(f"Tables to check: {len(tables)}")
        
        results = []
        for table_config in tables:
            table_name = table_config["table"]
            watermark = watermarks.get(table_name)
            result = self.cdc_load(table_config, watermark)
            results.append(result)
        
        # Summary
        success = sum(1 for r in results if r["status"] == "success")
        failed = sum(1 for r in results if r["status"] == "failed")
        total_rows = sum(r["rows_loaded"] for r in results)
        
        logger.info("=" * 60)
        logger.info("CDC COMPLETE")
        logger.info(f"  Tables: {success} success, {failed} failed")
        logger.info(f"  Total changed rows: {total_rows}")
        logger.info("=" * 60)
        
        return results
