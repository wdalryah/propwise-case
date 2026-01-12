#!/usr/bin/env python3
"""
CDC Daemon - Real-time Change Data Capture
==========================================

A DMS-like engine that continuously monitors MySQL binlog
and automatically ingests changes to MinIO raw bucket.

Features:
- Real-time capture using MySQL binary log
- Automatic INSERT, UPDATE, DELETE detection
- Batched writes to MinIO for efficiency
- Checkpoint/watermark persistence for recovery
- Graceful shutdown handling

Usage:
    python cdc_daemon.py --mode binlog   # Real-time binlog capture
    python cdc_daemon.py --mode polling  # Polling-based capture (fallback)
"""

import json
import logging
import os
import signal
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from queue import Queue, Empty

import pandas as pd
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent, 
    DeleteRowsEvent
)

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.connectors.mysql_connector import MySQLConnector
from ingestion.connectors.minio_connector import MinIOConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CDC_DAEMON")


class CDCDaemon:
    """
    Real-time CDC Daemon that captures MySQL changes and writes to MinIO.
    Similar to AWS DMS continuous replication.
    """
    
    def __init__(self, config_path: str = None):
        self.config_path = config_path or str(Path(__file__).parent / "configs")
        self.task_settings = self._load_config("task_settings.json")
        self.table_mappings = self._load_config("table_mappings.json")
        
        # State
        self.running = False
        self.checkpoint_file = str(Path(__file__).parent / "checkpoints" / "cdc_checkpoint.json")
        self.checkpoint = self._load_checkpoint()
        
        # Batching
        self.batch_queue = Queue()
        self.batch_size = 100  # Records per batch
        self.flush_interval = 10  # Seconds between flushes
        self.last_flush = time.time()
        
        # Connectors
        self.minio_connector = None
        
        # Tables to monitor
        self.monitored_tables = self._get_monitored_tables()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("CDC Daemon initialized")
        logger.info(f"Monitoring tables: {list(self.monitored_tables.keys())}")
    
    def _load_config(self, filename: str) -> Dict:
        """Load configuration file."""
        path = os.path.join(self.config_path, filename)
        with open(path, 'r') as f:
            return json.load(f)
    
    def _load_checkpoint(self) -> Dict:
        """Load checkpoint from file."""
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        return {"log_file": None, "log_pos": None, "last_timestamp": None}
    
    def _save_checkpoint(self, log_file: str = None, log_pos: int = None):
        """Save checkpoint to file."""
        self.checkpoint["log_file"] = log_file or self.checkpoint.get("log_file")
        self.checkpoint["log_pos"] = log_pos or self.checkpoint.get("log_pos")
        self.checkpoint["last_timestamp"] = datetime.now().isoformat()
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint, f, indent=2)
    
    def _get_monitored_tables(self) -> Dict[str, Dict]:
        """Get list of tables to monitor from config."""
        tables = {}
        for rule in self.table_mappings.get("rules", []):
            if rule.get("rule-type") == "selection" and rule.get("rule-action") == "explicit":
                schema = rule["object-locator"]["schema-name"]
                table = rule["object-locator"]["table-name"]
                key = f"{schema}.{table}"
                tables[key] = {
                    "schema": schema,
                    "table": table,
                    "cdc_config": rule.get("cdc-config", {})
                }
        return tables
    
    def _get_table_prefix(self) -> str:
        """Get table prefix from config."""
        for rule in self.table_mappings.get("rules", []):
            if rule.get("rule-type") == "transformation" and rule.get("rule-action") == "add-prefix":
                return rule.get("value", "")
        return "raw_"
    
    def _get_target_schema(self, source_schema: str) -> str:
        """Get target schema name."""
        for rule in self.table_mappings.get("rules", []):
            if (rule.get("rule-type") == "transformation" and 
                rule.get("rule-action") == "rename" and
                rule.get("rule-target") == "schema"):
                if rule.get("object-locator", {}).get("schema-name") == source_schema:
                    return rule.get("value", source_schema)
        return "propwise"
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def connect(self, use_docker_hosts: bool = False):
        """Connect to MinIO."""
        target_config = self.task_settings["target"]["connection"].copy()
        if use_docker_hosts:
            target_config["endpoint"] = self.task_settings["target"].get("docker_endpoint", target_config["endpoint"])
        
        self.minio_connector = MinIOConnector(target_config)
        self.minio_connector.connect()
        logger.info("✓ Connected to MinIO")
    
    def _create_audit_record(self, row_data: Dict, table: str, operation: str) -> Dict:
        """Add audit columns to a record."""
        extraction_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        extraction_time = datetime.now().isoformat()
        
        # Get ID for GPK
        id_value = row_data.get("id", row_data.get("ID", "unknown"))
        
        audit_data = {
            "extraction_time": extraction_time,
            "gpk": f"{id_value}~{extraction_timestamp}",
            "env": self.task_settings.get("task_settings", {}).get("audit_columns", {}).get("env", "test"),
            "region": self.task_settings.get("task_settings", {}).get("audit_columns", {}).get("region", "local"),
            "op": operation
        }
        
        # Merge original data with audit columns
        return {**row_data, **audit_data}
    
    def _flush_batch(self, records: List[Dict], table_key: str):
        """Write a batch of records to MinIO."""
        if not records:
            return
        
        table_info = self.monitored_tables.get(table_key, {})
        schema = table_info.get("schema", "propwise_source")
        table = table_info.get("table", "unknown")
        
        # Build target path
        prefix = self._get_table_prefix()
        target_schema = self._get_target_schema(schema)
        target_table = f"{prefix}{table}"
        
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        target_path = f"{target_schema}/{target_table}/cdc/{batch_id}"
        
        # Convert to DataFrame and write
        df = pd.DataFrame(records)
        
        logger.info(f"  Writing {len(records)} records to {target_path}")
        self.minio_connector.write_dataframe(
            df=df,
            path=target_path,
            file_format="csv"
        )
        
        logger.info(f"  ✓ Flushed {len(records)} records for {table_key}")
    
    def _process_binlog_event(self, event):
        """Process a single binlog event."""
        # Get schema and table
        schema = event.schema
        table = event.table
        table_key = f"{schema}.{table}"
        
        # Skip if not monitored
        if table_key not in self.monitored_tables:
            return
        
        # Determine operation type
        if isinstance(event, WriteRowsEvent):
            operation = "I"
            rows = event.rows
        elif isinstance(event, UpdateRowsEvent):
            operation = "U"
            # For updates, use the 'after_values'
            rows = [{"values": row["after_values"]} for row in event.rows]
        elif isinstance(event, DeleteRowsEvent):
            operation = "D"
            rows = event.rows
        else:
            return
        
        # Process rows
        for row in rows:
            row_data = row.get("values", row)
            record = self._create_audit_record(row_data, table, operation)
            self.batch_queue.put((table_key, record))
        
        logger.info(f"  Captured {len(rows)} {operation} events for {table_key}")
    
    def _batch_flusher(self):
        """Background thread to flush batches periodically."""
        batches = {}  # table_key -> list of records
        
        while self.running:
            try:
                # Try to get a record from queue
                table_key, record = self.batch_queue.get(timeout=1)
                
                if table_key not in batches:
                    batches[table_key] = []
                batches[table_key].append(record)
                
                # Check if we should flush (batch size reached)
                if len(batches[table_key]) >= self.batch_size:
                    self._flush_batch(batches[table_key], table_key)
                    batches[table_key] = []
                    self.last_flush = time.time()
                
            except Empty:
                pass
            
            # Check if we should flush (time interval)
            if time.time() - self.last_flush >= self.flush_interval:
                for table_key, records in batches.items():
                    if records:
                        self._flush_batch(records, table_key)
                batches = {}
                self.last_flush = time.time()
        
        # Final flush on shutdown
        for table_key, records in batches.items():
            if records:
                self._flush_batch(records, table_key)
    
    def run_binlog_mode(self, use_docker_hosts: bool = False):
        """
        Run CDC in binlog mode - real-time capture.
        This is similar to how AWS DMS works.
        """
        logger.info("=" * 60)
        logger.info("CDC DAEMON - BINLOG MODE (Real-time)")
        logger.info("=" * 60)
        
        self.connect(use_docker_hosts)
        self.running = True
        
        # MySQL connection settings for binlog
        source_config = self.task_settings["source"]["connection"]
        if use_docker_hosts:
            mysql_host = self.task_settings["source"].get("docker_host", source_config["host"])
            mysql_port = self.task_settings["source"].get("docker_port", source_config["port"])
        else:
            mysql_host = source_config["host"]
            mysql_port = source_config["port"]
        
        mysql_settings = {
            "host": mysql_host,
            "port": mysql_port,
            "user": source_config["user"],
            "passwd": source_config["password"]
        }
        
        # Start batch flusher thread
        flusher_thread = threading.Thread(target=self._batch_flusher, daemon=True)
        flusher_thread.start()
        
        # Get list of tables to monitor
        only_tables = [info["table"] for info in self.monitored_tables.values()]
        only_schemas = list(set(info["schema"] for info in self.monitored_tables.values()))
        
        logger.info(f"Monitoring schemas: {only_schemas}")
        logger.info(f"Monitoring tables: {only_tables}")
        
        # Resume from checkpoint if available
        resume_kwargs = {}
        if self.checkpoint.get("log_file") and self.checkpoint.get("log_pos"):
            resume_kwargs["log_file"] = self.checkpoint["log_file"]
            resume_kwargs["log_pos"] = self.checkpoint["log_pos"]
            logger.info(f"Resuming from checkpoint: {self.checkpoint['log_file']}:{self.checkpoint['log_pos']}")
        
        try:
            stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=100,  # Unique server ID for this CDC instance
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_tables=only_tables,
                only_schemas=only_schemas,
                blocking=True,
                resume_stream=bool(resume_kwargs),
                **resume_kwargs
            )
            
            logger.info("✓ Binlog stream connected - waiting for changes...")
            logger.info("  Press Ctrl+C to stop")
            
            event_count = 0
            for event in stream:
                if not self.running:
                    break
                
                self._process_binlog_event(event)
                event_count += 1
                
                # Save checkpoint periodically
                if event_count % 100 == 0:
                    self._save_checkpoint(stream.log_file, stream.log_pos)
            
            # Final checkpoint
            self._save_checkpoint(stream.log_file, stream.log_pos)
            stream.close()
            
        except Exception as e:
            logger.error(f"Binlog stream error: {e}")
            logger.info("Falling back to polling mode...")
            self.run_polling_mode(use_docker_hosts)
        
        logger.info("CDC Daemon stopped")
    
    def run_polling_mode(self, use_docker_hosts: bool = False, poll_interval: int = 5):
        """
        Run CDC in polling mode - fallback when binlog is not available.
        Polls the database every N seconds for changes.
        """
        logger.info("=" * 60)
        logger.info("CDC DAEMON - POLLING MODE")
        logger.info(f"Poll interval: {poll_interval} seconds")
        logger.info("=" * 60)
        
        self.connect(use_docker_hosts)
        self.running = True
        
        # Connect to MySQL
        source_config = self.task_settings["source"]["connection"].copy()
        if use_docker_hosts:
            source_config["host"] = self.task_settings["source"].get("docker_host", source_config["host"])
            source_config["port"] = self.task_settings["source"].get("docker_port", source_config["port"])
        
        mysql_connector = MySQLConnector(source_config)
        mysql_connector.connect()
        
        # Load watermarks
        watermarks = self.checkpoint.get("watermarks", {})
        
        logger.info("✓ Polling mode started - checking for changes...")
        logger.info("  Press Ctrl+C to stop")
        
        while self.running:
            try:
                for table_key, table_info in self.monitored_tables.items():
                    schema = table_info["schema"]
                    table = table_info["table"]
                    cdc_config = table_info.get("cdc_config", {})
                    tracking_column = cdc_config.get("tracking_column", "updated_at")
                    
                    # Get watermark for this table
                    watermark = watermarks.get(table_key)
                    
                    # Extract incremental changes
                    df = mysql_connector.extract_incremental(
                        schema=schema,
                        table=table,
                        tracking_column=tracking_column,
                        watermark=watermark
                    )
                    
                    if len(df) > 0:
                        logger.info(f"  Found {len(df)} changes in {table_key}")
                        
                        # Add audit columns
                        extraction_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                        extraction_time = datetime.now().isoformat()
                        
                        df["extraction_time"] = extraction_time
                        df["gpk"] = df["id"].astype(str) + "~" + extraction_timestamp
                        df["env"] = self.task_settings.get("task_settings", {}).get("audit_columns", {}).get("env", "test")
                        df["region"] = self.task_settings.get("task_settings", {}).get("audit_columns", {}).get("region", "local")
                        df["op"] = "U"  # Default to Update for polling mode
                        
                        # Detect deletes if configured
                        delete_config = cdc_config.get("delete_detection")
                        if delete_config:
                            delete_col = delete_config.get("column")
                            delete_val = delete_config.get("delete_value")
                            if delete_col in df.columns:
                                df.loc[df[delete_col] == delete_val, "op"] = "D"
                        
                        # Write to MinIO
                        prefix = self._get_table_prefix()
                        target_schema = self._get_target_schema(schema)
                        target_table = f"{prefix}{table}"
                        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
                        target_path = f"{target_schema}/{target_table}/cdc/{batch_id}"
                        
                        self.minio_connector.write_dataframe(
                            df=df,
                            path=target_path,
                            file_format="csv"
                        )
                        
                        logger.info(f"  ✓ Written {len(df)} records to {target_path}")
                        
                        # Update watermark
                        if tracking_column in df.columns:
                            watermarks[table_key] = str(df[tracking_column].max())
                
                # Save checkpoint
                self.checkpoint["watermarks"] = watermarks
                self._save_checkpoint()
                
                # Wait for next poll
                time.sleep(poll_interval)
                
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(poll_interval)
        
        mysql_connector.disconnect()
        logger.info("CDC Daemon stopped")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="CDC Daemon - Real-time Change Data Capture")
    parser.add_argument("--mode", choices=["binlog", "polling"], default="polling",
                        help="CDC mode: binlog (real-time) or polling (interval-based)")
    parser.add_argument("--poll-interval", type=int, default=5,
                        help="Polling interval in seconds (for polling mode)")
    parser.add_argument("--docker", action="store_true",
                        help="Use Docker internal hostnames")
    parser.add_argument("--config", help="Path to config directory")
    
    args = parser.parse_args()
    
    daemon = CDCDaemon(config_path=args.config)
    
    if args.mode == "binlog":
        daemon.run_binlog_mode(use_docker_hosts=args.docker)
    else:
        daemon.run_polling_mode(use_docker_hosts=args.docker, poll_interval=args.poll_interval)


if __name__ == "__main__":
    main()

