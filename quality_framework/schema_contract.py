"""
Schema Contract Validator (Soft Contract)
==========================================

Validates schema consistency between source and expected schema.
This is a SOFT contract - pipeline continues but alerts are raised.

Features:
- Schema drift detection (new/removed/changed columns)
- Integrity score calculation
- Alert generation for schema changes
- Metrics for dimension scores (completeness, integrity, uniqueness)
- Schema change logging
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime
from pathlib import Path
import hashlib

import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)


class SchemaContract:
    """
    Soft Schema Contract Validator.
    
    Compares source schema against expected schema defined in staging configs.
    Alerts on drift but allows pipeline to continue.
    """
    
    def __init__(
        self,
        staging_config_dir: str,
        postgres_config: Optional[Dict] = None
    ):
        """
        Initialize Schema Contract.
        
        Args:
            staging_config_dir: Path to staging configs (JSON files with expected schema)
            postgres_config: PostgreSQL connection for metadata store
        """
        self.staging_config_dir = Path(staging_config_dir)
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        self._db_conn = None
        self._expected_schemas: Dict[str, Dict] = {}
        self._load_expected_schemas()
    
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
    
    def _load_expected_schemas(self):
        """Load expected schemas from staging config files."""
        if not self.staging_config_dir.exists():
            logger.warning(f"Staging config dir not found: {self.staging_config_dir}")
            return
        
        for config_file in self.staging_config_dir.glob("*.json"):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                table_name = config.get("landing_zone_table", "").replace("raw_", "")
                if table_name:
                    # Extract expected columns from mapping
                    mapping = config.get("mapping", {})
                    expected_cols = set(mapping.keys())
                    
                    self._expected_schemas[table_name] = {
                        "columns": expected_cols,
                        "mapping": mapping,
                        "config": config,
                        "config_file": str(config_file)
                    }
                    logger.info(f"Loaded schema contract for {table_name}: {len(expected_cols)} columns")
                    
            except Exception as e:
                logger.error(f"Failed to load config {config_file}: {e}")
    
    # =========================================
    # SCHEMA VALIDATION
    # =========================================
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Dict:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: DataFrame to validate
            table_name: Table name to check against
            
        Returns:
            Dict with validation results and drift details
        """
        result = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "source_columns": list(df.columns),
            "source_column_count": len(df.columns),
            "expected_column_count": 0,
            "has_drift": False,
            "drift_details": {
                "new_columns": [],
                "missing_columns": [],
                "type_changes": []
            },
            "integrity_score": 100.0,
            "alerts": []
        }
        
        # Get expected schema
        if table_name not in self._expected_schemas:
            result["alerts"].append({
                "severity": "warning",
                "message": f"No schema contract found for table: {table_name}"
            })
            return result
        
        expected = self._expected_schemas[table_name]
        expected_cols = expected["columns"]
        source_cols = set(df.columns)
        
        result["expected_columns"] = list(expected_cols)
        result["expected_column_count"] = len(expected_cols)
        
        # Detect drift
        new_cols = source_cols - expected_cols
        missing_cols = expected_cols - source_cols
        common_cols = source_cols & expected_cols
        
        if new_cols:
            result["drift_details"]["new_columns"] = list(new_cols)
            result["has_drift"] = True
            result["alerts"].append({
                "severity": "warning",
                "message": f"New columns detected: {list(new_cols)}"
            })
        
        if missing_cols:
            result["drift_details"]["missing_columns"] = list(missing_cols)
            result["has_drift"] = True
            result["alerts"].append({
                "severity": "error",
                "message": f"Missing columns: {list(missing_cols)}"
            })
        
        # Calculate integrity score
        # 100% = perfect match, deduct for drift
        total_expected = len(expected_cols)
        matched = len(common_cols)
        new_penalty = len(new_cols) * 5  # 5% penalty per unexpected column
        missing_penalty = len(missing_cols) * 10  # 10% penalty per missing column
        
        if total_expected > 0:
            base_score = (matched / total_expected) * 100
            result["integrity_score"] = max(0, base_score - new_penalty - missing_penalty)
        
        # Log and persist
        if result["has_drift"]:
            self._log_schema_drift(table_name, result)
            self._record_metrics(table_name, result)
        
        return result
    
    def _log_schema_drift(self, table_name: str, result: Dict):
        """Log schema drift to database."""
        try:
            with self.db_conn.cursor() as cur:
                drift = result["drift_details"]
                
                # Log each change
                for col in drift.get("new_columns", []):
                    cur.execute("""
                        INSERT INTO pipeline.schema_changes 
                        (table_name, change_type, new_value, detected_at)
                        VALUES (%s, 'column_added', %s, NOW())
                    """, (table_name, Json({"column": col})))
                
                for col in drift.get("missing_columns", []):
                    cur.execute("""
                        INSERT INTO pipeline.schema_changes 
                        (table_name, change_type, old_value, detected_at)
                        VALUES (%s, 'column_removed', %s, NOW())
                    """, (table_name, Json({"column": col})))
                
                self.db_conn.commit()
                logger.info(f"Schema drift logged for {table_name}")
                
        except Exception as e:
            logger.error(f"Failed to log schema drift: {e}")
            self.db_conn.rollback()
    
    def _record_metrics(self, table_name: str, result: Dict):
        """Record schema metrics to database."""
        try:
            with self.db_conn.cursor() as cur:
                # Integrity score
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('schema_integrity_score', 'gauge', %s, %s)
                """, (result["integrity_score"], Json({"table_name": table_name})))
                
                # Column counts
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('schema_new_columns_count', 'gauge', %s, %s)
                """, (len(result["drift_details"]["new_columns"]), Json({"table_name": table_name})))
                
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('schema_missing_columns_count', 'gauge', %s, %s)
                """, (len(result["drift_details"]["missing_columns"]), Json({"table_name": table_name})))
                
                self.db_conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record metrics: {e}")
            self.db_conn.rollback()


class DimensionMetrics:
    """
    Calculates and tracks quality dimension metrics.
    
    Dimensions:
    - Completeness: % of non-null values
    - Uniqueness: % of unique values
    - Integrity: Schema consistency score
    - Validity: % of values matching expected format
    - Consistency: Cross-stage value matching
    """
    
    def __init__(self, postgres_config: Optional[Dict] = None):
        """Initialize dimension metrics calculator."""
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        self._db_conn = None
    
    @property
    def db_conn(self):
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    def close(self):
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
    
    def calculate_all_dimensions(
        self,
        df: pd.DataFrame,
        table_name: str,
        stage: str = "raw"
    ) -> Dict:
        """
        Calculate all quality dimension metrics for a DataFrame.
        
        Returns dict with dimension scores (0-100).
        """
        metrics = {
            "table_name": table_name,
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "dimensions": {}
        }
        
        # Completeness
        metrics["dimensions"]["completeness"] = self._calculate_completeness(df)
        
        # Uniqueness
        metrics["dimensions"]["uniqueness"] = self._calculate_uniqueness(df)
        
        # Validity (format compliance)
        metrics["dimensions"]["validity"] = self._calculate_validity(df)
        
        # Timeliness (data freshness) - requires timestamp column
        metrics["dimensions"]["timeliness"] = self._calculate_timeliness(df)
        
        # Calculate overall score (weighted average)
        weights = {
            "completeness": 0.30,
            "uniqueness": 0.25,
            "validity": 0.25,
            "timeliness": 0.20
        }
        
        overall = sum(
            metrics["dimensions"].get(dim, {}).get("score", 0) * weight
            for dim, weight in weights.items()
        )
        metrics["overall_quality_score"] = round(overall, 2)
        
        # Record metrics
        self._persist_metrics(metrics)
        
        return metrics
    
    def _calculate_completeness(self, df: pd.DataFrame) -> Dict:
        """Calculate completeness (non-null rate) for each column."""
        total_cells = len(df) * len(df.columns)
        null_cells = df.isna().sum().sum()
        
        completeness = {
            "score": round((1 - null_cells / total_cells) * 100, 2) if total_cells > 0 else 100,
            "total_cells": total_cells,
            "null_cells": int(null_cells),
            "columns": {}
        }
        
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            completeness["columns"][col] = {
                "null_count": null_count,
                "null_rate": round(null_count / len(df) * 100, 2) if len(df) > 0 else 0,
                "complete_rate": round((1 - null_count / len(df)) * 100, 2) if len(df) > 0 else 100
            }
        
        return completeness
    
    def _calculate_uniqueness(self, df: pd.DataFrame) -> Dict:
        """Calculate uniqueness (distinct rate) for each column."""
        uniqueness = {
            "score": 0,
            "columns": {}
        }
        
        scores = []
        for col in df.columns:
            non_null = df[col].dropna()
            total = len(non_null)
            distinct = non_null.nunique()
            duplicates = int(non_null.duplicated().sum())
            
            unique_rate = round(distinct / total * 100, 2) if total > 0 else 100
            scores.append(unique_rate)
            
            uniqueness["columns"][col] = {
                "total_values": total,
                "distinct_values": int(distinct),
                "duplicate_values": duplicates,
                "unique_rate": unique_rate
            }
        
        uniqueness["score"] = round(sum(scores) / len(scores), 2) if scores else 100
        return uniqueness
    
    def _calculate_validity(self, df: pd.DataFrame) -> Dict:
        """Calculate validity (format compliance) for each column."""
        validity = {
            "score": 100,
            "columns": {},
            "issues": []
        }
        
        scores = []
        for col in df.columns:
            col_validity = {"valid_rate": 100, "issues": []}
            non_null = df[col].dropna()
            
            if len(non_null) == 0:
                validity["columns"][col] = col_validity
                scores.append(100)
                continue
            
            # Check for JSON blobs in non-JSON columns
            json_count = 0
            for val in non_null.head(1000).astype(str):
                val_str = str(val).strip()
                if (val_str.startswith('{') and val_str.endswith('}')) or \
                   (val_str.startswith('[') and val_str.endswith(']')):
                    try:
                        json.loads(val_str)
                        json_count += 1
                    except:
                        pass
            
            if json_count > 0:
                col_validity["issues"].append(f"Contains {json_count} JSON values")
                col_validity["json_count"] = json_count
                validity["issues"].append({
                    "column": col,
                    "issue": "json_blob_detected",
                    "count": json_count
                })
            
            # Check for empty strings
            empty_count = int((non_null.astype(str) == "").sum())
            if empty_count > 0:
                col_validity["empty_string_count"] = empty_count
            
            # Calculate valid rate (100% - issues)
            issue_penalty = (json_count / len(non_null)) * 50 if len(non_null) > 0 else 0
            col_validity["valid_rate"] = round(100 - issue_penalty, 2)
            scores.append(col_validity["valid_rate"])
            
            validity["columns"][col] = col_validity
        
        validity["score"] = round(sum(scores) / len(scores), 2) if scores else 100
        return validity
    
    def _calculate_timeliness(self, df: pd.DataFrame) -> Dict:
        """Calculate timeliness (data freshness)."""
        timeliness = {
            "score": 100,
            "latest_timestamp": None,
            "hours_since_update": None
        }
        
        # Look for timestamp columns
        timestamp_cols = ["updated_at", "created_at", "extraction_time", "date_of_last_request"]
        
        for col in timestamp_cols:
            if col in df.columns:
                try:
                    timestamps = pd.to_datetime(df[col], errors='coerce').dropna()
                    if len(timestamps) > 0:
                        latest = timestamps.max()
                        hours_ago = (datetime.now() - latest.to_pydatetime()).total_seconds() / 3600
                        
                        timeliness["latest_timestamp"] = str(latest)
                        timeliness["hours_since_update"] = round(hours_ago, 2)
                        timeliness["timestamp_column"] = col
                        
                        # Score: 100 if < 24h, decreasing after
                        if hours_ago <= 24:
                            timeliness["score"] = 100
                        elif hours_ago <= 48:
                            timeliness["score"] = 80
                        elif hours_ago <= 72:
                            timeliness["score"] = 60
                        else:
                            timeliness["score"] = max(0, 100 - (hours_ago - 24) * 2)
                        
                        break
                except:
                    pass
        
        return timeliness
    
    def _persist_metrics(self, metrics: Dict):
        """Persist dimension metrics to database."""
        try:
            with self.db_conn.cursor() as cur:
                table_name = metrics["table_name"]
                stage = metrics["stage"]
                
                # Overall quality score
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('overall_quality_score', 'gauge', %s, %s)
                """, (
                    metrics["overall_quality_score"],
                    Json({"table_name": table_name, "stage": stage})
                ))
                
                # Individual dimension scores
                for dim_name, dim_data in metrics["dimensions"].items():
                    cur.execute("""
                        INSERT INTO quality.metrics 
                        (metric_name, metric_type, metric_value, labels)
                        VALUES (%s, 'gauge', %s, %s)
                    """, (
                        f"dimension_{dim_name}_score",
                        dim_data.get("score", 0),
                        Json({"table_name": table_name, "stage": stage})
                    ))
                
                # Row and column counts
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('data_row_count', 'gauge', %s, %s)
                """, (metrics["row_count"], Json({"table_name": table_name, "stage": stage})))
                
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('data_column_count', 'gauge', %s, %s)
                """, (metrics["column_count"], Json({"table_name": table_name, "stage": stage})))
                
                self.db_conn.commit()
                logger.info(f"Dimension metrics persisted for {table_name}/{stage}")
                
        except Exception as e:
            logger.error(f"Failed to persist metrics: {e}")
            self.db_conn.rollback()
    
    def compare_stages(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        table_name: str,
        key_column: str = "id"
    ) -> Dict:
        """
        Compare metrics between two stages (e.g., raw vs staging).
        
        Returns consistency metrics showing data flow integrity.
        """
        comparison = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "source_rows": len(source_df),
            "target_rows": len(target_df),
            "row_difference": len(target_df) - len(source_df),
            "consistency": {}
        }
        
        # Row count consistency
        if len(source_df) > 0:
            row_retention = len(target_df) / len(source_df) * 100
        else:
            row_retention = 100
        
        comparison["consistency"]["row_retention_rate"] = round(row_retention, 2)
        
        # Column mapping consistency
        source_cols = set(source_df.columns)
        target_cols = set(target_df.columns)
        
        comparison["consistency"]["column_mapping"] = {
            "source_columns": len(source_cols),
            "target_columns": len(target_cols),
            "common_columns": len(source_cols & target_cols)
        }
        
        # Key consistency (if key column exists in both)
        if key_column in source_df.columns and key_column in target_df.columns:
            source_keys = set(source_df[key_column].dropna().astype(str))
            target_keys = set(target_df[key_column].dropna().astype(str))
            
            missing_keys = source_keys - target_keys
            new_keys = target_keys - source_keys
            
            comparison["consistency"]["key_integrity"] = {
                "source_unique_keys": len(source_keys),
                "target_unique_keys": len(target_keys),
                "missing_in_target": len(missing_keys),
                "new_in_target": len(new_keys),
                "integrity_rate": round(
                    (1 - len(missing_keys) / len(source_keys)) * 100, 2
                ) if source_keys else 100
            }
        
        # Record comparison metrics
        self._persist_comparison_metrics(comparison)
        
        return comparison
    
    def _persist_comparison_metrics(self, comparison: Dict):
        """Persist stage comparison metrics."""
        try:
            with self.db_conn.cursor() as cur:
                table_name = comparison["table_name"]
                
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('stage_row_retention_rate', 'gauge', %s, %s)
                """, (
                    comparison["consistency"].get("row_retention_rate", 0),
                    Json({"table_name": table_name})
                ))
                
                key_integrity = comparison["consistency"].get("key_integrity", {})
                if key_integrity:
                    cur.execute("""
                        INSERT INTO quality.metrics 
                        (metric_name, metric_type, metric_value, labels)
                        VALUES ('stage_key_integrity_rate', 'gauge', %s, %s)
                    """, (
                        key_integrity.get("integrity_rate", 100),
                        Json({"table_name": table_name})
                    ))
                
                self.db_conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to persist comparison metrics: {e}")
            self.db_conn.rollback()

