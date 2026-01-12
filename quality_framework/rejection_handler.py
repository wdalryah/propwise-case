"""
Rejection Handler & Data Quarantine (Soft Contract)
====================================================

Handles records that violate quality rules by quarantining them
to a rejection bucket. Pipeline continues (soft contract).

Features:
- Rule-based rejection (JSON blobs, PII, invalid formats)
- PII detection and masking
- Quarantine bucket management
- Rejection reason tracking
- Metrics and alerting integration
"""

import json
import re
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime
from pathlib import Path
from io import BytesIO
import hashlib

import pandas as pd
from minio import Minio

import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


# =========================================
# PII PATTERNS
# =========================================

PII_PATTERNS = {
    "email": {
        "pattern": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        "sensitivity": 3,
        "mask": "***@***.***"
    },
    "phone": {
        "pattern": r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b',
        "sensitivity": 3,
        "mask": "***-***-****"
    },
    "credit_card": {
        "pattern": r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b',
        "sensitivity": 5,
        "mask": "****-****-****-****"
    },
    "ssn": {
        "pattern": r'\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b',
        "sensitivity": 5,
        "mask": "***-**-****"
    },
    "ip_address": {
        "pattern": r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b',
        "sensitivity": 2,
        "mask": "***.***.***.***"
    },
    "uae_phone": {
        "pattern": r'\b(?:\+971|00971|971)?[-\s]?(?:50|52|54|55|56|58)[-\s]?[0-9]{3}[-\s]?[0-9]{4}\b',
        "sensitivity": 3,
        "mask": "+971-**-***-****"
    },
    "emirates_id": {
        "pattern": r'\b784-[0-9]{4}-[0-9]{7}-[0-9]\b',
        "sensitivity": 5,
        "mask": "784-****-*******-*"
    }
}


class RejectionRule:
    """Defines a rejection rule."""
    
    def __init__(
        self,
        name: str,
        rule_type: str,
        columns: Optional[List[str]] = None,
        pattern: Optional[str] = None,
        condition: Optional[str] = None,
        severity: str = "warning",
        action: str = "quarantine"  # quarantine, mask, drop
    ):
        self.name = name
        self.rule_type = rule_type  # json_blob, pii, regex, custom
        self.columns = columns or []
        self.pattern = re.compile(pattern) if pattern else None
        self.condition = condition
        self.severity = severity
        self.action = action


# Default rejection rules
DEFAULT_RULES = [
    RejectionRule(
        name="json_blob_in_text",
        rule_type="json_blob",
        columns=["location", "status_name", "method_of_contact", "lead_source"],
        severity="warning",
        action="quarantine"
    ),
    RejectionRule(
        name="credit_card_detected",
        rule_type="pii",
        pattern=PII_PATTERNS["credit_card"]["pattern"],
        severity="critical",
        action="quarantine"
    ),
    RejectionRule(
        name="ssn_detected",
        rule_type="pii",
        pattern=PII_PATTERNS["ssn"]["pattern"],
        severity="critical",
        action="quarantine"
    ),
    RejectionRule(
        name="email_in_unexpected_column",
        rule_type="pii",
        columns=["budget", "user_id", "lead_type_id"],
        pattern=PII_PATTERNS["email"]["pattern"],
        severity="warning",
        action="mask"
    )
]


class RejectionHandler:
    """
    Handles rejection of records that violate quality rules.
    
    Soft contract: Pipeline continues, bad records quarantined.
    """
    
    REJECTED_BUCKET = "rejected-data"
    
    def __init__(
        self,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        postgres_config: Optional[Dict] = None,
        rules: Optional[List[RejectionRule]] = None
    ):
        """Initialize rejection handler."""
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        
        self.rules = rules or DEFAULT_RULES
        self._db_conn = None
        self._ensure_bucket()
    
    @property
    def db_conn(self):
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    def close(self):
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
    
    def _ensure_bucket(self):
        """Ensure rejection bucket exists."""
        try:
            if not self.minio_client.bucket_exists(self.REJECTED_BUCKET):
                self.minio_client.make_bucket(self.REJECTED_BUCKET)
                logger.info(f"Created rejection bucket: {self.REJECTED_BUCKET}")
        except Exception as e:
            logger.error(f"Failed to create rejection bucket: {e}")
    
    # =========================================
    # RULE CHECKING
    # =========================================
    
    def check_record(self, record: Dict) -> List[Dict]:
        """
        Check a single record against all rules.
        
        Returns list of violations found.
        """
        violations = []
        
        for rule in self.rules:
            violation = self._check_rule(record, rule)
            if violation:
                violations.append(violation)
        
        return violations
    
    def _check_rule(self, record: Dict, rule: RejectionRule) -> Optional[Dict]:
        """Check a record against a single rule."""
        columns_to_check = rule.columns if rule.columns else list(record.keys())
        
        for col in columns_to_check:
            if col not in record:
                continue
            
            value = record[col]
            if value is None or pd.isna(value):
                continue
            
            value_str = str(value)
            
            if rule.rule_type == "json_blob":
                if self._is_json_blob(value_str):
                    return {
                        "rule_name": rule.name,
                        "rule_type": rule.rule_type,
                        "column": col,
                        "value_sample": value_str[:100],
                        "severity": rule.severity,
                        "action": rule.action
                    }
            
            elif rule.rule_type == "pii" and rule.pattern:
                if rule.pattern.search(value_str):
                    return {
                        "rule_name": rule.name,
                        "rule_type": rule.rule_type,
                        "column": col,
                        "value_sample": "***REDACTED***",
                        "severity": rule.severity,
                        "action": rule.action
                    }
            
            elif rule.rule_type == "regex" and rule.pattern:
                if rule.pattern.search(value_str):
                    return {
                        "rule_name": rule.name,
                        "rule_type": rule.rule_type,
                        "column": col,
                        "value_sample": value_str[:100],
                        "severity": rule.severity,
                        "action": rule.action
                    }
        
        return None
    
    def _is_json_blob(self, value: str) -> bool:
        """Check if a value is a JSON blob."""
        value = value.strip()
        if (value.startswith('{') and value.endswith('}')) or \
           (value.startswith('[') and value.endswith(']')):
            try:
                json.loads(value)
                return True
            except:
                pass
        return False
    
    # =========================================
    # DATAFRAME PROCESSING
    # =========================================
    
    def process_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """
        Process a DataFrame, separating clean and rejected records.
        
        Args:
            df: Input DataFrame
            table_name: Table name for tracking
            
        Returns:
            Tuple of (clean_df, rejected_df, summary)
        """
        clean_records = []
        rejected_records = []
        violation_summary = {}
        
        for idx, row in df.iterrows():
            record = row.to_dict()
            violations = self.check_record(record)
            
            if violations:
                # Add violation metadata
                record["_rejection_reason"] = [v["rule_name"] for v in violations]
                record["_rejection_time"] = datetime.now().isoformat()
                record["_original_index"] = idx
                rejected_records.append(record)
                
                # Track violations
                for v in violations:
                    rule_name = v["rule_name"]
                    if rule_name not in violation_summary:
                        violation_summary[rule_name] = {
                            "count": 0,
                            "severity": v["severity"],
                            "columns": set()
                        }
                    violation_summary[rule_name]["count"] += 1
                    violation_summary[rule_name]["columns"].add(v["column"])
            else:
                clean_records.append(record)
        
        # Convert to DataFrames
        clean_df = pd.DataFrame(clean_records) if clean_records else pd.DataFrame()
        rejected_df = pd.DataFrame(rejected_records) if rejected_records else pd.DataFrame()
        
        # Build summary
        summary = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "total_records": len(df),
            "clean_records": len(clean_df),
            "rejected_records": len(rejected_df),
            "rejection_rate": round(len(rejected_df) / len(df) * 100, 2) if len(df) > 0 else 0,
            "violations": {
                k: {"count": v["count"], "severity": v["severity"], "columns": list(v["columns"])}
                for k, v in violation_summary.items()
            }
        }
        
        # Log and persist
        self._log_rejections(summary)
        self._record_metrics(summary)
        
        return clean_df, rejected_df, summary
    
    # =========================================
    # QUARANTINE
    # =========================================
    
    def quarantine_records(
        self,
        rejected_df: pd.DataFrame,
        table_name: str,
        batch_id: str
    ) -> str:
        """
        Write rejected records to quarantine bucket.
        
        Returns path to quarantined file.
        """
        if rejected_df.empty:
            return ""
        
        # Create path
        date_path = datetime.now().strftime("%Y/%m/%d")
        filename = f"rejected_{table_name}_{batch_id}.parquet"
        object_path = f"{table_name}/{date_path}/{filename}"
        
        # Write to buffer
        buffer = BytesIO()
        rejected_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        # Upload to MinIO
        try:
            self.minio_client.put_object(
                self.REJECTED_BUCKET,
                object_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"Quarantined {len(rejected_df)} records to {object_path}")
            return f"minio://{self.REJECTED_BUCKET}/{object_path}"
            
        except Exception as e:
            logger.error(f"Failed to quarantine records: {e}")
            return ""
    
    def _log_rejections(self, summary: Dict):
        """Log rejection summary to database."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality.logs 
                    (log_level, logger_name, source, message, log_metadata)
                    VALUES ('WARNING', 'rejection_handler', %s, %s, %s)
                """, (
                    summary["table_name"],
                    f"Rejected {summary['rejected_records']}/{summary['total_records']} records ({summary['rejection_rate']}%)",
                    Json(summary)
                ))
                self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to log rejections: {e}")
            self.db_conn.rollback()
    
    def _record_metrics(self, summary: Dict):
        """Record rejection metrics."""
        try:
            with self.db_conn.cursor() as cur:
                table_name = summary["table_name"]
                
                # Total rejected
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('rejected_records_total', 'counter', %s, %s)
                """, (summary["rejected_records"], Json({"table_name": table_name})))
                
                # Rejection rate
                cur.execute("""
                    INSERT INTO quality.metrics 
                    (metric_name, metric_type, metric_value, labels)
                    VALUES ('rejection_rate', 'gauge', %s, %s)
                """, (summary["rejection_rate"], Json({"table_name": table_name})))
                
                # Per-rule counts
                for rule_name, details in summary.get("violations", {}).items():
                    cur.execute("""
                        INSERT INTO quality.metrics 
                        (metric_name, metric_type, metric_value, labels)
                        VALUES ('rejection_by_rule', 'counter', %s, %s)
                    """, (details["count"], Json({
                        "table_name": table_name,
                        "rule_name": rule_name,
                        "severity": details["severity"]
                    })))
                
                self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to record metrics: {e}")
            self.db_conn.rollback()


class PIIDetector:
    """
    Detects and classifies PII in data.
    
    Used for data classification and governance.
    """
    
    def __init__(self, postgres_config: Optional[Dict] = None):
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
    
    def scan_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        sample_size: int = 1000
    ) -> Dict:
        """
        Scan DataFrame for PII patterns.
        
        Returns classification results per column.
        """
        results = {
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
            "columns": {},
            "summary": {
                "total_columns": len(df.columns),
                "columns_with_pii": 0,
                "pii_types_found": set()
            }
        }
        
        for col in df.columns:
            col_result = self._scan_column(df[col].head(sample_size), col)
            results["columns"][col] = col_result
            
            if col_result["pii_detected"]:
                results["summary"]["columns_with_pii"] += 1
                results["summary"]["pii_types_found"].update(col_result["pii_types"])
        
        results["summary"]["pii_types_found"] = list(results["summary"]["pii_types_found"])
        
        # Persist classification
        self._persist_classification(table_name, results)
        
        return results
    
    def _scan_column(self, col_data: pd.Series, col_name: str) -> Dict:
        """Scan a column for PII patterns."""
        result = {
            "column_name": col_name,
            "pii_detected": False,
            "pii_types": [],
            "match_counts": {},
            "sensitivity_level": 0,
            "requires_masking": False
        }
        
        for pii_type, config in PII_PATTERNS.items():
            pattern = re.compile(config["pattern"])
            matches = 0
            
            for value in col_data.dropna().astype(str):
                if pattern.search(value):
                    matches += 1
            
            if matches > 0:
                result["pii_detected"] = True
                result["pii_types"].append(pii_type)
                result["match_counts"][pii_type] = matches
                result["sensitivity_level"] = max(
                    result["sensitivity_level"],
                    config["sensitivity"]
                )
        
        if result["sensitivity_level"] >= 3:
            result["requires_masking"] = True
        
        return result
    
    def _persist_classification(self, table_name: str, results: Dict):
        """Persist PII classification to governance tables."""
        try:
            with self.db_conn.cursor() as cur:
                for col_name, col_result in results["columns"].items():
                    if col_result["pii_detected"]:
                        # Determine classification type
                        if "credit_card" in col_result["pii_types"] or "ssn" in col_result["pii_types"]:
                            classification = "financial"
                        elif "email" in col_result["pii_types"] or "phone" in col_result["pii_types"]:
                            classification = "PII"
                        else:
                            classification = "sensitive"
                        
                        cur.execute("""
                            INSERT INTO governance.data_classification 
                            (table_name, column_name, classification_type, sensitivity_level, 
                             requires_encryption, requires_masking)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (table_name, column_name) 
                            DO UPDATE SET
                                classification_type = EXCLUDED.classification_type,
                                sensitivity_level = EXCLUDED.sensitivity_level,
                                requires_masking = EXCLUDED.requires_masking
                        """, (
                            table_name,
                            col_name,
                            classification,
                            col_result["sensitivity_level"],
                            col_result["sensitivity_level"] >= 4,
                            col_result["requires_masking"]
                        ))
                
                self.db_conn.commit()
                logger.info(f"PII classification persisted for {table_name}")
                
        except Exception as e:
            logger.error(f"Failed to persist classification: {e}")
            self.db_conn.rollback()
    
    def mask_pii(
        self,
        df: pd.DataFrame,
        table_name: str,
        classification_results: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Mask PII in DataFrame based on classification.
        
        Returns DataFrame with PII masked.
        """
        if classification_results is None:
            classification_results = self.scan_dataframe(df, table_name)
        
        masked_df = df.copy()
        
        for col_name, col_result in classification_results["columns"].items():
            if col_result["requires_masking"] and col_name in masked_df.columns:
                # Apply masking for each PII type found
                for pii_type in col_result["pii_types"]:
                    if pii_type in PII_PATTERNS:
                        pattern = re.compile(PII_PATTERNS[pii_type]["pattern"])
                        mask = PII_PATTERNS[pii_type]["mask"]
                        
                        masked_df[col_name] = masked_df[col_name].astype(str).apply(
                            lambda x: pattern.sub(mask, x) if pd.notna(x) else x
                        )
        
        return masked_df

