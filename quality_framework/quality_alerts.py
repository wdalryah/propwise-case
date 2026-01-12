"""
Quality Alert Manager
=====================

Integrates quality framework with observability alerts.
Provides specialized alerting for:
- Schema drift
- Quality dimension violations
- PII detection
- Rejection thresholds
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


class QualityAlertManager:
    """
    Manages quality-specific alerts and integrates with observability.
    
    Alert types:
    - schema_drift: New/removed/changed columns
    - dimension_violation: Quality scores below threshold
    - pii_detected: Sensitive data found
    - rejection_threshold: Too many rejected records
    - freshness_violation: Stale data
    """
    
    # Default thresholds
    THRESHOLDS = {
        "completeness_min": 80.0,
        "uniqueness_min": 90.0,
        "validity_min": 90.0,
        "timeliness_min": 70.0,
        "overall_quality_min": 75.0,
        "rejection_rate_max": 5.0,
        "integrity_score_min": 95.0
    }
    
    def __init__(
        self,
        postgres_config: Optional[Dict] = None,
        thresholds: Optional[Dict] = None
    ):
        """Initialize quality alert manager."""
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        self.thresholds = {**self.THRESHOLDS, **(thresholds or {})}
        self._db_conn = None
    
    @property
    def db_conn(self):
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    def close(self):
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
    
    # =========================================
    # ALERT CREATION
    # =========================================
    
    def _create_alert(
        self,
        alert_type: str,
        severity: str,
        source: str,
        title: str,
        message: str,
        metadata: Optional[Dict] = None
    ) -> int:
        """Create an alert in the database."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality.alerts 
                    (alert_type, severity, source, title, message, alert_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING alert_id
                """, (
                    alert_type,
                    severity,
                    source,
                    title,
                    message,
                    Json(metadata) if metadata else None
                ))
                alert_id = cur.fetchone()[0]
                self.db_conn.commit()
                
                logger.warning(f"Alert [{severity.upper()}]: {title}")
                return alert_id
                
        except Exception as e:
            logger.error(f"Failed to create alert: {e}")
            self.db_conn.rollback()
            return 0
    
    # =========================================
    # SCHEMA DRIFT ALERTS
    # =========================================
    
    def check_schema_drift(
        self,
        validation_result: Dict
    ) -> List[int]:
        """
        Check schema validation result and create alerts if needed.
        
        Returns list of alert IDs created.
        """
        alerts = []
        table_name = validation_result.get("table_name", "unknown")
        
        if not validation_result.get("has_drift"):
            return alerts
        
        drift = validation_result.get("drift_details", {})
        
        # New columns alert
        new_cols = drift.get("new_columns", [])
        if new_cols:
            alert_id = self._create_alert(
                alert_type="schema_drift",
                severity="warning",
                source=f"schema_contract_{table_name}",
                title=f"New columns detected in {table_name}",
                message=f"Detected {len(new_cols)} new column(s): {new_cols}. "
                        f"Consider updating staging config if intentional.",
                metadata={
                    "table_name": table_name,
                    "new_columns": new_cols,
                    "drift_type": "column_added"
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        # Missing columns alert (more severe)
        missing_cols = drift.get("missing_columns", [])
        if missing_cols:
            alert_id = self._create_alert(
                alert_type="schema_drift",
                severity="error",
                source=f"schema_contract_{table_name}",
                title=f"Missing columns in {table_name}",
                message=f"Expected column(s) not found: {missing_cols}. "
                        f"This may cause downstream failures.",
                metadata={
                    "table_name": table_name,
                    "missing_columns": missing_cols,
                    "drift_type": "column_removed"
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        # Integrity score alert
        integrity = validation_result.get("integrity_score", 100)
        if integrity < self.thresholds["integrity_score_min"]:
            alert_id = self._create_alert(
                alert_type="schema_drift",
                severity="error" if integrity < 80 else "warning",
                source=f"schema_contract_{table_name}",
                title=f"Low schema integrity score for {table_name}",
                message=f"Schema integrity score is {integrity:.1f}% "
                        f"(threshold: {self.thresholds['integrity_score_min']}%).",
                metadata={
                    "table_name": table_name,
                    "integrity_score": integrity,
                    "threshold": self.thresholds["integrity_score_min"]
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        return alerts
    
    # =========================================
    # DIMENSION ALERTS
    # =========================================
    
    def check_dimension_scores(
        self,
        dimension_metrics: Dict
    ) -> List[int]:
        """
        Check dimension metrics and create alerts for violations.
        
        Returns list of alert IDs created.
        """
        alerts = []
        table_name = dimension_metrics.get("table_name", "unknown")
        stage = dimension_metrics.get("stage", "unknown")
        dimensions = dimension_metrics.get("dimensions", {})
        
        # Check each dimension
        dimension_checks = [
            ("completeness", "completeness_min", "Data completeness"),
            ("uniqueness", "uniqueness_min", "Data uniqueness"),
            ("validity", "validity_min", "Data validity"),
            ("timeliness", "timeliness_min", "Data timeliness")
        ]
        
        for dim_name, threshold_key, display_name in dimension_checks:
            dim_data = dimensions.get(dim_name, {})
            score = dim_data.get("score", 100)
            threshold = self.thresholds.get(threshold_key, 0)
            
            if score < threshold:
                severity = "critical" if score < threshold * 0.5 else "warning"
                
                alert_id = self._create_alert(
                    alert_type="dimension_violation",
                    severity=severity,
                    source=f"quality_metrics_{table_name}",
                    title=f"{display_name} below threshold for {table_name}",
                    message=f"{display_name} score is {score:.1f}% "
                            f"(threshold: {threshold}%) for {table_name}/{stage}.",
                    metadata={
                        "table_name": table_name,
                        "stage": stage,
                        "dimension": dim_name,
                        "score": score,
                        "threshold": threshold,
                        "details": dim_data
                    }
                )
                if alert_id:
                    alerts.append(alert_id)
        
        # Check overall quality
        overall = dimension_metrics.get("overall_quality_score", 100)
        if overall < self.thresholds["overall_quality_min"]:
            alert_id = self._create_alert(
                alert_type="dimension_violation",
                severity="error",
                source=f"quality_metrics_{table_name}",
                title=f"Overall quality score low for {table_name}",
                message=f"Overall quality score is {overall:.1f}% "
                        f"(threshold: {self.thresholds['overall_quality_min']}%).",
                metadata={
                    "table_name": table_name,
                    "stage": stage,
                    "overall_score": overall,
                    "dimension_scores": {
                        k: v.get("score", 0) for k, v in dimensions.items()
                    }
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        return alerts
    
    # =========================================
    # REJECTION ALERTS
    # =========================================
    
    def check_rejection_rate(
        self,
        rejection_summary: Dict
    ) -> List[int]:
        """
        Check rejection summary and create alerts if threshold exceeded.
        """
        alerts = []
        table_name = rejection_summary.get("table_name", "unknown")
        rejection_rate = rejection_summary.get("rejection_rate", 0)
        
        if rejection_rate > self.thresholds["rejection_rate_max"]:
            severity = "critical" if rejection_rate > 10 else "warning"
            
            alert_id = self._create_alert(
                alert_type="rejection_threshold",
                severity=severity,
                source=f"rejection_handler_{table_name}",
                title=f"High rejection rate for {table_name}",
                message=f"Rejection rate is {rejection_rate:.2f}% "
                        f"({rejection_summary['rejected_records']}/{rejection_summary['total_records']} records). "
                        f"Threshold: {self.thresholds['rejection_rate_max']}%.",
                metadata={
                    "table_name": table_name,
                    "rejection_rate": rejection_rate,
                    "rejected_records": rejection_summary["rejected_records"],
                    "total_records": rejection_summary["total_records"],
                    "violations": rejection_summary.get("violations", {})
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        # Alert on critical violations (credit cards, SSN)
        violations = rejection_summary.get("violations", {})
        for rule_name, details in violations.items():
            if details.get("severity") == "critical":
                alert_id = self._create_alert(
                    alert_type="pii_detected",
                    severity="critical",
                    source=f"rejection_handler_{table_name}",
                    title=f"Critical data violation: {rule_name}",
                    message=f"Detected {details['count']} records with critical violation "
                            f"'{rule_name}' in column(s): {details['columns']}.",
                    metadata={
                        "table_name": table_name,
                        "rule_name": rule_name,
                        "count": details["count"],
                        "columns": details["columns"]
                    }
                )
                if alert_id:
                    alerts.append(alert_id)
        
        return alerts
    
    # =========================================
    # PII ALERTS
    # =========================================
    
    def check_pii_detection(
        self,
        classification_result: Dict
    ) -> List[int]:
        """
        Check PII detection results and create alerts.
        """
        alerts = []
        table_name = classification_result.get("table_name", "unknown")
        summary = classification_result.get("summary", {})
        
        pii_types = summary.get("pii_types_found", [])
        columns_with_pii = summary.get("columns_with_pii", 0)
        
        if not pii_types:
            return alerts
        
        # High sensitivity PII
        high_sensitivity_types = {"credit_card", "ssn", "emirates_id"}
        found_high_sensitivity = set(pii_types) & high_sensitivity_types
        
        if found_high_sensitivity:
            alert_id = self._create_alert(
                alert_type="pii_detected",
                severity="critical",
                source=f"pii_detector_{table_name}",
                title=f"High-sensitivity PII detected in {table_name}",
                message=f"Detected high-sensitivity PII types: {list(found_high_sensitivity)}. "
                        f"Immediate action required.",
                metadata={
                    "table_name": table_name,
                    "pii_types": list(found_high_sensitivity),
                    "columns_affected": columns_with_pii
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        # Standard PII
        standard_pii = set(pii_types) - high_sensitivity_types
        if standard_pii:
            alert_id = self._create_alert(
                alert_type="pii_detected",
                severity="warning",
                source=f"pii_detector_{table_name}",
                title=f"PII detected in {table_name}",
                message=f"Detected PII types: {list(standard_pii)} "
                        f"in {columns_with_pii} column(s). Review data classification.",
                metadata={
                    "table_name": table_name,
                    "pii_types": list(standard_pii),
                    "columns_affected": columns_with_pii
                }
            )
            if alert_id:
                alerts.append(alert_id)
        
        return alerts
    
    # =========================================
    # BATCH CHECK
    # =========================================
    
    def run_all_checks(
        self,
        schema_result: Optional[Dict] = None,
        dimension_metrics: Optional[Dict] = None,
        rejection_summary: Optional[Dict] = None,
        pii_result: Optional[Dict] = None
    ) -> Dict:
        """
        Run all alert checks and return summary.
        """
        all_alerts = []
        
        if schema_result:
            all_alerts.extend(self.check_schema_drift(schema_result))
        
        if dimension_metrics:
            all_alerts.extend(self.check_dimension_scores(dimension_metrics))
        
        if rejection_summary:
            all_alerts.extend(self.check_rejection_rate(rejection_summary))
        
        if pii_result:
            all_alerts.extend(self.check_pii_detection(pii_result))
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_alerts": len(all_alerts),
            "alert_ids": all_alerts,
            "checks_performed": {
                "schema_drift": schema_result is not None,
                "dimension_scores": dimension_metrics is not None,
                "rejection_rate": rejection_summary is not None,
                "pii_detection": pii_result is not None
            }
        }

