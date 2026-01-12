"""
Alert Manager
=============

Manages alerts for the Propwise data platform.

Supports:
- PostgreSQL alert storage
- Slack notifications
- Email notifications (SMTP)
- PagerDuty integration
- Alert deduplication
- Alert acknowledgment and resolution
"""

import json
import logging
import smtplib
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import hashlib

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

import psycopg2
from psycopg2.extras import RealDictCursor, Json

logger = logging.getLogger(__name__)


class AlertManager:
    """
    Manages alerts with multiple notification channels.
    
    Features:
    - Alert persistence to PostgreSQL
    - Multi-channel notifications (Slack, Email, PagerDuty)
    - Alert deduplication
    - Severity-based routing
    - Alert lifecycle management (acknowledge, resolve)
    """
    
    # Alert severity levels
    SEVERITY_LEVELS = {
        "info": 0,
        "warning": 1,
        "error": 2,
        "critical": 3
    }
    
    # Alert types
    ALERT_TYPES = [
        "pipeline_failure",
        "quality_violation",
        "schema_drift",
        "threshold_breach",
        "freshness_violation",
        "system_error"
    ]
    
    def __init__(
        self,
        postgres_config: Optional[Dict] = None,
        slack_webhook_url: Optional[str] = None,
        pagerduty_routing_key: Optional[str] = None,
        smtp_config: Optional[Dict] = None,
        dedup_window_minutes: int = 60
    ):
        """
        Initialize Alert Manager.
        
        Args:
            postgres_config: PostgreSQL connection config
            slack_webhook_url: Slack incoming webhook URL
            pagerduty_routing_key: PagerDuty events API routing key
            smtp_config: SMTP configuration for email alerts
            dedup_window_minutes: Window for alert deduplication
        """
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        
        self.slack_webhook_url = slack_webhook_url
        self.pagerduty_routing_key = pagerduty_routing_key
        self.smtp_config = smtp_config
        self.dedup_window_minutes = dedup_window_minutes
        
        self._db_conn = None
    
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
    
    def _generate_dedup_key(
        self,
        alert_type: str,
        source: str,
        title: str
    ) -> str:
        """Generate deduplication key for an alert."""
        key_string = f"{alert_type}:{source}:{title}"
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]
    
    def _is_duplicate(self, dedup_key: str) -> bool:
        """Check if an alert is a duplicate within the dedup window."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM quality.alerts
                    WHERE alert_metadata->>'dedup_key' = %s
                    AND created_at > NOW() - INTERVAL '%s minutes'
                    AND status != 'resolved'
                """, (dedup_key, self.dedup_window_minutes))
                count = cur.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.error(f"Dedup check failed: {e}")
            return False
    
    # =========================================
    # ALERT CREATION
    # =========================================
    
    def send_alert(
        self,
        severity: str,
        title: str,
        message: str,
        alert_type: str = "system_error",
        source: str = "propwise_etl",
        metadata: Optional[Dict] = None,
        notify: bool = True,
        dedup: bool = True
    ) -> Optional[int]:
        """
        Create and send an alert.
        
        Args:
            severity: info, warning, error, critical
            title: Alert title
            message: Alert message/description
            alert_type: Type of alert
            source: Source system/pipeline
            metadata: Additional context
            notify: Whether to send notifications
            dedup: Whether to check for duplicates
            
        Returns:
            Alert ID if created, None if deduplicated
        """
        severity = severity.lower()
        if severity not in self.SEVERITY_LEVELS:
            severity = "warning"
        
        # Generate dedup key
        dedup_key = self._generate_dedup_key(alert_type, source, title)
        
        # Check for duplicates
        if dedup and self._is_duplicate(dedup_key):
            logger.debug(f"Alert deduplicated: {title}")
            return None
        
        # Prepare metadata
        alert_metadata = metadata or {}
        alert_metadata["dedup_key"] = dedup_key
        
        # Persist to database
        alert_id = self._persist_alert(
            alert_type=alert_type,
            severity=severity,
            source=source,
            title=title,
            message=message,
            metadata=alert_metadata
        )
        
        logger.warning(f"Alert created [{severity.upper()}]: {title}")
        
        # Send notifications
        if notify and alert_id:
            self._send_notifications(
                severity=severity,
                title=title,
                message=message,
                alert_type=alert_type,
                source=source,
                alert_id=alert_id,
                metadata=alert_metadata
            )
        
        return alert_id
    
    def _persist_alert(
        self,
        alert_type: str,
        severity: str,
        source: str,
        title: str,
        message: str,
        metadata: Dict
    ) -> int:
        """Persist alert to PostgreSQL."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality.alerts 
                    (alert_type, severity, source, title, message, alert_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING alert_id
                """, (alert_type, severity, source, title, message, Json(metadata)))
                alert_id = cur.fetchone()[0]
                self.db_conn.commit()
                return alert_id
        except Exception as e:
            logger.error(f"Failed to persist alert: {e}")
            self.db_conn.rollback()
            return 0
    
    # =========================================
    # NOTIFICATION CHANNELS
    # =========================================
    
    def _send_notifications(
        self,
        severity: str,
        title: str,
        message: str,
        alert_type: str,
        source: str,
        alert_id: int,
        metadata: Dict
    ):
        """Send notifications via configured channels."""
        # Slack for all severities
        if self.slack_webhook_url:
            self._send_slack_notification(
                severity, title, message, alert_type, source, alert_id
            )
        
        # PagerDuty for critical/error
        if self.pagerduty_routing_key and severity in ["critical", "error"]:
            self._send_pagerduty_notification(
                severity, title, message, alert_type, source, alert_id
            )
        
        # Email for critical
        if self.smtp_config and severity == "critical":
            self._send_email_notification(
                severity, title, message, alert_type, source, alert_id
            )
    
    def _send_slack_notification(
        self,
        severity: str,
        title: str,
        message: str,
        alert_type: str,
        source: str,
        alert_id: int
    ):
        """Send notification to Slack."""
        if not REQUESTS_AVAILABLE:
            logger.warning("requests library not available for Slack notifications")
            return
        
        # Severity colors
        colors = {
            "info": "#36a64f",
            "warning": "#ffcc00",
            "error": "#ff6600",
            "critical": "#ff0000"
        }
        
        # Severity emojis
        emojis = {
            "info": ":information_source:",
            "warning": ":warning:",
            "error": ":x:",
            "critical": ":rotating_light:"
        }
        
        payload = {
            "attachments": [{
                "color": colors.get(severity, "#808080"),
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emojis.get(severity, '')} [{severity.upper()}] {title}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        }
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Source:* {source} | *Type:* {alert_type} | *ID:* {alert_id}"
                            }
                        ]
                    }
                ]
            }]
        }
        
        try:
            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10
            )
            if response.status_code == 200:
                logger.debug("Slack notification sent")
            else:
                logger.warning(f"Slack notification failed: {response.status_code}")
        except Exception as e:
            logger.error(f"Slack notification error: {e}")
    
    def _send_pagerduty_notification(
        self,
        severity: str,
        title: str,
        message: str,
        alert_type: str,
        source: str,
        alert_id: int
    ):
        """Send notification to PagerDuty."""
        if not REQUESTS_AVAILABLE:
            return
        
        # Map severity to PagerDuty severity
        pd_severity = {
            "critical": "critical",
            "error": "error",
            "warning": "warning",
            "info": "info"
        }.get(severity, "warning")
        
        payload = {
            "routing_key": self.pagerduty_routing_key,
            "event_action": "trigger",
            "dedup_key": f"propwise-alert-{alert_id}",
            "payload": {
                "summary": f"[{severity.upper()}] {title}",
                "severity": pd_severity,
                "source": source,
                "custom_details": {
                    "message": message,
                    "alert_type": alert_type,
                    "alert_id": alert_id
                }
            }
        }
        
        try:
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10
            )
            if response.status_code == 202:
                logger.debug("PagerDuty notification sent")
            else:
                logger.warning(f"PagerDuty notification failed: {response.status_code}")
        except Exception as e:
            logger.error(f"PagerDuty notification error: {e}")
    
    def _send_email_notification(
        self,
        severity: str,
        title: str,
        message: str,
        alert_type: str,
        source: str,
        alert_id: int
    ):
        """Send email notification."""
        if not self.smtp_config:
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config.get('from', 'alerts@propwise.com')
            msg['To'] = self.smtp_config.get('to', 'data-team@propwise.com')
            msg['Subject'] = f"[{severity.upper()}] Propwise Alert: {title}"
            
            body = f"""
Propwise Data Platform Alert
============================

Severity: {severity.upper()}
Type: {alert_type}
Source: {source}
Alert ID: {alert_id}

Title: {title}

Message:
{message}

---
This is an automated alert from the Propwise Data Platform.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(
                self.smtp_config.get('host', 'localhost'),
                self.smtp_config.get('port', 587)
            )
            
            if self.smtp_config.get('use_tls'):
                server.starttls()
            
            if self.smtp_config.get('username'):
                server.login(
                    self.smtp_config['username'],
                    self.smtp_config['password']
                )
            
            server.send_message(msg)
            server.quit()
            
            logger.debug("Email notification sent")
            
        except Exception as e:
            logger.error(f"Email notification error: {e}")
    
    # =========================================
    # ALERT MANAGEMENT
    # =========================================
    
    def acknowledge_alert(
        self,
        alert_id: int,
        acknowledged_by: str = "system"
    ) -> bool:
        """Acknowledge an alert."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    UPDATE quality.alerts 
                    SET status = 'acknowledged', 
                        acknowledged_at = NOW()
                    WHERE alert_id = %s AND status = 'open'
                """, (alert_id,))
                self.db_conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to acknowledge alert: {e}")
            self.db_conn.rollback()
            return False
    
    def resolve_alert(
        self,
        alert_id: int,
        resolution_note: Optional[str] = None
    ) -> bool:
        """Resolve an alert."""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    UPDATE quality.alerts 
                    SET status = 'resolved', 
                        resolved_at = NOW(),
                        alert_metadata = alert_metadata || %s
                    WHERE alert_id = %s AND status != 'resolved'
                """, (Json({"resolution_note": resolution_note}), alert_id))
                self.db_conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to resolve alert: {e}")
            self.db_conn.rollback()
            return False
    
    def get_open_alerts(
        self,
        severity: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get open alerts."""
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                if severity:
                    cur.execute("""
                        SELECT * FROM quality.alerts
                        WHERE status = 'open' AND severity = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (severity, limit))
                else:
                    cur.execute("""
                        SELECT * FROM quality.alerts
                        WHERE status = 'open'
                        ORDER BY 
                            CASE severity 
                                WHEN 'critical' THEN 0 
                                WHEN 'error' THEN 1 
                                WHEN 'warning' THEN 2 
                                ELSE 3 
                            END,
                            created_at DESC
                        LIMIT %s
                    """, (limit,))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []
    
    # =========================================
    # CONVENIENCE METHODS
    # =========================================
    
    def alert_pipeline_failure(
        self,
        pipeline_name: str,
        error_message: str,
        task_name: Optional[str] = None
    ) -> int:
        """Create alert for pipeline failure."""
        return self.send_alert(
            severity="critical",
            title=f"Pipeline Failed: {pipeline_name}",
            message=f"Pipeline '{pipeline_name}' failed.\n\nError: {error_message}",
            alert_type="pipeline_failure",
            source=pipeline_name,
            metadata={"task_name": task_name} if task_name else None
        )
    
    def alert_quality_violation(
        self,
        table_name: str,
        check_name: str,
        details: str,
        severity: str = "warning"
    ) -> int:
        """Create alert for data quality violation."""
        return self.send_alert(
            severity=severity,
            title=f"Quality Check Failed: {check_name}",
            message=f"Quality check '{check_name}' failed for table '{table_name}'.\n\nDetails: {details}",
            alert_type="quality_violation",
            source=f"quality_check_{table_name}",
            metadata={"table_name": table_name, "check_name": check_name}
        )
    
    def alert_schema_drift(
        self,
        table_name: str,
        changes: List[Dict]
    ) -> int:
        """Create alert for schema drift."""
        change_summary = "\n".join([
            f"- {c['change_type']}: {c.get('column_name', 'N/A')}"
            for c in changes
        ])
        
        return self.send_alert(
            severity="warning",
            title=f"Schema Drift Detected: {table_name}",
            message=f"Schema changes detected for table '{table_name}':\n\n{change_summary}",
            alert_type="schema_drift",
            source=f"schema_registry_{table_name}",
            metadata={"changes": changes}
        )
    
    def alert_freshness_violation(
        self,
        table_name: str,
        hours_stale: float,
        threshold_hours: float
    ) -> int:
        """Create alert for data freshness violation."""
        return self.send_alert(
            severity="error",
            title=f"Stale Data: {table_name}",
            message=f"Table '{table_name}' has not been updated in {hours_stale:.1f} hours (threshold: {threshold_hours} hours).",
            alert_type="freshness_violation",
            source=f"freshness_monitor_{table_name}",
            metadata={"hours_stale": hours_stale, "threshold_hours": threshold_hours}
        )

