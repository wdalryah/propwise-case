"""
Structured Logger
=================

Provides structured logging for the Propwise data platform.

Features:
- JSON-formatted logs
- PostgreSQL persistence
- Context enrichment
- Log correlation (trace_id, run_id)
- Multiple output handlers
"""

import json
import logging
import sys
import traceback
from typing import Dict, Optional, Any, List
from datetime import datetime
from contextlib import contextmanager
import threading
import uuid

import psycopg2
from psycopg2.extras import Json

# Thread-local storage for context
_context = threading.local()


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add context from thread-local storage
        if hasattr(_context, 'data'):
            log_entry["context"] = _context.data.copy()
        
        # Add extra fields
        if self.include_extra:
            extra_keys = set(record.__dict__.keys()) - {
                'name', 'msg', 'args', 'created', 'filename', 'funcName',
                'levelname', 'levelno', 'lineno', 'module', 'msecs',
                'pathname', 'process', 'processName', 'relativeCreated',
                'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                'message', 'taskName'
            }
            for key in extra_keys:
                log_entry[key] = getattr(record, key)
        
        return json.dumps(log_entry, default=str)


class PostgresLogHandler(logging.Handler):
    """Handler that persists logs to PostgreSQL."""
    
    def __init__(
        self,
        postgres_config: Dict,
        batch_size: int = 100,
        flush_interval: float = 5.0
    ):
        super().__init__()
        self.postgres_config = postgres_config
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self._buffer: List[Dict] = []
        self._lock = threading.Lock()
        self._db_conn = None
    
    @property
    def db_conn(self):
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    def emit(self, record: logging.LogRecord):
        """Emit a log record."""
        try:
            log_entry = {
                "log_level": record.levelname,
                "logger_name": record.name,
                "source": record.module,
                "message": record.getMessage(),
                "exception": None,
                "log_metadata": {}
            }
            
            # Add exception if present
            if record.exc_info:
                log_entry["exception"] = "".join(
                    traceback.format_exception(*record.exc_info)
                )
            
            # Add context
            if hasattr(_context, 'data'):
                log_entry["log_metadata"]["context"] = _context.data.copy()
            
            # Add extra fields
            extra_keys = set(record.__dict__.keys()) - {
                'name', 'msg', 'args', 'created', 'filename', 'funcName',
                'levelname', 'levelno', 'lineno', 'module', 'msecs',
                'pathname', 'process', 'processName', 'relativeCreated',
                'stack_info', 'exc_info', 'exc_text', 'thread', 'threadName',
                'message', 'taskName'
            }
            for key in extra_keys:
                log_entry["log_metadata"][key] = getattr(record, key)
            
            with self._lock:
                self._buffer.append(log_entry)
                
                if len(self._buffer) >= self.batch_size:
                    self._flush()
                    
        except Exception:
            self.handleError(record)
    
    def _flush(self):
        """Flush buffered logs to database."""
        if not self._buffer:
            return
        
        try:
            with self.db_conn.cursor() as cur:
                for entry in self._buffer:
                    cur.execute("""
                        INSERT INTO quality.logs 
                        (log_level, logger_name, source, message, exception, log_metadata)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        entry["log_level"],
                        entry["logger_name"],
                        entry["source"],
                        entry["message"],
                        entry["exception"],
                        Json(entry["log_metadata"]) if entry["log_metadata"] else None
                    ))
                self.db_conn.commit()
            self._buffer.clear()
        except Exception as e:
            # Fallback to stderr
            sys.stderr.write(f"Failed to flush logs to PostgreSQL: {e}\n")
            self.db_conn.rollback()
    
    def close(self):
        """Close handler and flush remaining logs."""
        with self._lock:
            self._flush()
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
        super().close()


class StructuredLogger:
    """
    Structured logger with context management and multiple outputs.
    
    Usage:
        logger = StructuredLogger("etl_pipeline")
        
        # Basic logging
        logger.info("Pipeline started", extra={"table": "leads"})
        
        # With context
        with logger.context(run_id="abc123", pipeline="leads_etl"):
            logger.info("Processing data")  # Automatically includes context
            logger.error("Failed", exception=e)
    """
    
    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        enable_console: bool = True,
        enable_postgres: bool = True,
        postgres_config: Optional[Dict] = None,
        json_format: bool = True
    ):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name
            level: Log level
            enable_console: Enable console output
            enable_postgres: Enable PostgreSQL persistence
            postgres_config: PostgreSQL connection config
            json_format: Use JSON formatting for console
        """
        self.name = name
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._logger.handlers = []  # Clear existing handlers
        
        # Console handler
        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            
            if json_format:
                console_handler.setFormatter(JsonFormatter())
            else:
                console_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
            
            self._logger.addHandler(console_handler)
        
        # PostgreSQL handler
        if enable_postgres:
            pg_config = postgres_config or {
                "host": "localhost",
                "port": 5433,
                "database": "propwise_metadata",
                "user": "metadata_user",
                "password": "metadata123"
            }
            
            try:
                pg_handler = PostgresLogHandler(pg_config)
                pg_handler.setLevel(level)
                self._logger.addHandler(pg_handler)
            except Exception as e:
                sys.stderr.write(f"Failed to initialize PostgreSQL log handler: {e}\n")
    
    @contextmanager
    def context(self, **kwargs):
        """
        Context manager for adding context to all logs within scope.
        
        Usage:
            with logger.context(run_id="123", pipeline="etl"):
                logger.info("Processing")  # Includes run_id and pipeline
        """
        if not hasattr(_context, 'data'):
            _context.data = {}
        
        old_data = _context.data.copy()
        _context.data.update(kwargs)
        
        try:
            yield
        finally:
            _context.data = old_data
    
    def _log(
        self,
        level: int,
        message: str,
        extra: Optional[Dict] = None,
        exception: Optional[Exception] = None
    ):
        """Internal logging method."""
        extra = extra or {}
        
        # Add trace_id if not present
        if 'trace_id' not in extra:
            if hasattr(_context, 'data') and 'trace_id' in _context.data:
                extra['trace_id'] = _context.data['trace_id']
        
        if exception:
            self._logger.log(level, message, extra=extra, exc_info=exception)
        else:
            self._logger.log(level, message, extra=extra)
    
    def debug(self, message: str, extra: Optional[Dict] = None):
        """Log debug message."""
        self._log(logging.DEBUG, message, extra)
    
    def info(self, message: str, extra: Optional[Dict] = None):
        """Log info message."""
        self._log(logging.INFO, message, extra)
    
    def warning(self, message: str, extra: Optional[Dict] = None):
        """Log warning message."""
        self._log(logging.WARNING, message, extra)
    
    def error(
        self,
        message: str,
        extra: Optional[Dict] = None,
        exception: Optional[Exception] = None
    ):
        """Log error message."""
        self._log(logging.ERROR, message, extra, exception)
    
    def critical(
        self,
        message: str,
        extra: Optional[Dict] = None,
        exception: Optional[Exception] = None
    ):
        """Log critical message."""
        self._log(logging.CRITICAL, message, extra, exception)
    
    def log_pipeline_start(
        self,
        pipeline_name: str,
        run_id: str,
        config: Optional[Dict] = None
    ):
        """Log pipeline start event."""
        self.info(
            f"Pipeline started: {pipeline_name}",
            extra={
                "event": "pipeline_start",
                "pipeline_name": pipeline_name,
                "run_id": run_id,
                "config": config
            }
        )
    
    def log_pipeline_end(
        self,
        pipeline_name: str,
        run_id: str,
        status: str,
        duration_seconds: float,
        rows_processed: int = 0
    ):
        """Log pipeline end event."""
        level = logging.INFO if status == "success" else logging.ERROR
        self._log(
            level,
            f"Pipeline completed: {pipeline_name} ({status})",
            extra={
                "event": "pipeline_end",
                "pipeline_name": pipeline_name,
                "run_id": run_id,
                "status": status,
                "duration_seconds": duration_seconds,
                "rows_processed": rows_processed
            }
        )
    
    def log_task_start(self, task_name: str, run_id: str):
        """Log task start event."""
        self.info(
            f"Task started: {task_name}",
            extra={
                "event": "task_start",
                "task_name": task_name,
                "run_id": run_id
            }
        )
    
    def log_task_end(
        self,
        task_name: str,
        run_id: str,
        status: str,
        duration_seconds: float
    ):
        """Log task end event."""
        level = logging.INFO if status == "success" else logging.ERROR
        self._log(
            level,
            f"Task completed: {task_name} ({status})",
            extra={
                "event": "task_end",
                "task_name": task_name,
                "run_id": run_id,
                "status": status,
                "duration_seconds": duration_seconds
            }
        )
    
    def log_quality_check(
        self,
        check_name: str,
        table_name: str,
        passed: bool,
        details: Optional[Dict] = None
    ):
        """Log quality check result."""
        level = logging.INFO if passed else logging.WARNING
        self._log(
            level,
            f"Quality check {'passed' if passed else 'failed'}: {check_name}",
            extra={
                "event": "quality_check",
                "check_name": check_name,
                "table_name": table_name,
                "passed": passed,
                "details": details
            }
        )
    
    def log_data_profile(
        self,
        table_name: str,
        row_count: int,
        column_count: int,
        completeness_score: float
    ):
        """Log data profile summary."""
        self.info(
            f"Data profile: {table_name}",
            extra={
                "event": "data_profile",
                "table_name": table_name,
                "row_count": row_count,
                "column_count": column_count,
                "completeness_score": completeness_score
            }
        )


def get_logger(name: str) -> StructuredLogger:
    """Get or create a structured logger."""
    return StructuredLogger(name)


def new_trace_id() -> str:
    """Generate a new trace ID."""
    return str(uuid.uuid4())[:8]

