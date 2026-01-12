"""
Common Utilities
================

Shared utility functions for data processing.
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, Optional
import pandas as pd
from minio import Minio


def get_minio_client(config: Dict = None) -> Minio:
    """
    Create and return a MinIO client.
    
    Args:
        config: Optional config dict with endpoint, access_key, secret_key
        
    Returns:
        Minio client instance
    """
    if config is None:
        config = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin123",
            "secure": False
        }
    
    return Minio(
        endpoint=config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config.get("secure", False)
    )


def read_config(config_path: str) -> Dict:
    """
    Read JSON configuration file.
    
    Args:
        config_path: Path to config file
        
    Returns:
        Config dictionary
    """
    with open(config_path, 'r') as f:
        return json.load(f)


def parse_timestamp(value: Any, format: str = None) -> Optional[datetime]:
    """
    Parse timestamp from various formats.
    
    Args:
        value: Value to parse
        format: Expected format string (optional)
        
    Returns:
        Parsed datetime or None
    """
    if pd.isna(value) or value == '' or value is None:
        return None
    
    try:
        if format:
            return datetime.strptime(str(value), format)
        else:
            # Try common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y",
                "%d/%m/%Y %H:%M",
                "%d/%m/%Y"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(str(value), fmt)
                except ValueError:
                    continue
            return None
    except Exception:
        return None


def parse_boolean(value: Any) -> Optional[bool]:
    """
    Parse boolean from various representations.
    
    Args:
        value: Value to parse
        
    Returns:
        Boolean or None
    """
    if pd.isna(value) or value == '' or value is None:
        return None
    
    value_str = str(value).upper().strip()
    
    if value_str in ('TRUE', '1', 'YES', 'Y', 'T'):
        return True
    elif value_str in ('FALSE', '0', 'NO', 'N', 'F'):
        return False
    return None


def clean_string(value: Any) -> Optional[str]:
    """
    Clean and normalize a string value.
    
    Args:
        value: Value to clean
        
    Returns:
        Cleaned string or None
    """
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string
    value_str = str(value).strip()
    
    if value_str == '':
        return None
    
    # Normalize whitespace
    value_str = re.sub(r'\s+', ' ', value_str)
    
    return value_str


def normalize_status(status: str) -> str:
    """
    Normalize status values to standard format.
    
    Args:
        status: Raw status string
        
    Returns:
        Normalized status
    """
    if pd.isna(status) or status is None:
        return "UNKNOWN"
    
    status = str(status).strip().upper()
    
    # Map common variations
    status_map = {
        "NEW": "NEW",
        "CONTACTED": "CONTACTED",
        "QUALIFIED": "QUALIFIED",
        "CONVERTED": "CONVERTED",
        "LOST": "LOST",
        "NOT INTERESTED": "NOT_INTERESTED",
        "NOT_INTERESTED": "NOT_INTERESTED",
        "SWITCHED OFF": "UNREACHABLE",
        "NO ANSWER": "UNREACHABLE",
        "WRONG NUMBER": "UNREACHABLE",
        "DELETED": "DELETED"
    }
    
    return status_map.get(status, status.replace(" ", "_"))


def parse_numeric(value: Any) -> Optional[float]:
    """
    Parse numeric value from string.
    
    Args:
        value: Value to parse
        
    Returns:
        Float or None
    """
    if pd.isna(value) or value == '' or value is None:
        return None
    
    try:
        # Remove common formatting
        value_str = str(value).replace(',', '').replace('$', '').replace(' ', '')
        return float(value_str)
    except (ValueError, TypeError):
        return None


def generate_batch_id() -> str:
    """
    Generate a unique batch ID based on current timestamp.
    
    Returns:
        Batch ID string
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_partition_values(dt: datetime) -> Dict:
    """
    Extract partition values from a datetime.
    
    Args:
        dt: Datetime object
        
    Returns:
        Dict with year, month, day values
    """
    if dt is None:
        return {"year": 0, "month": 0, "day": 0}
    
    return {
        "year": dt.year,
        "month": dt.month,
        "day": dt.day
    }

