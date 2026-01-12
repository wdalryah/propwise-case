"""
Great Expectations Data Profiler
================================

Profiles data using Great Expectations and persists results
to the PostgreSQL metadata store.

Features:
- Column-level statistics (null rates, distinct counts, min/max)
- Automated expectation generation
- Profile comparison for drift detection
- PostgreSQL persistence
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from io import BytesIO

import pandas as pd
import numpy as np

try:
    import great_expectations as gx
    from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False

from minio import Minio
import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Profiles data and generates statistics using Great Expectations.
    
    Features:
    - Automatic profiling of DataFrames
    - Column-level statistics
    - Profile persistence to PostgreSQL
    - Drift detection between profiles
    """
    
    def __init__(
        self,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        postgres_config: Optional[Dict] = None
    ):
        """Initialize profiler."""
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
    
    # =========================================
    # DATA LOADING
    # =========================================
    
    def load_from_minio(
        self,
        bucket: str,
        prefix: str
    ) -> pd.DataFrame:
        """Load data from MinIO."""
        objects = list(self.minio_client.list_objects(bucket, prefix=prefix, recursive=True))
        
        dfs = []
        for obj in objects:
            if obj.is_dir:
                continue
            
            obj_name = obj.object_name
            if not (obj_name.endswith('.csv') or obj_name.endswith('.parquet')):
                continue
            
            try:
                response = self.minio_client.get_object(bucket, obj_name)
                data = response.read()
                response.close()
                response.release_conn()
                
                if obj_name.endswith('.parquet'):
                    df = pd.read_parquet(BytesIO(data))
                else:
                    df = pd.read_csv(BytesIO(data))
                
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to load {obj_name}: {e}")
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    # =========================================
    # PROFILING
    # =========================================
    
    def profile_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Dict:
        """
        Profile a DataFrame and return statistics.
        
        Returns:
            Dict with table and column-level statistics
        """
        start_time = datetime.now()
        
        profile = {
            "table_name": table_name,
            "profile_date": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
            "summary": {}
        }
        
        # Profile each column
        for col in df.columns:
            profile["columns"][col] = self._profile_column(df, col)
        
        # Generate summary
        profile["summary"] = self._generate_summary(df, profile["columns"])
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        profile["profiling_duration_seconds"] = round(duration, 2)
        
        logger.info(f"Profiled {table_name}: {len(df)} rows, {len(df.columns)} columns in {duration:.2f}s")
        
        return profile
    
    def _profile_column(self, df: pd.DataFrame, column: str) -> Dict:
        """Profile a single column."""
        col_data = df[column]
        total = len(col_data)
        
        profile = {
            "column_name": column,
            "data_type": str(col_data.dtype),
            "total_count": total
        }
        
        # Completeness
        null_count = int(col_data.isna().sum())
        profile["null_count"] = null_count
        profile["null_percentage"] = round(null_count / total * 100, 4) if total > 0 else 0
        profile["non_null_count"] = total - null_count
        profile["has_nulls"] = null_count > 0
        
        # Uniqueness
        non_null = col_data.dropna()
        distinct_count = int(non_null.nunique())
        profile["distinct_count"] = distinct_count
        profile["distinct_percentage"] = round(distinct_count / len(non_null) * 100, 4) if len(non_null) > 0 else 0
        
        duplicates = int(non_null.duplicated().sum())
        profile["duplicate_count"] = duplicates
        profile["has_duplicates"] = duplicates > 0
        
        # Type-specific profiling
        if pd.api.types.is_numeric_dtype(col_data):
            profile.update(self._profile_numeric(col_data))
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            profile.update(self._profile_datetime(col_data))
        else:
            profile.update(self._profile_string(col_data))
        
        # Top values
        profile["top_values"] = self._get_top_values(non_null)
        
        return profile
    
    def _profile_numeric(self, col_data: pd.Series) -> Dict:
        """Profile numeric column."""
        try:
            numeric = pd.to_numeric(col_data, errors='coerce').dropna()
        except:
            return {}
        
        if len(numeric) == 0:
            return {}
        
        profile = {
            "min_value": float(numeric.min()),
            "max_value": float(numeric.max()),
            "mean_value": float(numeric.mean()),
            "std_dev": float(numeric.std()) if len(numeric) > 1 else 0,
            "median_value": float(numeric.median()),
            "sum_value": float(numeric.sum()),
            "zero_count": int((numeric == 0).sum()),
            "negative_count": int((numeric < 0).sum()),
            "positive_count": int((numeric > 0).sum())
        }
        
        # Outlier detection (IQR)
        q1 = numeric.quantile(0.25)
        q3 = numeric.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = int(((numeric < lower_bound) | (numeric > upper_bound)).sum())
        
        profile["outlier_count"] = outliers
        profile["has_outliers"] = outliers > 0
        
        return profile
    
    def _profile_datetime(self, col_data: pd.Series) -> Dict:
        """Profile datetime column."""
        non_null = col_data.dropna()
        
        if len(non_null) == 0:
            return {}
        
        return {
            "min_date": str(non_null.min()),
            "max_date": str(non_null.max()),
            "date_range_days": (non_null.max() - non_null.min()).days if len(non_null) > 0 else 0
        }
    
    def _profile_string(self, col_data: pd.Series) -> Dict:
        """Profile string column."""
        non_null = col_data.dropna().astype(str)
        
        if len(non_null) == 0:
            return {}
        
        lengths = non_null.str.len()
        
        profile = {
            "min_length": int(lengths.min()),
            "max_length": int(lengths.max()),
            "avg_length": round(float(lengths.mean()), 2),
            "empty_string_count": int((non_null == "").sum())
        }
        
        # Check for JSON-like values
        json_count = 0
        for val in non_null.head(1000):
            val_str = str(val).strip()
            if (val_str.startswith('{') and val_str.endswith('}')) or \
               (val_str.startswith('[') and val_str.endswith(']')):
                json_count += 1
        profile["json_like_count"] = json_count
        
        return profile
    
    def _get_top_values(self, col_data: pd.Series, n: int = 10) -> List[Dict]:
        """Get top N most frequent values."""
        if len(col_data) == 0:
            return []
        
        value_counts = col_data.value_counts().head(n)
        total = len(col_data)
        
        return [
            {
                "value": str(val)[:100],
                "count": int(count),
                "percentage": round(count / total * 100, 2)
            }
            for val, count in value_counts.items()
        ]
    
    def _generate_summary(self, df: pd.DataFrame, columns: Dict) -> Dict:
        """Generate summary statistics."""
        total_cells = len(df) * len(df.columns)
        total_nulls = sum(c.get("null_count", 0) for c in columns.values())
        
        return {
            "total_cells": total_cells,
            "total_null_cells": total_nulls,
            "overall_null_percentage": round(total_nulls / total_cells * 100, 4) if total_cells > 0 else 0,
            "columns_with_nulls": sum(1 for c in columns.values() if c.get("has_nulls")),
            "columns_with_duplicates": sum(1 for c in columns.values() if c.get("has_duplicates")),
            "columns_with_outliers": sum(1 for c in columns.values() if c.get("has_outliers", False)),
            "completeness_score": round((1 - total_nulls / total_cells) * 100, 2) if total_cells > 0 else 100
        }
    
    # =========================================
    # PERSISTENCE
    # =========================================
    
    def save_profile(self, profile: Dict) -> int:
        """Save profile to PostgreSQL."""
        try:
            with self.db_conn.cursor() as cur:
                # Save table profile
                cur.execute("""
                    INSERT INTO profiling.table_profiles
                    (table_name, profile_date, row_count, column_count, 
                     profiling_duration_seconds, profile_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (table_name, profile_date) 
                    DO UPDATE SET 
                        row_count = EXCLUDED.row_count,
                        column_count = EXCLUDED.column_count,
                        profiling_duration_seconds = EXCLUDED.profiling_duration_seconds,
                        profile_metadata = EXCLUDED.profile_metadata
                    RETURNING profile_id
                """, (
                    profile["table_name"],
                    datetime.now().date(),
                    profile["row_count"],
                    profile["column_count"],
                    profile.get("profiling_duration_seconds"),
                    Json(profile["summary"])
                ))
                
                profile_id = cur.fetchone()[0]
                
                # Save column profiles
                for col_name, col_profile in profile["columns"].items():
                    cur.execute("""
                        INSERT INTO profiling.column_profiles
                        (profile_id, column_name, data_type, null_count, null_percentage,
                         non_null_count, distinct_count, distinct_percentage, duplicate_count,
                         min_value, max_value, mean_value, std_dev, median_value,
                         min_length, max_length, avg_length, top_values,
                         has_nulls, has_duplicates, has_outliers)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        profile_id,
                        col_name,
                        col_profile.get("data_type"),
                        col_profile.get("null_count"),
                        col_profile.get("null_percentage"),
                        col_profile.get("non_null_count"),
                        col_profile.get("distinct_count"),
                        col_profile.get("distinct_percentage"),
                        col_profile.get("duplicate_count"),
                        col_profile.get("min_value"),
                        col_profile.get("max_value"),
                        col_profile.get("mean_value"),
                        col_profile.get("std_dev"),
                        col_profile.get("median_value"),
                        col_profile.get("min_length"),
                        col_profile.get("max_length"),
                        col_profile.get("avg_length"),
                        Json(col_profile.get("top_values")) if col_profile.get("top_values") else None,
                        col_profile.get("has_nulls"),
                        col_profile.get("has_duplicates"),
                        col_profile.get("has_outliers", False)
                    ))
                
                self.db_conn.commit()
                logger.info(f"Profile saved with ID {profile_id}")
                return profile_id
                
        except Exception as e:
            logger.error(f"Failed to save profile: {e}")
            self.db_conn.rollback()
            return 0
    
    def get_latest_profile(self, table_name: str) -> Optional[Dict]:
        """Get the most recent profile for a table."""
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM profiling.table_profiles
                    WHERE table_name = %s
                    ORDER BY profile_date DESC
                    LIMIT 1
                """, (table_name,))
                return dict(cur.fetchone()) if cur.fetchone() else None
        except Exception as e:
            logger.error(f"Failed to get profile: {e}")
            return None
    
    # =========================================
    # CONVENIENCE METHODS
    # =========================================
    
    def profile_raw_table(self, table_name: str, persist: bool = True) -> Dict:
        """Profile a raw table from MinIO."""
        df = self.load_from_minio("raw-data", f"raw_{table_name}/")
        
        if df.empty:
            return {"error": f"No data found for {table_name}"}
        
        profile = self.profile_dataframe(df, table_name)
        
        if persist:
            profile["profile_id"] = self.save_profile(profile)
        
        return profile
    
    def profile_staging_table(self, table_name: str, persist: bool = True) -> Dict:
        """Profile a staging table from MinIO."""
        df = self.load_from_minio("staging", f"processed_{table_name}/")
        
        if df.empty:
            return {"error": f"No staging data found for {table_name}"}
        
        profile = self.profile_dataframe(df, f"{table_name}_staging")
        
        if persist:
            profile["profile_id"] = self.save_profile(profile)
        
        return profile
    
    def profile_all(self, persist: bool = True) -> Dict:
        """Profile all tables."""
        results = {
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
        
        for table in ["leads", "sales"]:
            # Raw
            raw_profile = self.profile_raw_table(table, persist)
            results["tables"][f"{table}_raw"] = {
                "row_count": raw_profile.get("row_count", 0),
                "completeness": raw_profile.get("summary", {}).get("completeness_score", 0)
            }
            
            # Staging
            staging_profile = self.profile_staging_table(table, persist)
            results["tables"][f"{table}_staging"] = {
                "row_count": staging_profile.get("row_count", 0),
                "completeness": staging_profile.get("summary", {}).get("completeness_score", 0)
            }
        
        return results


# =========================================
# AIRFLOW TASK FUNCTIONS
# =========================================

def profile_raw_data(**context) -> Dict:
    """Airflow task to profile raw data."""
    profiler = DataProfiler(minio_endpoint='minio:9000')
    
    results = {}
    for table in ["leads", "sales"]:
        profile = profiler.profile_raw_table(table, persist=True)
        results[table] = {
            "row_count": profile.get("row_count", 0),
            "column_count": profile.get("column_count", 0),
            "completeness": profile.get("summary", {}).get("completeness_score", 0)
        }
        print(f"Profiled {table}: {results[table]}")
    
    profiler.close()
    context['ti'].xcom_push(key='raw_profiles', value=results)
    return results


def profile_staging_data(**context) -> Dict:
    """Airflow task to profile staging data."""
    profiler = DataProfiler(minio_endpoint='minio:9000')
    
    results = {}
    for table in ["leads", "sales"]:
        profile = profiler.profile_staging_table(table, persist=True)
        results[table] = {
            "row_count": profile.get("row_count", 0),
            "column_count": profile.get("column_count", 0),
            "completeness": profile.get("summary", {}).get("completeness_score", 0)
        }
        print(f"Profiled staging {table}: {results[table]}")
    
    profiler.close()
    context['ti'].xcom_push(key='staging_profiles', value=results)
    return results


# =========================================
# CLI
# =========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Profile data")
    parser.add_argument("--table", choices=["leads", "sales", "all"], default="all")
    parser.add_argument("--stage", choices=["raw", "staging", "all"], default="all")
    parser.add_argument("--no-persist", action="store_true")
    
    args = parser.parse_args()
    
    profiler = DataProfiler()
    
    tables = ["leads", "sales"] if args.table == "all" else [args.table]
    stages = ["raw", "staging"] if args.stage == "all" else [args.stage]
    
    for table in tables:
        for stage in stages:
            if stage == "raw":
                profile = profiler.profile_raw_table(table, persist=not args.no_persist)
            else:
                profile = profiler.profile_staging_table(table, persist=not args.no_persist)
            
            print(f"\n{'='*60}")
            print(f"Profile: {table} ({stage})")
            print(f"{'='*60}")
            print(f"Rows: {profile.get('row_count', 0)}")
            print(f"Columns: {profile.get('column_count', 0)}")
            print(f"Completeness: {profile.get('summary', {}).get('completeness_score', 0)}%")
    
    profiler.close()

