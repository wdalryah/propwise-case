#!/usr/bin/env python3
"""
Log Compaction Job (Landing Zone -> Staging)
=====================================

Reads raw data from MinIO landing zone, applies log compaction,
and writes deduplicated data to processed zone.

Core Logic:
1. Read raw CSV files from landing zone
2. Apply column mappings & type conversions
3. Filter out DELETE records (op != 'D')
4. Deduplicate: keep latest record per GPK (ordered by merge_cols)
5. Write to processed zone as Parquet with partitions
"""

import json
import logging
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LogCompaction:
    """Log compaction processor for MinIO data lake."""
    
    def __init__(self, config: Dict, minio_client: Minio):
        self.config = config
        self.minio = minio_client
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def run(self) -> Dict:
        """Execute log compaction pipeline."""
        
        table_name = self.config["landing_zone_table"]
        logger.info(f"Starting log compaction: {table_name}")
        
        # 1. Load raw data
        df = self._load_raw_data()
        if df.empty:
            logger.warning("No data found")
            return {"status": "empty", "rows": 0}
        
        rows_loaded = len(df)
        logger.info(f"  Loaded {rows_loaded} rows")
        
        # 2. Apply column mappings
        df = self._apply_mappings(df)
        
        # 3. Convert timestamps
        df = self._convert_timestamps(df)
        
        # 4. Filter deletes (op != 'D')
        if 'op' in df.columns:
            before = len(df)
            df = df[df['op'] != 'D'].copy()
            logger.info(f"  Filtered deletes: {before - len(df)} removed")
        
        # 5. Deduplicate (keep latest per GPK)
        df = self._deduplicate(df)
        
        # 6. Add partitions
        df = self._add_partitions(df)
        
        # 7. Add audit columns
        df['_batch_id'] = self.batch_id
        df['_processed_at'] = datetime.now().isoformat()
        
        # 8. Write to processed zone
        output_path = self._write_output(df)
        
        logger.info(f"✓ Complete: {len(df)} rows written to {output_path}")
        
        return {
            "status": "success",
            "table": table_name,
            "rows_loaded": rows_loaded,
            "rows_written": len(df),
            "output_path": output_path
        }
    
    def _load_raw_data(self) -> pd.DataFrame:
        """Load all CSV files from landing zone."""
        bucket = self.config["landing_zone_bucket"]
        prefix = self.config["landing_zone_path"]
        
        dfs = []
        objects = self.minio.list_objects(bucket, prefix=prefix, recursive=True)
        
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                response = self.minio.get_object(bucket, obj.object_name)
                df = pd.read_csv(io.BytesIO(response.read()), dtype=str)
                response.close()
                response.release_conn()
                dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    def _apply_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply column mappings based on schema_evolution mode:
        
        - strict: Only mapped columns, error on unmapped (legacy behavior)
        - flexible: Apply mappings + KEEP unmapped columns as-is (default)
        - pass_through: No mappings, keep all columns as-is
        
        Also handles:
        - Missing columns (in mapping but not in source) → based on missing_columns setting
        - Unmapped columns (in source but not in mapping) → based on unmapped_columns setting
        """
        schema_config = self.config.get("schema_evolution", {})
        mode = schema_config.get("mode", "flexible")  # Default to flexible
        unmapped_handling = schema_config.get("unmapped_columns", "keep")
        missing_handling = schema_config.get("missing_columns", "null")  # error | warn | null | skip
        log_drift = schema_config.get("log_schema_drift", True)
        
        mapping = self.config.get("mapping", {})
        
        # Get mapped input columns and build rename map
        mapped_input_cols = set()
        rename_map = {}
        missing_cols = []
        
        for col_config in mapping.values():
            input_col = col_config["input_column"]
            output_col = col_config["output_column"]
            mapped_input_cols.add(input_col)
            
            if input_col in df.columns:
                rename_map[input_col] = output_col
            else:
                missing_cols.append({"input": input_col, "output": output_col, "config": col_config})
        
        # === Handle MISSING columns (Schema Contraction) ===
        if missing_cols:
            missing_names = [m["input"] for m in missing_cols]
            
            if log_drift:
                logger.warning(f"  ⚠️  Schema contraction: {len(missing_cols)} columns missing from source")
                logger.warning(f"     Missing: {missing_names[:10]}{'...' if len(missing_names) > 10 else ''}")
            
            if missing_handling == "error":
                raise ValueError(f"Missing required columns: {missing_names}")
            
            elif missing_handling == "null":
                # Add missing columns with NULL values
                logger.info(f"     Action: Adding {len(missing_cols)} columns with NULL values")
                for m in missing_cols:
                    output_col = m["output"]
                    df[output_col] = None
                    # Don't add to rename_map since we created the output column directly
            
            elif missing_handling == "warn":
                logger.warning(f"     Action: Skipping missing columns (warn mode)")
                # Continue without these columns
            
            elif missing_handling == "skip":
                # Silently skip
                pass
        
        # === Handle UNMAPPED columns (Schema Expansion) ===
        unmapped_cols = [c for c in df.columns if c not in mapped_input_cols]
        
        if unmapped_cols and log_drift:
            logger.info(f"  📋 Schema expansion: {len(unmapped_cols)} new/unmapped columns")
            logger.info(f"     Unmapped: {unmapped_cols[:10]}{'...' if len(unmapped_cols) > 10 else ''}")
        
        # === Apply mode-specific logic ===
        if mode == "pass_through":
            # No transformations, keep all columns as-is
            logger.info("  Mode: pass_through - keeping all columns as-is")
            return df
        
        elif mode == "strict":
            # Only keep mapped columns, drop unmapped
            if unmapped_cols:
                logger.warning(f"  Mode: strict - dropping {len(unmapped_cols)} unmapped columns")
            df = df.rename(columns=rename_map)
            
            # Only keep columns that were successfully mapped (output names)
            expected_outputs = [m["output"] for m in mapping.values()]
            existing_output_cols = [c for c in expected_outputs if c in df.columns]
            return df[existing_output_cols]
        
        else:  # flexible (default)
            # Apply mappings AND keep unmapped columns
            logger.info(f"  Mode: flexible - {len(rename_map)} mapped + {len(unmapped_cols)} unmapped columns")
            df = df.rename(columns=rename_map)
            # All columns are kept (both mapped and unmapped)
            return df
    
    def _convert_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert timestamp columns to datetime."""
        ts_cols = self.config.get("timestamp_cols", {})
        
        for ts_config in ts_cols.values():
            col = ts_config["col"]
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep latest record per GPK using merge_cols for ordering."""
        if 'gpk' not in df.columns:
            logger.warning("No GPK column, skipping deduplication")
            return df
        
        # Get merge columns for ordering
        merge_cols = list(self.config.get("merge_cols", {}).values())
        
        # Map to output column names
        sort_cols = []
        for col in merge_cols:
            for map_config in self.config.get("mapping", {}).values():
                if map_config["input_column"] == col:
                    sort_cols.append(map_config["output_column"])
                    break
        
        if not sort_cols:
            sort_cols = ['extraction_time']
        
        # Filter to existing columns
        sort_cols = [c for c in sort_cols if c in df.columns]
        
        before = len(df)
        
        # Sort descending (latest first) and keep first per GPK
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=False, na_position='last')
        
        df = df.drop_duplicates(subset=['gpk'], keep='first')
        
        logger.info(f"  Deduplicated: {before} -> {len(df)} ({before - len(df)} removed)")
        
        return df
    
    def _clear_target(self, bucket: str, prefix: str):
        """Clear existing data in target path (for idempotency)."""
        try:
            objects = list(self.minio.list_objects(bucket, prefix=prefix, recursive=True))
            if objects:
                logger.info(f"  Clearing {len(objects)} existing files...")
                for obj in objects:
                    self.minio.remove_object(bucket, obj.object_name)
        except Exception as e:
            logger.warning(f"  Could not clear target: {e}")
    
    def _add_partitions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add year/month partition columns."""
        partitions = self.config.get("partitions", {})
        
        for part_config in partitions.values():
            col = part_config["col"]
            name = part_config["name"]
            
            if col not in df.columns:
                continue
            
            # Ensure datetime
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            if name == "year":
                df[name] = df[col].dt.year.fillna(0).astype(int)
            elif name == "month":
                df[name] = df[col].dt.month.fillna(0).astype(int)
            elif name == "day":
                df[name] = df[col].dt.day.fillna(0).astype(int)
        
        return df
    
    def _write_output(self, df: pd.DataFrame) -> str:
        """Write to processed zone as partitioned Parquet (overwrite mode)."""
        bucket = self.config["target_bucket"]
        target_path = self.config["target_path"]
        
        # Clear existing data (idempotency - overwrite mode)
        self._clear_target(bucket, target_path)
        
        # Get partition columns
        partitions = self.config.get("partitions", {})
        partition_cols = [p["name"] for p in partitions.values()]
        
        if partition_cols and all(col in df.columns for col in partition_cols):
            # Write partitioned data
            files_written = 0
            for partition_vals, group_df in df.groupby(partition_cols):
                if not isinstance(partition_vals, tuple):
                    partition_vals = (partition_vals,)
                
                # Build partition path: year=2023/month=5/
                partition_parts = [f"{col}={val}" for col, val in zip(partition_cols, partition_vals)]
                partition_path = "/".join(partition_parts)
                
                file_path = f"{target_path}/{partition_path}/data_{self.batch_id}.parquet"
                
                buffer = io.BytesIO()
                group_df.to_parquet(buffer, index=False, engine='pyarrow')
                buffer.seek(0)
                
                self.minio.put_object(
                    bucket_name=bucket,
                    object_name=file_path,
                    data=buffer,
                    length=buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                files_written += 1
            
            logger.info(f"  Written {files_written} partition files")
            return f"minio://{bucket}/{target_path}/"
        else:
            # Write single file (no partitions)
            file_path = f"{target_path}/{self.batch_id}/data.parquet"
            
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            self.minio.put_object(
                bucket_name=bucket,
                object_name=file_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            
            return f"minio://{bucket}/{file_path}"


def get_minio_client(endpoint: str = "localhost:9000") -> Minio:
    """Create MinIO client."""
    return Minio(
        endpoint=endpoint,
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )


def run_compaction(config_path: str, minio_endpoint: str = "localhost:9000") -> Dict:
    """Run log compaction for a single table."""
    with open(config_path) as f:
        config = json.load(f)
    
    minio = get_minio_client(minio_endpoint)
    processor = LogCompaction(config, minio)
    return processor.run()


def run_all(config_dir: str = "processing/common_code/staging/configs", 
            minio_endpoint: str = "localhost:9000") -> List[Dict]:
    """Run log compaction for all tables in config directory."""
    
    results = []
    config_files = sorted(Path(config_dir).glob("*.json"))
    
    logger.info(f"Processing {len(config_files)} tables...")
    
    for config_file in config_files:
        logger.info(f"\n{'='*50}")
        result = run_compaction(str(config_file), minio_endpoint)
        results.append(result)
    
    # Summary
    success = sum(1 for r in results if r.get("status") == "success")
    total_rows = sum(r.get("rows_written", 0) for r in results)
    
    logger.info(f"\n{'='*50}")
    logger.info(f"COMPLETE: {success}/{len(results)} tables, {total_rows} total rows")
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Log Compaction Job")
    parser.add_argument("--config", help="Single config file path")
    parser.add_argument("--config-dir", default="processing/common_code/staging/configs")
    parser.add_argument("--minio", default="localhost:9000", help="MinIO endpoint")
    
    args = parser.parse_args()
    
    if args.config:
        result = run_compaction(args.config, args.minio)
        print(json.dumps(result, indent=2, default=str))
    else:
        results = run_all(args.config_dir, args.minio)
        print(json.dumps(results, indent=2, default=str))
