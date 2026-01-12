#!/usr/bin/env python3
"""
Staging Job Runner
==================

Main entry point for running the staging (log compaction) job.
Reads job settings and processes all configured tables.
"""

import json
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.staging.scripts.log_compaction_tables import run_all_compactions, run_log_compaction

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/processing/staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)


def load_job_settings(settings_path: str = "jobs/job_settings.json") -> Dict:
    """Load job settings from JSON file."""
    with open(settings_path, 'r') as f:
        return json.load(f)


def run_staging_job(table_name: str = None):
    """
    Run the staging job for one or all tables.
    
    Args:
        table_name: Optional specific table to process
    """
    settings = load_job_settings()
    
    # Build MinIO config from job settings
    source_config = settings["connections"]["source"]
    minio_config = {
        "endpoint": source_config["endpoint"],
        "access_key": source_config["access_key"],
        "secret_key": source_config["secret_key"],
        "secure": False
    }
    
    staging_settings = settings["processing_stages"]["staging"]
    config_path = staging_settings["config_path"]
    
    job_start = datetime.now()
    
    logger.info("=" * 60)
    logger.info("PROPWISE STAGING JOB")
    logger.info("=" * 60)
    logger.info(f"Job Name: {settings['job_settings']['name']}")
    logger.info(f"Start Time: {job_start.isoformat()}")
    logger.info(f"Config Path: {config_path}")
    
    if table_name:
        # Process single table
        table_config = None
        for table in settings["tables"]:
            if table["name"] == table_name:
                table_config = table
                break
        
        if not table_config:
            logger.error(f"Table '{table_name}' not found in job settings")
            return {"status": "failed", "error": f"Table not found: {table_name}"}
        
        config_file = Path(config_path) / table_config["config"]
        logger.info(f"Processing single table: {table_name}")
        results = [run_log_compaction(str(config_file), minio_config)]
    else:
        # Process all enabled tables
        enabled_tables = [t for t in settings["tables"] if t.get("enabled", True)]
        logger.info(f"Processing {len(enabled_tables)} tables")
        
        results = run_all_compactions(config_path, minio_config)
    
    job_end = datetime.now()
    duration = (job_end - job_start).total_seconds()
    
    # Summary
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    total_rows = sum(r.get("rows_written", 0) for r in results)
    
    summary = {
        "job_name": settings['job_settings']['name'],
        "start_time": job_start.isoformat(),
        "end_time": job_end.isoformat(),
        "duration_seconds": duration,
        "tables_processed": len(results),
        "tables_success": success_count,
        "tables_failed": failed_count,
        "total_rows_written": total_rows,
        "results": results
    }
    
    # Save results
    os.makedirs("logs/processing", exist_ok=True)
    results_file = f"logs/processing/staging_results_{job_start.strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info("\n" + "=" * 60)
    logger.info("JOB COMPLETE")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"Tables: {success_count} success, {failed_count} failed")
    logger.info(f"Total Rows: {total_rows}")
    logger.info(f"Results saved to: {results_file}")
    logger.info("=" * 60)
    
    return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Staging Job")
    parser.add_argument(
        "--table",
        type=str,
        help="Specific table to process (optional, processes all if not specified)"
    )
    
    args = parser.parse_args()
    
    # Ensure log directory exists
    os.makedirs("logs/processing", exist_ok=True)
    
    result = run_staging_job(args.table)
    
    if result.get("tables_failed", 0) > 0:
        sys.exit(1)

