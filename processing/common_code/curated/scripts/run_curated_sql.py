#!/usr/bin/env python3
"""
Curated Layer SQL Runner
=========================

Thin wrapper to execute Trino SQL scripts for the curated layer.
All transformation logic is in the SQL files - this just orchestrates execution.

Usage:
    python run_curated_sql.py --trino-host trino --trino-port 8080
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List

from trino.dbapi import connect
from trino.auth import BasicAuthentication

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CuratedSQLRunner:
    """Executes curated layer SQL scripts via Trino."""
    
    def __init__(self, trino_host: str = "trino", trino_port: int = 8080, trino_user: str = "airflow"):
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.trino_user = trino_user
        self.connection = None
    
    def connect(self):
        """Establish Trino connection."""
        logger.info(f"Connecting to Trino at {self.trino_host}:{self.trino_port}")
        self.connection = connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog="hive",
            schema="default"
        )
        logger.info("Trino connection established")
    
    def close(self):
        """Close Trino connection."""
        if self.connection:
            self.connection.close()
            logger.info("Trino connection closed")
    
    def execute_sql_file(self, sql_file: str, script_name: str) -> Dict:
        """Execute a single SQL file."""
        result = {
            "script": script_name,
            "file": sql_file,
            "status": "pending",
            "rows_affected": 0
        }
        
        try:
            logger.info(f"Executing: {script_name} ({sql_file})")
            
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolons to handle multiple statements
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            cursor = self.connection.cursor()
            total_rows = 0
            
            for i, stmt in enumerate(statements, 1):
                # Skip comments-only statements
                if stmt.startswith('--') and '\n' not in stmt:
                    continue
                
                logger.info(f"  Statement {i}/{len(statements)}")
                try:
                    cursor.execute(stmt)
                    # Fetch to complete the query
                    try:
                        rows = cursor.fetchall()
                        total_rows += len(rows) if rows else 0
                    except:
                        pass
                except Exception as stmt_error:
                    logger.warning(f"  Statement {i} warning: {stmt_error}")
            
            cursor.close()
            
            result["status"] = "success"
            result["rows_affected"] = total_rows
            logger.info(f"  Completed: {script_name}")
            
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            logger.error(f"  Failed: {script_name} - {e}")
        
        return result
    
    def run_all(self, config_path: str, sql_dir: str) -> List[Dict]:
        """Run all SQL scripts defined in the config."""
        results = []
        
        # Load config
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        logger.info("=" * 60)
        logger.info("CURATED LAYER - DWH LOAD VIA TRINO SQL")
        logger.info("=" * 60)
        
        self.connect()
        
        try:
            # Execute SQL scripts in order
            for script in sorted(config['sql_scripts'], key=lambda x: x['order']):
                sql_file = os.path.join(sql_dir, script['file'])
                
                if not os.path.exists(sql_file):
                    logger.warning(f"SQL file not found: {sql_file}")
                    results.append({
                        "script": script['name'],
                        "status": "skipped",
                        "reason": "file_not_found"
                    })
                    continue
                
                result = self.execute_sql_file(sql_file, script['name'])
                results.append(result)
                
                # Stop on failure (optional - could continue)
                if result["status"] == "failed":
                    logger.error(f"Stopping due to failure in {script['name']}")
                    break
        
        finally:
            self.close()
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("CURATED LOAD SUMMARY")
        logger.info("=" * 60)
        for r in results:
            status_icon = "✓" if r["status"] == "success" else "✗" if r["status"] == "failed" else "○"
            logger.info(f"  {status_icon} {r['script']}: {r['status']}")
        
        return results


def run_curated_load(
    trino_host: str = "trino",
    trino_port: int = 8080,
    config_dir: str = None,
    sql_dir: str = None
) -> List[Dict]:
    """Entry point for Airflow."""
    
    # Default paths
    base_dir = Path(__file__).parent.parent
    config_dir = config_dir or str(base_dir / "configs")
    sql_dir = sql_dir or str(base_dir / "sql")
    
    config_path = os.path.join(config_dir, "curated_job.json")
    
    runner = CuratedSQLRunner(trino_host, trino_port)
    return runner.run_all(config_path, sql_dir)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Curated Layer SQL Runner")
    parser.add_argument("--trino-host", default="localhost", help="Trino host")
    parser.add_argument("--trino-port", type=int, default=8080, help="Trino port")
    parser.add_argument("--config-dir", help="Config directory")
    parser.add_argument("--sql-dir", help="SQL scripts directory")
    
    args = parser.parse_args()
    
    results = run_curated_load(
        trino_host=args.trino_host,
        trino_port=args.trino_port,
        config_dir=args.config_dir,
        sql_dir=args.sql_dir
    )
    
    print(json.dumps(results, indent=2))

