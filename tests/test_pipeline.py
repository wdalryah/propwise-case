#!/usr/bin/env python3
"""
Propwise Pipeline Test Suite
============================
Comprehensive tests for all pipeline components.

Author: Mohamed Alryah - abdalrhman.alryah@linux.com

Usage:
    python tests/test_pipeline.py
    python tests/test_pipeline.py --verbose
    python tests/test_pipeline.py --test ingestion
"""

import os
import sys
import json
import time
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

# Configuration
CONFIG = {
    "minio": {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin123"
    },
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root123",
        "database": "propwise_source"
    },
    "postgres_dwh": {
        "host": "localhost",
        "port": 5432,
        "user": "dwh_user",
        "password": "dwh123",
        "database": "propwise_dwh"
    },
    "trino": {
        "host": "localhost",
        "port": 8090,
        "user": "trino"
    },
    "airflow": {
        "host": "localhost",
        "port": 8085,
        "user": "admin",
        "password": "admin123"
    }
}

# Test results storage
RESULTS = {
    "tests": [],
    "passed": 0,
    "failed": 0,
    "warnings": 0,
    "start_time": None,
    "end_time": None
}

VERBOSE = False


def log(message, level="INFO"):
    """Print log message."""
    colors = {
        "INFO": "\033[0m",
        "OK": "\033[92m",
        "WARN": "\033[93m",
        "ERROR": "\033[91m",
        "HEADER": "\033[94m"
    }
    color = colors.get(level, "\033[0m")
    reset = "\033[0m"
    
    if level == "HEADER":
        print(f"\n{'='*60}")
        print(f"{color}  {message}{reset}")
        print(f"{'='*60}")
    else:
        timestamp = datetime.now().strftime("%H:%M:%S")
        symbol = {"OK": "✓", "ERROR": "✗", "WARN": "⚠"}.get(level, "•")
        print(f"[{timestamp}] {color}{symbol} {message}{reset}")


def record_result(test_name, passed, message="", details=None):
    """Record test result."""
    result = {
        "test": test_name,
        "passed": passed,
        "message": message,
        "details": details,
        "timestamp": datetime.now().isoformat()
    }
    RESULTS["tests"].append(result)
    
    if passed:
        RESULTS["passed"] += 1
        log(f"{test_name}: {message}", "OK")
    else:
        RESULTS["failed"] += 1
        log(f"{test_name}: {message}", "ERROR")
    
    if VERBOSE and details:
        print(f"    Details: {details}")


# ============================================================
# TEST FUNCTIONS
# ============================================================

def test_docker_services():
    """Test that all Docker services are running."""
    log("Docker Services", "HEADER")
    
    services = [
        ("propwise-minio", "MinIO"),
        ("propwise-mysql-source", "MySQL"),
        ("propwise-trino", "Trino"),
        ("propwise-postgres-dwh", "PostgreSQL DWH"),
        ("propwise-airflow-webserver", "Airflow")
    ]
    
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True, text=True, cwd=PROJECT_ROOT
        )
        
        running = result.stdout.lower()
        
        for container, name in services:
            if container.lower() in running and "running" in running:
                record_result(f"docker_{name.lower()}", True, f"{name} is running")
            else:
                record_result(f"docker_{name.lower()}", False, f"{name} is NOT running")
                
    except Exception as e:
        record_result("docker_check", False, f"Docker check failed: {e}")


def test_minio_connectivity():
    """Test MinIO connectivity and buckets."""
    log("MinIO Connectivity", "HEADER")
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG["minio"]["endpoint"],
            CONFIG["minio"]["access_key"],
            CONFIG["minio"]["secret_key"],
            secure=False
        )
        
        buckets = [b.name for b in client.list_buckets()]
        record_result("minio_connection", True, f"Connected, {len(buckets)} buckets")
        
        required = ["raw-data", "staging", "rejected", "logs"]
        for bucket in required:
            if bucket in buckets:
                record_result(f"minio_bucket_{bucket}", True, f"Bucket '{bucket}' exists")
            else:
                record_result(f"minio_bucket_{bucket}", False, f"Bucket '{bucket}' MISSING")
                
    except Exception as e:
        record_result("minio_connection", False, f"Connection failed: {e}")


def test_mysql_source():
    """Test MySQL source database."""
    log("MySQL Source", "HEADER")
    
    try:
        import pymysql
        
        conn = pymysql.connect(
            host=CONFIG["mysql"]["host"],
            port=CONFIG["mysql"]["port"],
            user=CONFIG["mysql"]["user"],
            password=CONFIG["mysql"]["password"],
            database=CONFIG["mysql"]["database"]
        )
        cursor = conn.cursor()
        
        record_result("mysql_connection", True, "Connected to MySQL")
        
        # Check tables
        for table in ["leads", "sales"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            if count > 0:
                record_result(f"mysql_{table}", True, f"{count} records")
            else:
                record_result(f"mysql_{table}", False, "Table is empty")
        
        conn.close()
        
    except Exception as e:
        record_result("mysql_connection", False, f"Connection failed: {e}")


def test_full_load_ingestion():
    """Test full load ingestion."""
    log("Full Load Ingestion", "HEADER")
    
    try:
        from ingestion.run_ingestion import run_full_load
        
        log("Running full load...", "INFO")
        result = run_full_load()
        
        record_result("full_load", True, "Full load completed")
        
        # Check MinIO for data
        from minio import Minio
        client = Minio(
            CONFIG["minio"]["endpoint"],
            CONFIG["minio"]["access_key"],
            CONFIG["minio"]["secret_key"],
            secure=False
        )
        
        raw_files = list(client.list_objects("raw-data", prefix="propwise/", recursive=True))
        csv_files = [f for f in raw_files if f.object_name.endswith('.csv')]
        
        if len(csv_files) > 0:
            record_result("full_load_data", True, f"{len(csv_files)} CSV files in raw-data")
        else:
            record_result("full_load_data", False, "No CSV files found in raw-data")
            
    except Exception as e:
        record_result("full_load", False, f"Full load failed: {e}")


def test_cdc_ingestion():
    """Test CDC ingestion."""
    log("CDC Ingestion", "HEADER")
    
    try:
        import pymysql
        
        # Insert test record
        test_id = f"CDC_TEST_{datetime.now().strftime('%H%M%S')}"
        ts = datetime.now().strftime("%m/%d/%Y %H:%M")
        
        conn = pymysql.connect(
            host=CONFIG["mysql"]["host"],
            port=CONFIG["mysql"]["port"],
            user=CONFIG["mysql"]["user"],
            password=CONFIG["mysql"]["password"],
            database=CONFIG["mysql"]["database"]
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO leads (
                id, date_of_last_request, buyer, seller, budget,
                created_at, updated_at, user_id, location, status_name,
                lead_source, campaign, lead_type
            ) VALUES (%s, %s, 'TRUE', 'FALSE', 500000, %s, %s, 9999,
                'CDC Test', 'New', 'cdc_test', 'test', 'Primary')
        """, (test_id, ts, ts, ts))
        conn.commit()
        conn.close()
        
        record_result("cdc_insert", True, f"Inserted test record: {test_id}")
        
        # Run CDC
        from ingestion.run_ingestion import run_cdc
        result = run_cdc()
        
        record_result("cdc_run", True, "CDC completed")
        
    except Exception as e:
        record_result("cdc", False, f"CDC failed: {e}")


def test_schema_contract():
    """Test soft schema contract."""
    log("Schema Contract", "HEADER")
    
    try:
        config_path = PROJECT_ROOT / "processing/common_code/staging/configs/01_leads.json"
        
        with open(config_path) as f:
            config = json.load(f)
        
        schema_config = config.get("schema_evolution", {})
        mode = schema_config.get("mode", "not set")
        unmapped = schema_config.get("unmapped_columns", "not set")
        missing = schema_config.get("missing_columns", "not set")
        
        record_result("schema_config", True, 
            f"Mode: {mode}, Unmapped: {unmapped}, Missing: {missing}")
        
        if mode == "flexible":
            record_result("schema_mode", True, "Soft contract (flexible) enabled")
        else:
            record_result("schema_mode", False, f"Unexpected mode: {mode}")
            
    except Exception as e:
        record_result("schema_contract", False, f"Schema check failed: {e}")


def test_staging_data():
    """Test staging data quality."""
    log("Staging Data Quality", "HEADER")
    
    try:
        from minio import Minio
        import pandas as pd
        from io import BytesIO
        
        client = Minio(
            CONFIG["minio"]["endpoint"],
            CONFIG["minio"]["access_key"],
            CONFIG["minio"]["secret_key"],
            secure=False
        )
        
        staging_files = list(client.list_objects("staging", recursive=True))
        parquet_files = [f for f in staging_files if f.object_name.endswith('.parquet')]
        
        if len(parquet_files) > 0:
            record_result("staging_files", True, f"{len(parquet_files)} parquet files")
            
            # Check quality of first file
            obj = client.get_object("staging", parquet_files[0].object_name)
            df = pd.read_parquet(BytesIO(obj.read()))
            
            row_count = len(df)
            null_rate = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
            dup_rate = (len(df) - len(df.drop_duplicates())) / len(df) * 100 if len(df) > 0 else 0
            
            record_result("staging_quality", True, 
                f"Rows: {row_count}, Null: {null_rate:.1f}%, Dup: {dup_rate:.1f}%")
        else:
            record_result("staging_files", False, "No parquet files in staging")
            RESULTS["warnings"] += 1
            
    except Exception as e:
        record_result("staging_data", False, f"Staging check failed: {e}")


def test_dwh():
    """Test DWH schema and data."""
    log("Data Warehouse", "HEADER")
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=CONFIG["postgres_dwh"]["host"],
            port=CONFIG["postgres_dwh"]["port"],
            user=CONFIG["postgres_dwh"]["user"],
            password=CONFIG["postgres_dwh"]["password"],
            database=CONFIG["postgres_dwh"]["database"]
        )
        cursor = conn.cursor()
        
        record_result("dwh_connection", True, "Connected to DWH")
        
        # Check tables
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = [
            "dim_date", "dim_lead_status", "dim_lead_source", "dim_lead_type",
            "dim_agent", "dim_property_type", "dim_area", "dim_campaign",
            "dim_sale_category", "fact_lead", "fact_sale"
        ]
        
        for table in expected_tables:
            if table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                record_result(f"dwh_{table}", True, f"{count} records")
            else:
                record_result(f"dwh_{table}", False, "Table missing")
        
        conn.close()
        
    except Exception as e:
        record_result("dwh", False, f"DWH check failed: {e}")


def test_trino():
    """Test Trino connectivity."""
    log("Trino Query Engine", "HEADER")
    
    try:
        result = subprocess.run(
            ["docker", "exec", "propwise-trino", "trino", "--execute", "SHOW CATALOGS"],
            capture_output=True, text=True, timeout=30
        )
        
        if "hive" in result.stdout.lower():
            record_result("trino_connection", True, "Trino accessible, hive catalog exists")
        else:
            record_result("trino_connection", False, "Trino accessible but hive catalog missing")
        
        # Check staging tables
        result = subprocess.run(
            ["docker", "exec", "propwise-trino", "trino", "--execute", 
             "SHOW TABLES FROM hive.staging"],
            capture_output=True, text=True, timeout=30
        )
        
        tables = result.stdout.strip().split('\n')
        if len(tables) > 0 and tables[0]:
            record_result("trino_staging", True, f"{len(tables)} tables in staging")
        else:
            record_result("trino_staging", False, "No tables in staging schema")
            
    except Exception as e:
        record_result("trino", False, f"Trino check failed: {e}")


def test_logs_bucket():
    """Test logs bucket for observability."""
    log("Observability (Logs)", "HEADER")
    
    try:
        from minio import Minio
        
        client = Minio(
            CONFIG["minio"]["endpoint"],
            CONFIG["minio"]["access_key"],
            CONFIG["minio"]["secret_key"],
            secure=False
        )
        
        buckets = [b.name for b in client.list_buckets()]
        
        if "logs" in buckets:
            record_result("logs_bucket", True, "Logs bucket exists")
            
            # Check for log files
            log_files = list(client.list_objects("logs", recursive=True))
            record_result("logs_files", True, f"{len(log_files)} log files")
        else:
            record_result("logs_bucket", False, "Logs bucket missing")
            
    except Exception as e:
        record_result("logs", False, f"Logs check failed: {e}")


# ============================================================
# MAIN
# ============================================================

def run_all_tests():
    """Run all tests."""
    RESULTS["start_time"] = datetime.now().isoformat()
    
    print("\n" + "=" * 60)
    print("  PROPWISE PIPELINE TEST SUITE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Run tests
    test_docker_services()
    test_minio_connectivity()
    test_mysql_source()
    test_full_load_ingestion()
    test_cdc_ingestion()
    test_schema_contract()
    test_staging_data()
    test_dwh()
    test_trino()
    test_logs_bucket()
    
    RESULTS["end_time"] = datetime.now().isoformat()
    
    # Summary
    log("TEST SUMMARY", "HEADER")
    print(f"\n  \033[92mPassed: {RESULTS['passed']}\033[0m")
    print(f"  \033[91mFailed: {RESULTS['failed']}\033[0m")
    print(f"  \033[93mWarnings: {RESULTS['warnings']}\033[0m")
    print(f"  Total: {RESULTS['passed'] + RESULTS['failed']}")
    
    # Save results
    results_file = PROJECT_ROOT / "tests" / "test_results.json"
    with open(results_file, "w") as f:
        json.dump(RESULTS, f, indent=2, default=str)
    print(f"\n  Results saved to: {results_file}")
    
    if RESULTS['failed'] == 0:
        print("\n\033[92m🎉 All tests passed!\033[0m\n")
        return True
    else:
        print(f"\n\033[93m⚠ {RESULTS['failed']} tests failed.\033[0m\n")
        return False


def run_single_test(test_name):
    """Run a single test by name."""
    tests = {
        "docker": test_docker_services,
        "minio": test_minio_connectivity,
        "mysql": test_mysql_source,
        "ingestion": test_full_load_ingestion,
        "cdc": test_cdc_ingestion,
        "schema": test_schema_contract,
        "staging": test_staging_data,
        "dwh": test_dwh,
        "trino": test_trino,
        "logs": test_logs_bucket
    }
    
    if test_name in tests:
        RESULTS["start_time"] = datetime.now().isoformat()
        tests[test_name]()
        RESULTS["end_time"] = datetime.now().isoformat()
        return RESULTS['failed'] == 0
    else:
        print(f"Unknown test: {test_name}")
        print(f"Available tests: {', '.join(tests.keys())}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Propwise Pipeline Test Suite")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--test", "-t", type=str, help="Run specific test")
    args = parser.parse_args()
    
    VERBOSE = args.verbose
    
    if args.test:
        success = run_single_test(args.test)
    else:
        success = run_all_tests()
    
    sys.exit(0 if success else 1)

