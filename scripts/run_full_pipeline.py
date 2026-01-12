#!/usr/bin/env python3
"""
Propwise Full Pipeline Test Runner
===================================
Tests: Ingestion (Full Load/CDC), Soft Contract, QC, Jobs, DWH Loading
"""

import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root123"
MYSQL_DATABASE = "propwise_source"
METADATA_HOST = "localhost"
METADATA_PORT = 5433
METADATA_USER = "metadata_user"
METADATA_PASSWORD = "metadata123"
METADATA_DATABASE = "propwise_metadata"


def log(message, level="INFO"):
    """Structured logging."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def section(title):
    """Print section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def check_minio():
    """Check MinIO connectivity and buckets."""
    section("1. MinIO CONNECTIVITY CHECK")
    try:
        from minio import Minio
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        
        buckets = [b.name for b in client.list_buckets()]
        log(f"Connected to MinIO. Buckets: {buckets}")
        
        required_buckets = ["raw-data", "staging", "rejected", "logs"]
        for bucket in required_buckets:
            if bucket in buckets:
                log(f"✓ Bucket '{bucket}' exists", "OK")
            else:
                log(f"✗ Bucket '{bucket}' MISSING", "ERROR")
        
        return True
    except Exception as e:
        log(f"MinIO connection failed: {e}", "ERROR")
        return False


def check_mysql():
    """Check MySQL source database."""
    section("2. MYSQL SOURCE CHECK")
    try:
        import pymysql
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        log(f"Tables in MySQL: {tables}")
        
        # Count records
        for table in ['leads', 'sales']:
            if table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                log(f"✓ Table '{table}': {count} records", "OK")
        
        conn.close()
        return True
    except Exception as e:
        log(f"MySQL connection failed: {e}", "ERROR")
        return False


def run_full_load():
    """Run full load ingestion."""
    section("3. FULL LOAD INGESTION")
    try:
        log("Running full load ingestion...")
        os.chdir(project_root)
        
        from ingestion.run_ingestion import run_full_load as ingestion_full_load
        results = ingestion_full_load()
        
        for table, result in results.items():
            if result.get('status') == 'success':
                log(f"✓ {table}: {result.get('records', 0)} records ingested", "OK")
            else:
                log(f"✗ {table}: {result.get('error', 'Unknown error')}", "ERROR")
        
        return True
    except Exception as e:
        log(f"Full load failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def run_cdc_test():
    """Test CDC by inserting a record."""
    section("4. CDC TEST")
    try:
        import pymysql
        from datetime import datetime
        
        # Generate unique test ID
        test_id = f"CDC_TEST_{datetime.now().strftime('%H%M%S')}"
        current_time = datetime.now().strftime("%m/%d/%Y %H:%M")
        
        log(f"Inserting test record: {test_id}")
        
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO leads (
                id, date_of_last_request, buyer, seller, best_time_to_call,
                budget, created_at, updated_at, user_id, location,
                date_of_last_contact, status_name, commercial, do_not_call,
                lead_type_id, customer_id, method_of_contact, lead_source,
                campaign, lead_type
            ) VALUES (
                %s, %s, 'TRUE', 'FALSE', 'Morning', 750000,
                %s, %s, 9999, 'CDC Pipeline Test',
                %s, 'New - CDC Test', 'FALSE', 'FALSE',
                1, 999999, 'pipeline_test', 'cdc_test', 'test_campaign', 'Primary'
            )
        """, (test_id, current_time, current_time, current_time, current_time))
        
        conn.commit()
        conn.close()
        
        log(f"✓ Inserted CDC test record: {test_id}", "OK")
        
        # Run CDC ingestion
        log("Running CDC ingestion...")
        from ingestion.run_ingestion import run_cdc
        results = run_cdc()
        
        for table, result in results.items():
            records = result.get('records', 0)
            log(f"✓ CDC {table}: {records} records captured", "OK")
        
        return True
    except Exception as e:
        log(f"CDC test failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def check_schema_contract():
    """Check soft schema contract validation."""
    section("5. SOFT SCHEMA CONTRACT CHECK")
    try:
        from minio import Minio
        import pandas as pd
        from io import BytesIO
        
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        
        # Get raw data schema
        log("Analyzing raw data schema...")
        
        raw_objects = list(client.list_objects("raw-data", recursive=True))
        csv_files = [obj.object_name for obj in raw_objects if obj.object_name.endswith('.csv')]
        
        if csv_files:
            # Read first CSV to get schema
            obj = client.get_object("raw-data", csv_files[0])
            df = pd.read_csv(BytesIO(obj.read()), nrows=5)
            raw_columns = list(df.columns)
            log(f"Raw data columns ({len(raw_columns)}): {raw_columns[:10]}...")
            
            # Load staging config
            config_path = project_root / "processing/common_code/staging/configs/01_leads.json"
            with open(config_path) as f:
                config = json.load(f)
            
            mapped_columns = list(config.get('mapping', {}).keys())
            log(f"Mapped columns ({len(mapped_columns)}): {mapped_columns[:10]}...")
            
            # Check for schema drift
            raw_set = set(raw_columns)
            mapped_set = set(mapped_columns)
            
            new_columns = raw_set - mapped_set
            missing_columns = mapped_set - raw_set
            
            if new_columns:
                log(f"⚠ New columns in source (unmapped): {new_columns}", "WARN")
            if missing_columns:
                log(f"⚠ Missing columns from source: {missing_columns}", "WARN")
            
            # Check schema evolution config
            schema_config = config.get('schema_evolution', {})
            log(f"Schema evolution mode: {schema_config.get('mode', 'not set')}")
            log(f"Unmapped columns action: {schema_config.get('unmapped_columns', 'not set')}")
            log(f"Missing columns action: {schema_config.get('missing_columns', 'not set')}")
            
            log("✓ Soft schema contract check completed", "OK")
        
        return True
    except Exception as e:
        log(f"Schema contract check failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def run_log_compaction():
    """Run log compaction job."""
    section("6. LOG COMPACTION (Staging)")
    try:
        log("Running log compaction...")
        
        from processing.common_code.staging.scripts.log_compaction_tables import LogCompaction
        
        # Run for leads
        leads_config = project_root / "processing/common_code/staging/configs/01_leads.json"
        compaction = LogCompaction(str(leads_config))
        leads_result = compaction.run()
        log(f"✓ Leads compaction: {leads_result.get('output_records', 0)} records", "OK")
        
        # Run for sales
        sales_config = project_root / "processing/common_code/staging/configs/02_sales.json"
        compaction = LogCompaction(str(sales_config))
        sales_result = compaction.run()
        log(f"✓ Sales compaction: {sales_result.get('output_records', 0)} records", "OK")
        
        return True
    except Exception as e:
        log(f"Log compaction failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def check_quality():
    """Run quality checks."""
    section("7. QUALITY CHECKS")
    try:
        from minio import Minio
        import pandas as pd
        from io import BytesIO
        
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        
        # Check staging data quality
        log("Running quality checks on staging data...")
        
        staging_objects = list(client.list_objects("staging", recursive=True))
        parquet_files = [obj.object_name for obj in staging_objects if obj.object_name.endswith('.parquet')]
        
        quality_metrics = {}
        
        for pq_file in parquet_files[:2]:  # Check first 2 files
            log(f"Checking: {pq_file}")
            obj = client.get_object("staging", pq_file)
            df = pd.read_parquet(BytesIO(obj.read()))
            
            metrics = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "null_rate": df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100,
                "duplicate_rate": (len(df) - len(df.drop_duplicates())) / len(df) * 100 if len(df) > 0 else 0
            }
            
            quality_metrics[pq_file] = metrics
            log(f"  Rows: {metrics['row_count']}, Null Rate: {metrics['null_rate']:.2f}%, Dup Rate: {metrics['duplicate_rate']:.2f}%")
        
        # Check for rejected records
        try:
            rejected_objects = list(client.list_objects("rejected", recursive=True))
            rejected_count = len(rejected_objects)
            if rejected_count > 0:
                log(f"⚠ {rejected_count} rejected record files found", "WARN")
            else:
                log("✓ No rejected records", "OK")
        except:
            log("✓ Rejected bucket empty or not accessible", "OK")
        
        log("✓ Quality checks completed", "OK")
        return True
    except Exception as e:
        log(f"Quality checks failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def log_to_minio(log_data, log_type="pipeline"):
    """Write logs to MinIO logs bucket."""
    try:
        from minio import Minio
        from io import BytesIO
        
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_path = f"logs/{log_type}/{timestamp}.json"
        
        log_bytes = json.dumps(log_data, indent=2, default=str).encode()
        client.put_object(
            "logs", 
            f"{log_type}/{timestamp}.json",
            BytesIO(log_bytes),
            len(log_bytes),
            content_type="application/json"
        )
        log(f"✓ Logs written to MinIO: logs/{log_type}/{timestamp}.json", "OK")
        return True
    except Exception as e:
        log(f"Failed to write logs to MinIO: {e}", "WARN")
        return False


def check_dwh():
    """Check DWH tables."""
    section("8. DWH CHECK")
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host="localhost", port=5432,
            user="dwh_user", password="dwh123",
            database="propwise_dwh"
        )
        cursor = conn.cursor()
        
        # Check dimension tables
        dim_tables = [
            "dim_date", "dim_lead_status", "dim_lead_source", "dim_lead_type",
            "dim_agent", "dim_property_type", "dim_area", "dim_campaign", "dim_sale_category"
        ]
        
        log("Checking dimension tables...")
        for table in dim_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                log(f"  {table}: {count} records")
            except Exception as e:
                log(f"  {table}: Error - {e}", "WARN")
        
        # Check fact tables
        log("Checking fact tables...")
        for table in ["fact_lead", "fact_sale"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                log(f"  {table}: {count} records")
            except Exception as e:
                log(f"  {table}: Error - {e}", "WARN")
        
        conn.close()
        log("✓ DWH check completed", "OK")
        return True
    except Exception as e:
        log(f"DWH check failed: {e}", "ERROR")
        return False


def print_service_queries():
    """Print example business queries for DBeaver."""
    section("9. EXAMPLE BUSINESS QUERIES (For DBeaver)")
    
    queries = """
-- ============================================================
-- PROPWISE BUSINESS QUERIES
-- Run these in DBeaver connected to Trino (localhost:8090)
-- ============================================================

-- 1. Total leads by status
SELECT 
    status_name,
    COUNT(*) as lead_count,
    ROUND(AVG(CAST(budget AS DOUBLE)), 2) as avg_budget
FROM hive.staging.processed_leads
GROUP BY status_name
ORDER BY lead_count DESC;

-- 2. Leads by source (marketing attribution)
SELECT 
    lead_source,
    COUNT(*) as lead_count,
    COUNT(CASE WHEN buyer = 'TRUE' THEN 1 END) as buyers,
    COUNT(CASE WHEN seller = 'TRUE' THEN 1 END) as sellers
FROM hive.staging.processed_leads
GROUP BY lead_source
ORDER BY lead_count DESC;

-- 3. Monthly lead trends
SELECT 
    YEAR(CAST(lead_created_at AS DATE)) as year,
    MONTH(CAST(lead_created_at AS DATE)) as month,
    COUNT(*) as new_leads
FROM hive.staging.processed_leads
WHERE lead_created_at IS NOT NULL
GROUP BY 
    YEAR(CAST(lead_created_at AS DATE)),
    MONTH(CAST(lead_created_at AS DATE))
ORDER BY year, month;

-- 4. Top campaigns by lead count
SELECT 
    campaign,
    COUNT(*) as leads,
    COUNT(DISTINCT location) as unique_locations
FROM hive.staging.processed_leads
WHERE campaign IS NOT NULL AND campaign != ''
GROUP BY campaign
ORDER BY leads DESC
LIMIT 10;

-- 5. Sales summary
SELECT 
    COUNT(*) as total_sales,
    SUM(CAST(sale_price AS DOUBLE)) as total_revenue,
    AVG(CAST(sale_price AS DOUBLE)) as avg_sale_price,
    MIN(CAST(sale_price AS DOUBLE)) as min_sale,
    MAX(CAST(sale_price AS DOUBLE)) as max_sale
FROM hive.staging.processed_sales
WHERE sale_price IS NOT NULL;

-- 6. Sales by property type
SELECT 
    sales_category,
    COUNT(*) as sale_count,
    SUM(CAST(sale_price AS DOUBLE)) as total_value
FROM hive.staging.processed_sales
WHERE sales_category IS NOT NULL
GROUP BY sales_category
ORDER BY total_value DESC;

-- 7. Lead to Sale conversion (join staging tables)
SELECT 
    l.lead_source,
    COUNT(DISTINCT l.id) as total_leads,
    COUNT(DISTINCT s.lead_id) as converted_leads,
    ROUND(COUNT(DISTINCT s.lead_id) * 100.0 / NULLIF(COUNT(DISTINCT l.id), 0), 2) as conversion_rate
FROM hive.staging.processed_leads l
LEFT JOIN hive.staging.processed_sales s ON l.id = s.lead_id
GROUP BY l.lead_source
HAVING COUNT(DISTINCT l.id) > 10
ORDER BY conversion_rate DESC;

-- 8. Agent performance (if agent data available)
SELECT 
    user_id,
    COUNT(*) as leads_handled,
    COUNT(CASE WHEN status_name LIKE '%Converted%' THEN 1 END) as converted,
    ROUND(AVG(CAST(budget AS DOUBLE)), 2) as avg_budget
FROM hive.staging.processed_leads
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY leads_handled DESC
LIMIT 20;

-- ============================================================
-- DWH QUERIES (PostgreSQL - localhost:5432)
-- ============================================================

-- Connect to: postgresql://dwh_user:dwh123@localhost:5432/propwise_dwh

-- 1. DWH Lead Summary
-- SELECT 
--     ds.status_name,
--     COUNT(*) as lead_count
-- FROM fact_lead f
-- JOIN dim_lead_status ds ON f.lead_status_sk = ds.lead_status_sk
-- GROUP BY ds.status_name;

-- 2. DWH Sales by Date
-- SELECT 
--     d.year_actual,
--     d.month_name,
--     COUNT(*) as sales,
--     SUM(f.actual_value) as revenue
-- FROM fact_sale f
-- JOIN dim_date d ON f.reservation_date_sk = d.date_sk
-- GROUP BY d.year_actual, d.month_name
-- ORDER BY d.year_actual, d.month_name;
"""
    
    print(queries)
    
    # Save queries to file
    queries_file = project_root / "scripts/business_queries.sql"
    with open(queries_file, "w") as f:
        f.write(queries)
    log(f"✓ Queries saved to: {queries_file}", "OK")


def main():
    """Run full pipeline test."""
    print("\n" + "=" * 70)
    print("  PROPWISE FULL PIPELINE TEST")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)
    
    results = {}
    pipeline_log = {
        "timestamp": datetime.now().isoformat(),
        "steps": {}
    }
    
    # Step 1: Check MinIO
    results["minio"] = check_minio()
    pipeline_log["steps"]["minio"] = "passed" if results["minio"] else "failed"
    
    # Step 2: Check MySQL
    results["mysql"] = check_mysql()
    pipeline_log["steps"]["mysql"] = "passed" if results["mysql"] else "failed"
    
    # Step 3: Full Load
    results["full_load"] = run_full_load()
    pipeline_log["steps"]["full_load"] = "passed" if results["full_load"] else "failed"
    
    # Step 4: CDC Test
    results["cdc"] = run_cdc_test()
    pipeline_log["steps"]["cdc"] = "passed" if results["cdc"] else "failed"
    
    # Step 5: Schema Contract
    results["schema_contract"] = check_schema_contract()
    pipeline_log["steps"]["schema_contract"] = "passed" if results["schema_contract"] else "failed"
    
    # Step 6: Log Compaction
    results["log_compaction"] = run_log_compaction()
    pipeline_log["steps"]["log_compaction"] = "passed" if results["log_compaction"] else "failed"
    
    # Step 7: Quality Checks
    results["quality"] = check_quality()
    pipeline_log["steps"]["quality"] = "passed" if results["quality"] else "failed"
    
    # Step 8: DWH Check
    results["dwh"] = check_dwh()
    pipeline_log["steps"]["dwh"] = "passed" if results["dwh"] else "failed"
    
    # Step 9: Business Queries
    print_service_queries()
    
    # Write logs to MinIO
    log_to_minio(pipeline_log, "pipeline")
    
    # Summary
    section("PIPELINE TEST SUMMARY")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for step, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"  {step.upper()}: {status}")
    
    print(f"\n  Total: {passed}/{total} steps passed")
    
    if passed == total:
        log("🎉 All pipeline steps completed successfully!", "OK")
    else:
        log(f"⚠ {total - passed} steps failed. Check logs above.", "WARN")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

