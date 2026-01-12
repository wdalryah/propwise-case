# Propwise Pipeline Testing Guide

**Author:** Mohamed Alryah - abdalrhman.alryah@linux.com

This document contains all commands and scripts to test the Propwise Data Platform.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Docker Services](#2-docker-services)
3. [Data Loading](#3-data-loading)
4. [Ingestion Testing](#4-ingestion-testing)
5. [CDC Testing](#5-cdc-testing)
6. [Log Compaction](#6-log-compaction)
7. [Quality Checks](#7-quality-checks)
8. [DWH Testing](#8-dwh-testing)
9. [Airflow Testing](#9-airflow-testing)
10. [Trino/DBeaver Testing](#10-trinodbeaver-testing)
11. [Full Pipeline Test](#11-full-pipeline-test)

---

## 1. Prerequisites

```bash
# Navigate to project directory
cd /Users/mdalryah/developement/propwise-use-case

# Ensure Docker Desktop is running
docker info

# Check Python dependencies
pip install minio pandas pyarrow pymysql psycopg2-binary
```

---

## 2. Docker Services

### Start All Services
```bash
docker compose up -d
```

### Check Service Status
```bash
docker compose ps
```

### Check Individual Service Logs
```bash
# MinIO
docker logs propwise-minio

# MySQL
docker logs propwise-mysql-source

# PostgreSQL DWH
docker logs propwise-postgres-dwh

# Trino
docker logs propwise-trino

# Airflow
docker logs propwise-airflow-webserver
```

### Restart Services
```bash
# Restart all
docker compose restart

# Restart specific service
docker compose restart minio
docker compose restart mysql
docker compose restart trino
```

### Stop All Services
```bash
docker compose down
```

### Full Reset (Delete Data)
```bash
docker compose down -v
docker compose up -d
```

---

## 3. Data Loading

### Load Source Data into MySQL
```bash
cd /Users/mdalryah/developement/propwise-use-case
python scripts/load_source_data.py
```

### Verify Data in MySQL
```bash
docker exec propwise-mysql-source mysql -u root -proot123 -e "
USE propwise_source;
SELECT 'leads' as tbl, COUNT(*) as cnt FROM leads
UNION ALL
SELECT 'sales', COUNT(*) FROM sales;
"
```

---

## 4. Ingestion Testing

### Run Full Load
```bash
cd /Users/mdalryah/developement/propwise-use-case
python ingestion/run_ingestion.py --mode full
```

### Verify Full Load in MinIO
```bash
# List raw-data bucket
docker exec propwise-minio mc ls local/raw-data/propwise/ --recursive | head -20

# Or via Python
python << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
for obj in client.list_objects("raw-data", prefix="propwise/", recursive=True):
    print(f"{obj.object_name} - {obj.size} bytes")
EOF
```

### Check Ingestion Logs
```bash
cat logs/full_load_results.json
```

---

## 5. CDC Testing

### Insert Test Record
```bash
python << 'EOF'
import pymysql
from datetime import datetime

test_id = f"CDC_TEST_{datetime.now().strftime('%H%M%S')}"
current_time = datetime.now().strftime("%m/%d/%Y %H:%M")

conn = pymysql.connect(
    host='localhost', port=3306,
    user='root', password='root123',
    database='propwise_source'
)
cursor = conn.cursor()

cursor.execute("""
    INSERT INTO leads (
        id, date_of_last_request, buyer, seller, best_time_to_call,
        budget, created_at, updated_at, user_id, location,
        status_name, commercial, do_not_call, lead_type_id,
        customer_id, method_of_contact, lead_source, campaign, lead_type
    ) VALUES (
        %s, %s, 'TRUE', 'FALSE', 'Morning', 500000,
        %s, %s, 9999, 'CDC Test Location',
        'New', 'FALSE', 'FALSE', 1, 999999,
        'cdc_test', 'cdc_source', 'test_campaign', 'Primary'
    )
""", (test_id, current_time, current_time, current_time))

conn.commit()
conn.close()
print(f"✓ Inserted: {test_id}")
EOF
```

### Run CDC Ingestion
```bash
python ingestion/run_ingestion.py --mode cdc
```

### Start CDC Daemon (Real-time)
```bash
# Run in background
python ingestion/cdc_daemon.py --mode polling --poll-interval 5 &

# Check daemon logs
tail -f /tmp/cdc_daemon.log
```

### Verify CDC Data
```bash
cat logs/cdc_results.json
cat logs/watermarks.json
```

---

## 6. Log Compaction

### Run Log Compaction for Leads
```bash
python << 'EOF'
import sys
sys.path.insert(0, '.')
from minio import Minio
from processing.common_code.staging.scripts.log_compaction_tables import LogCompaction

client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
compaction = LogCompaction(
    "processing/common_code/staging/configs/01_leads.json",
    client
)
result = compaction.run()
print(f"Leads compaction: {result}")
EOF
```

### Run Log Compaction for Sales
```bash
python << 'EOF'
import sys
sys.path.insert(0, '.')
from minio import Minio
from processing.common_code.staging.scripts.log_compaction_tables import LogCompaction

client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
compaction = LogCompaction(
    "processing/common_code/staging/configs/02_sales.json",
    client
)
result = compaction.run()
print(f"Sales compaction: {result}")
EOF
```

### Verify Staging Data
```bash
python << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
for obj in client.list_objects("staging", recursive=True):
    print(f"{obj.object_name} - {obj.size} bytes")
EOF
```

---

## 7. Quality Checks

### Check Data Quality on Staging
```bash
python << 'EOF'
from minio import Minio
import pandas as pd
from io import BytesIO

client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)

# Get first parquet file
for obj in client.list_objects("staging", prefix="processed_leads/", recursive=True):
    if obj.object_name.endswith('.parquet'):
        data = client.get_object("staging", obj.object_name)
        df = pd.read_parquet(BytesIO(data.read()))
        
        print(f"File: {obj.object_name}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"Null Rate: {df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100:.2f}%")
        print(f"Duplicate Rate: {(len(df) - len(df.drop_duplicates())) / len(df) * 100:.2f}%")
        break
EOF
```

### Check Rejected Records
```bash
python << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
rejected = list(client.list_objects("rejected", recursive=True))
print(f"Rejected files: {len(rejected)}")
for obj in rejected[:10]:
    print(f"  {obj.object_name}")
EOF
```

---

## 8. DWH Testing

### Initialize DWH Schema
```bash
docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh \
  -f /docker-entrypoint-initdb.d/01-init-dwh.sql
```

### Check DWH Tables
```bash
docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;
"
```

### Check Dimension Counts
```bash
docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh -c "
SELECT 'dim_date' as tbl, COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_lead_status', COUNT(*) FROM dim_lead_status
UNION ALL SELECT 'dim_lead_source', COUNT(*) FROM dim_lead_source
UNION ALL SELECT 'dim_agent', COUNT(*) FROM dim_agent
UNION ALL SELECT 'fact_lead', COUNT(*) FROM fact_lead
UNION ALL SELECT 'fact_sale', COUNT(*) FROM fact_sale;
"
```

### Connect to DWH via psql
```bash
docker exec -it propwise-postgres-dwh psql -U dwh_user -d propwise_dwh
```

---

## 9. Airflow Testing

### Access Airflow UI
```
URL: http://localhost:8085
Username: admin
Password: admin123
```

### Trigger DAG via CLI
```bash
docker exec propwise-airflow-webserver airflow dags trigger propwise_etl_dag
```

### Check DAG Status
```bash
docker exec propwise-airflow-webserver airflow dags list
docker exec propwise-airflow-webserver airflow tasks list propwise_etl_dag
```

### View Task Logs
```bash
docker exec propwise-airflow-webserver airflow tasks logs propwise_etl_dag log_compaction 2026-01-12
```

---

## 10. Trino/DBeaver Testing

### Connect via Trino CLI
```bash
docker exec -it propwise-trino trino
```

### Sample Trino Queries
```sql
-- List catalogs
SHOW CATALOGS;

-- List schemas in hive
SHOW SCHEMAS FROM hive;

-- Query staging data
SELECT COUNT(*) FROM hive.staging.processed_leads;

-- Query with grouping
SELECT status_name, COUNT(*) as cnt
FROM hive.staging.processed_leads
GROUP BY status_name
ORDER BY cnt DESC;
```

### DBeaver Connection Settings

**Trino:**
```
Driver: Trino
Host: localhost
Port: 8090
Database: hive
Username: trino
Password: (empty)
```

**PostgreSQL DWH:**
```
Host: localhost
Port: 5432
Database: propwise_dwh
Username: dwh_user
Password: dwh123
```

---

## 11. Full Pipeline Test

### Run Complete Pipeline Test
```bash
cd /Users/mdalryah/developement/propwise-use-case
python scripts/run_full_pipeline.py
```

### Manual Step-by-Step Test
```bash
# Step 1: Check services
docker compose ps

# Step 2: Full load ingestion
python ingestion/run_ingestion.py --mode full

# Step 3: Insert CDC test record
python scripts/cdc_test_loader.py

# Step 4: Run CDC
python ingestion/run_ingestion.py --mode cdc

# Step 5: Run log compaction (via Airflow or manual)
# Via Airflow UI: trigger propwise_etl_dag

# Step 6: Check staging data in Trino
docker exec -it propwise-trino trino --execute "SELECT COUNT(*) FROM hive.staging.processed_leads"

# Step 7: Check DWH
docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh -c "SELECT COUNT(*) FROM dim_date"
```

---

## Quick Reference Commands

```bash
# Project directory
cd /Users/mdalryah/developement/propwise-use-case

# Start everything
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f <service_name>

# MinIO Console
open http://localhost:9001  # minioadmin/minioadmin123

# Airflow Console
open http://localhost:8085  # admin/admin123

# Trino CLI
docker exec -it propwise-trino trino

# MySQL CLI
docker exec -it propwise-mysql-source mysql -u root -proot123 propwise_source

# PostgreSQL DWH CLI
docker exec -it propwise-postgres-dwh psql -U dwh_user propwise_dwh

# Generate HTML docs
cd docs && python generate_all_html.py
```

---

## Troubleshooting

### Service Not Starting
```bash
# Check logs
docker compose logs <service_name>

# Restart service
docker compose restart <service_name>
```

### Port Already in Use
```bash
# Find process using port
lsof -i :9000

# Kill process
kill -9 <PID>
```

### Reset Everything
```bash
docker compose down -v
docker compose up -d
python scripts/load_source_data.py
```

---

**Propwise use-case, abdalrhman.alryah@linux.com**

