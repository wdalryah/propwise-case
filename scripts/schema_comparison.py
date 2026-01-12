#!/usr/bin/env python3
"""
Schema & Data Comparison Script
================================

Compares:
1. MySQL source schema vs MinIO raw schema
2. Lists all transformations applied during ingestion
3. Compares record counts for quality checks
"""

import pandas as pd
from minio import Minio
import pymysql
import io
import json

print("=" * 80)
print("PROPWISE DATA PLATFORM - SCHEMA & DATA COMPARISON")
print("=" * 80)

# ==========================================
# 1. MYSQL SOURCE SCHEMA
# ==========================================
print("\n" + "=" * 80)
print("1. MYSQL SOURCE SCHEMA")
print("=" * 80)

try:
    mysql_conn = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='root123',
        database='propwise_source'
    )
    cursor = mysql_conn.cursor()
    
    # LEADS table schema
    print("\n📋 LEADS TABLE (MySQL):")
    print("-" * 40)
    cursor.execute("DESCRIBE leads")
    leads_schema_mysql = cursor.fetchall()
    print(f"{'Column':<30} {'Type':<30} {'Null':<10}")
    print("-" * 70)
    for col in leads_schema_mysql:
        print(f"{col[0]:<30} {col[1]:<30} {col[2]:<10}")
    
    # SALES table schema
    print("\n📋 SALES TABLE (MySQL):")
    print("-" * 40)
    cursor.execute("DESCRIBE sales")
    sales_schema_mysql = cursor.fetchall()
    print(f"{'Column':<30} {'Type':<30} {'Null':<10}")
    print("-" * 70)
    for col in sales_schema_mysql:
        print(f"{col[0]:<30} {col[1]:<30} {col[2]:<10}")
    
    # Get row counts
    cursor.execute("SELECT COUNT(*) FROM leads")
    leads_count_mysql = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sales")
    sales_count_mysql = cursor.fetchone()[0]
    
    print(f"\n📊 MySQL Row Counts:")
    print(f"   Leads: {leads_count_mysql:,}")
    print(f"   Sales: {sales_count_mysql:,}")
    
    mysql_conn.close()
except Exception as e:
    print(f"❌ MySQL Error: {e}")
    leads_count_mysql = 0
    sales_count_mysql = 0

# ==========================================
# 2. MINIO RAW BUCKET SCHEMA
# ==========================================
print("\n" + "=" * 80)
print("2. MINIO RAW BUCKET SCHEMA")
print("=" * 80)

try:
    minio_client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
    
    # Find raw leads file
    leads_files = list(minio_client.list_objects("raw-data", prefix="propwise/raw_leads/", recursive=True))
    
    if leads_files:
        latest_leads = sorted([f.object_name for f in leads_files if f.object_name.endswith('.csv')])[-1]
        print(f"\n📋 RAW_LEADS (MinIO): {latest_leads}")
        print("-" * 40)
        
        response = minio_client.get_object("raw-data", latest_leads)
        leads_df = pd.read_csv(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        print(f"{'Column':<30} {'Type':<30}")
        print("-" * 60)
        for col in leads_df.columns:
            print(f"{col:<30} {str(leads_df[col].dtype):<30}")
        
        leads_count_minio = len(leads_df)
    else:
        print("   No leads data found in raw-data bucket")
        leads_count_minio = 0
        leads_df = None
    
    # Find raw sales file
    sales_files = list(minio_client.list_objects("raw-data", prefix="propwise/raw_sales/", recursive=True))
    
    if sales_files:
        latest_sales = sorted([f.object_name for f in sales_files if f.object_name.endswith('.csv')])[-1]
        print(f"\n📋 RAW_SALES (MinIO): {latest_sales}")
        print("-" * 40)
        
        response = minio_client.get_object("raw-data", latest_sales)
        sales_df = pd.read_csv(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        print(f"{'Column':<30} {'Type':<30}")
        print("-" * 60)
        for col in sales_df.columns:
            print(f"{col:<30} {str(sales_df[col].dtype):<30}")
        
        sales_count_minio = len(sales_df)
    else:
        print("   No sales data found in raw-data bucket")
        sales_count_minio = 0
        sales_df = None
    
    print(f"\n📊 MinIO Row Counts:")
    print(f"   Raw Leads: {leads_count_minio:,}")
    print(f"   Raw Sales: {sales_count_minio:,}")

except Exception as e:
    print(f"❌ MinIO Error: {e}")
    leads_count_minio = 0
    sales_count_minio = 0
    leads_df = None
    sales_df = None

# ==========================================
# 3. SCHEMA COMPARISON (MySQL vs MinIO)
# ==========================================
print("\n" + "=" * 80)
print("3. SCHEMA COMPARISON (MySQL vs MinIO)")
print("=" * 80)

mysql_leads_cols = [col[0] for col in leads_schema_mysql] if 'leads_schema_mysql' in dir() else []
mysql_sales_cols = [col[0] for col in sales_schema_mysql] if 'sales_schema_mysql' in dir() else []
minio_leads_cols = list(leads_df.columns) if leads_df is not None else []
minio_sales_cols = list(sales_df.columns) if sales_df is not None else []

print("\n📋 LEADS SCHEMA COMPARISON:")
print("-" * 60)
print(f"{'MySQL Columns':<30} {'MinIO Columns':<30}")
print("-" * 60)

# Find added columns (in MinIO but not in MySQL)
added_leads_cols = set(minio_leads_cols) - set(mysql_leads_cols)
common_leads_cols = set(mysql_leads_cols) & set(minio_leads_cols)

for col in mysql_leads_cols:
    minio_col = col if col in minio_leads_cols else "—"
    print(f"{col:<30} {minio_col:<30}")

print("\n🆕 AUDIT COLUMNS ADDED (Leads):")
for col in sorted(added_leads_cols):
    print(f"   + {col}")

print("\n📋 SALES SCHEMA COMPARISON:")
print("-" * 60)
print(f"{'MySQL Columns':<30} {'MinIO Columns':<30}")
print("-" * 60)

added_sales_cols = set(minio_sales_cols) - set(mysql_sales_cols)

for col in mysql_sales_cols:
    minio_col = col if col in minio_sales_cols else "—"
    print(f"{col:<30} {minio_col:<30}")

print("\n🆕 AUDIT COLUMNS ADDED (Sales):")
for col in sorted(added_sales_cols):
    print(f"   + {col}")

# ==========================================
# 4. TRANSFORMATIONS APPLIED
# ==========================================
print("\n" + "=" * 80)
print("4. TRANSFORMATIONS APPLIED BY INGESTION ENGINE")
print("=" * 80)

transformations = """
┌─────────────────────────────────────────────────────────────────────────────┐
│                     INGESTION ENGINE TRANSFORMATIONS                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  📁 TABLE TRANSFORMATIONS:                                                   │
│  ─────────────────────────                                                   │
│  1. Table Prefix: "raw_" added to all table names                           │
│     • leads → raw_leads                                                      │
│     • sales → raw_sales                                                      │
│                                                                              │
│  2. Schema Rename:                                                           │
│     • propwise_source → propwise                                             │
│                                                                              │
│  📝 COLUMN TRANSFORMATIONS (Audit Columns Added):                           │
│  ────────────────────────────────────────────────                           │
│  1. extraction_time (timestamp)                                              │
│     → Current timestamp when data was extracted                              │
│     → Format: ISO 8601 (YYYY-MM-DDTHH:MM:SS.ffffff)                          │
│                                                                              │
│  2. gpk - Global Primary Key (string)                                        │
│     → Composite key: {id}~{extraction_timestamp}                             │
│     → Example: "12345~20260110143022"                                        │
│     → Purpose: Unique identifier across all extractions                      │
│                                                                              │
│  3. env (string)                                                             │
│     → Environment identifier                                                 │
│     → Value: "test" (configurable)                                           │
│                                                                              │
│  4. region (string)                                                          │
│     → Region identifier                                                      │
│     → Value: "local" (configurable)                                          │
│                                                                              │
│  5. op - Operation Type (string, 1 char)                                     │
│     → I = Insert (Full Load)                                                 │
│     → U = Update (CDC)                                                       │
│     → D = Delete (CDC with soft delete detection)                            │
│                                                                              │
│  ⚠️  NO DATA TRANSFORMATIONS:                                                │
│  ───────────────────────────                                                 │
│  • NO type conversions                                                       │
│  • NO value modifications                                                    │
│  • NO deduplication                                                          │
│  • NO filtering                                                              │
│  • Source data preserved exactly as-is                                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
"""
print(transformations)

# ==========================================
# 5. DATA QUALITY COMPARISON
# ==========================================
print("\n" + "=" * 80)
print("5. DATA QUALITY COMPARISON (MySQL vs MinIO)")
print("=" * 80)

print("\n📊 ROW COUNT COMPARISON:")
print("-" * 60)
print(f"{'Dataset':<20} {'MySQL':<15} {'MinIO':<15} {'Match':<10}")
print("-" * 60)

leads_match = "✅ YES" if leads_count_mysql == leads_count_minio else f"❌ NO (diff: {leads_count_minio - leads_count_mysql})"
sales_match = "✅ YES" if sales_count_mysql == sales_count_minio else f"❌ NO (diff: {sales_count_minio - sales_count_mysql})"

print(f"{'Leads':<20} {leads_count_mysql:<15,} {leads_count_minio:<15,} {leads_match}")
print(f"{'Sales':<20} {sales_count_mysql:<15,} {sales_count_minio:<15,} {sales_match}")

# Sample data comparison
if leads_df is not None and leads_count_mysql > 0:
    print("\n📋 SAMPLE DATA VERIFICATION (First 5 IDs from Leads):")
    print("-" * 60)
    
    # Get first 5 IDs from MinIO
    minio_ids = leads_df['id'].head(5).tolist()
    print(f"MinIO raw_leads IDs: {minio_ids}")
    
    # Check if they exist in MySQL
    try:
        mysql_conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='root123',
            database='propwise_source'
        )
        cursor = mysql_conn.cursor()
        
        id_list = ','.join([f"'{id}'" for id in minio_ids])
        cursor.execute(f"SELECT id FROM leads WHERE id IN ({id_list})")
        mysql_ids = [row[0] for row in cursor.fetchall()]
        
        matching = set(str(id) for id in minio_ids) & set(str(id) for id in mysql_ids)
        print(f"Found in MySQL: {len(matching)}/{len(minio_ids)} IDs match")
        
        mysql_conn.close()
    except Exception as e:
        print(f"   Error checking MySQL: {e}")

print("\n" + "=" * 80)
print("COMPARISON COMPLETE")
print("=" * 80)

