#!/usr/bin/env python3
"""
CDC Daemon Test Script
=======================

Tests the real-time CDC daemon by:
1. Starting the CDC daemon in the background
2. Inserting test records into MySQL
3. Verifying they appear in MinIO raw bucket

Usage:
    # Terminal 1: Start the CDC daemon
    python ingestion/cdc_daemon.py --mode polling --poll-interval 5
    
    # Terminal 2: Run this test script
    python scripts/test_cdc_daemon.py
"""

import time
import pymysql
from datetime import datetime
from minio import Minio
import io
import pandas as pd

print("=" * 60)
print("CDC DAEMON TEST")
print("=" * 60)

# Generate unique test ID
test_id = f"CDC_AUTO_{datetime.now().strftime('%H%M%S')}"

# ==========================================
# 1. INSERT TEST RECORD INTO MYSQL
# ==========================================
print(f"\n1️⃣ Inserting test record into MySQL (id={test_id})...")

conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='root123',
    database='propwise_source'
)
cursor = conn.cursor()

# Insert a test lead
cursor.execute("""
    INSERT INTO leads (
        id, date_of_last_request, buyer, seller, best_time_to_call,
        budget, created_at, updated_at, user_id, location,
        date_of_last_contact, status_name, commercial, merged,
        area_id, compound_id, developer_id, meeting_flag, do_not_call,
        lead_type_id, customer_id, method_of_contact, lead_source,
        campaign, lead_type
    ) VALUES (
        %s, %s, 'TRUE', 'FALSE', 'Morning',
        500000, %s, %s, 9999, 'Auto CDC Test Location',
        %s, 'New', 'FALSE', NULL,
        NULL, NULL, NULL, NULL, 'FALSE',
        1, 999999, 'auto_cdc_test', 'auto_cdc_test',
        'auto_cdc_campaign', 'Primary'
    )
""", (
    test_id,
    datetime.now().strftime("%m/%d/%Y %H:%M"),
    datetime.now().strftime("%m/%d/%Y %H:%M"),
    datetime.now().strftime("%m/%d/%Y %H:%M"),
    datetime.now().strftime("%m/%d/%Y %H:%M")
))
conn.commit()
print(f"   ✓ Inserted test record: {test_id}")

# ==========================================
# 2. WAIT FOR CDC TO PICK IT UP
# ==========================================
print(f"\n2️⃣ Waiting for CDC daemon to capture the change...")
print("   (If CDC daemon is running, it should pick this up within poll interval)")
print("   Waiting 15 seconds...")

for i in range(15, 0, -1):
    print(f"   {i}...", end='\r')
    time.sleep(1)
print("   Done waiting!    ")

# ==========================================
# 3. CHECK MINIO FOR THE RECORD
# ==========================================
print(f"\n3️⃣ Checking MinIO for the captured record...")

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

# List all CDC files
cdc_files = list(minio_client.list_objects(
    "raw-data", 
    prefix="propwise/raw_leads/cdc/", 
    recursive=True
))

print(f"   Found {len(cdc_files)} CDC files in raw-data bucket")

# Check latest CDC files for our test record
found = False
for obj in sorted(cdc_files, key=lambda x: x.last_modified, reverse=True)[:10]:
    if obj.object_name.endswith('.csv'):
        response = minio_client.get_object("raw-data", obj.object_name)
        df = pd.read_csv(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        if test_id in df['id'].astype(str).values:
            print(f"\n   ✅ FOUND test record in: {obj.object_name}")
            print(f"   File timestamp: {obj.last_modified}")
            
            # Show the record
            test_row = df[df['id'].astype(str) == test_id].iloc[0]
            print("\n   Record details:")
            print(f"   - id: {test_row['id']}")
            print(f"   - extraction_time: {test_row.get('extraction_time', 'N/A')}")
            print(f"   - gpk: {test_row.get('gpk', 'N/A')}")
            print(f"   - op: {test_row.get('op', 'N/A')}")
            found = True
            break

if not found:
    print("\n   ⚠️  Test record NOT FOUND in CDC files")
    print("   Make sure the CDC daemon is running:")
    print("   python ingestion/cdc_daemon.py --mode polling --poll-interval 5")

# ==========================================
# 4. UPDATE THE RECORD
# ==========================================
print(f"\n4️⃣ Updating test record in MySQL...")

cursor.execute("""
    UPDATE leads 
    SET status_name = 'Updated via CDC Auto Test',
        budget = 750000,
        updated_at = %s
    WHERE id = %s
""", (datetime.now().strftime("%m/%d/%Y %H:%M"), test_id))
conn.commit()
print(f"   ✓ Updated test record: {test_id}")

# Wait again
print("\n   Waiting 10 seconds for CDC to capture update...")
time.sleep(10)

# ==========================================
# 5. CLEANUP (Optional)
# ==========================================
print(f"\n5️⃣ Cleanup...")
# cursor.execute("DELETE FROM leads WHERE id = %s", (test_id,))
# conn.commit()
# print(f"   ✓ Deleted test record: {test_id}")

conn.close()

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)
print(f"""
Summary:
- Test record ID: {test_id}
- Record found in MinIO: {'YES ✅' if found else 'NO ❌'}

To monitor real-time, run the CDC daemon:
  python ingestion/cdc_daemon.py --mode polling --poll-interval 5

The daemon will continuously watch for changes and ingest them automatically.
""")

