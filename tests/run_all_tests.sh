#!/bin/bash
# ============================================================
# Propwise Full Pipeline Test Script
# Author: Mohamed Alryah - abdalrhman.alryah@linux.com
# ============================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project directory
PROJECT_DIR="/Users/mdalryah/developement/propwise-use-case"
cd "$PROJECT_DIR"

# Counters
PASSED=0
FAILED=0

# Helper functions
print_header() {
    echo ""
    echo "============================================================"
    echo -e "${BLUE}  $1${NC}"
    echo "============================================================"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    ((PASSED++))
}

print_fail() {
    echo -e "${RED}✗ $1${NC}"
    ((FAILED++))
}

print_warn() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "  $1"
}

# ============================================================
# TEST 1: Docker Services
# ============================================================
print_header "TEST 1: Docker Services Check"

if docker compose ps | grep -q "propwise-minio.*Up"; then
    print_success "MinIO is running"
else
    print_fail "MinIO is NOT running"
fi

if docker compose ps | grep -q "propwise-mysql.*Up"; then
    print_success "MySQL is running"
else
    print_fail "MySQL is NOT running"
fi

if docker compose ps | grep -q "propwise-trino.*Up"; then
    print_success "Trino is running"
else
    print_fail "Trino is NOT running"
fi

if docker compose ps | grep -q "propwise-postgres-dwh.*Up"; then
    print_success "PostgreSQL DWH is running"
else
    print_fail "PostgreSQL DWH is NOT running"
fi

if docker compose ps | grep -q "propwise-airflow-webserver.*Up"; then
    print_success "Airflow is running"
else
    print_fail "Airflow is NOT running"
fi

# ============================================================
# TEST 2: MinIO Buckets
# ============================================================
print_header "TEST 2: MinIO Buckets Check"

BUCKETS=$(python3 << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
buckets = [b.name for b in client.list_buckets()]
print(" ".join(buckets))
EOF
)

for bucket in raw-data staging rejected logs; do
    if echo "$BUCKETS" | grep -q "$bucket"; then
        print_success "Bucket '$bucket' exists"
    else
        print_fail "Bucket '$bucket' MISSING"
    fi
done

# ============================================================
# TEST 3: MySQL Source Data
# ============================================================
print_header "TEST 3: MySQL Source Data Check"

LEADS_COUNT=$(docker exec propwise-mysql-source mysql -u root -proot123 -N -e "SELECT COUNT(*) FROM propwise_source.leads" 2>/dev/null)
SALES_COUNT=$(docker exec propwise-mysql-source mysql -u root -proot123 -N -e "SELECT COUNT(*) FROM propwise_source.sales" 2>/dev/null)

if [ "$LEADS_COUNT" -gt 0 ]; then
    print_success "Leads table has $LEADS_COUNT records"
else
    print_fail "Leads table is empty"
fi

if [ "$SALES_COUNT" -gt 0 ]; then
    print_success "Sales table has $SALES_COUNT records"
else
    print_fail "Sales table is empty"
fi

# ============================================================
# TEST 4: Full Load Ingestion
# ============================================================
print_header "TEST 4: Full Load Ingestion"

print_info "Running full load..."
python ingestion/run_ingestion.py --mode full > /tmp/fullload.log 2>&1

if grep -q "FULL LOAD COMPLETE" /tmp/fullload.log; then
    print_success "Full load completed"
    ROWS=$(grep "Total rows:" /tmp/fullload.log | tail -1 | awk '{print $NF}')
    print_info "Total rows ingested: $ROWS"
else
    print_fail "Full load failed"
    cat /tmp/fullload.log
fi

# ============================================================
# TEST 5: CDC Ingestion
# ============================================================
print_header "TEST 5: CDC Ingestion"

# Insert test record
print_info "Inserting CDC test record..."
python3 << 'EOF'
import pymysql
from datetime import datetime

test_id = f"TEST_{datetime.now().strftime('%H%M%S')}"
ts = datetime.now().strftime("%m/%d/%Y %H:%M")

conn = pymysql.connect(host='localhost', port=3306, user='root', password='root123', database='propwise_source')
cursor = conn.cursor()
cursor.execute("""
    INSERT INTO leads (id, date_of_last_request, buyer, seller, budget, created_at, updated_at, 
    user_id, location, status_name, lead_source, campaign, lead_type)
    VALUES (%s, %s, 'TRUE', 'FALSE', 500000, %s, %s, 9999, 'Test', 'New', 'test', 'test', 'Primary')
""", (test_id, ts, ts, ts))
conn.commit()
conn.close()
print(f"Inserted: {test_id}")
EOF

print_info "Running CDC..."
python ingestion/run_ingestion.py --mode cdc > /tmp/cdc.log 2>&1

if grep -q "CDC COMPLETE" /tmp/cdc.log; then
    print_success "CDC completed"
else
    print_fail "CDC failed"
fi

# ============================================================
# TEST 6: Raw Data in MinIO
# ============================================================
print_header "TEST 6: Raw Data in MinIO"

RAW_FILES=$(python3 << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
count = len(list(client.list_objects("raw-data", prefix="propwise/", recursive=True)))
print(count)
EOF
)

if [ "$RAW_FILES" -gt 0 ]; then
    print_success "Raw data bucket has $RAW_FILES files"
else
    print_fail "Raw data bucket is empty"
fi

# ============================================================
# TEST 7: Staging Data Check
# ============================================================
print_header "TEST 7: Staging Data Check"

STAGING_FILES=$(python3 << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
files = list(client.list_objects("staging", recursive=True))
parquet_count = len([f for f in files if f.object_name.endswith('.parquet')])
print(parquet_count)
EOF
)

if [ "$STAGING_FILES" -gt 0 ]; then
    print_success "Staging bucket has $STAGING_FILES parquet files"
else
    print_warn "Staging bucket has no parquet files (run log compaction)"
fi

# ============================================================
# TEST 8: DWH Tables
# ============================================================
print_header "TEST 8: DWH Tables Check"

DWH_TABLES=$(docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh -t -c "
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
" 2>/dev/null | tr -d ' ')

if [ "$DWH_TABLES" -ge 10 ]; then
    print_success "DWH has $DWH_TABLES tables"
else
    print_warn "DWH has only $DWH_TABLES tables (expected 12)"
    print_info "Initializing DWH schema..."
    docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh \
        -f /docker-entrypoint-initdb.d/01-init-dwh.sql > /dev/null 2>&1
fi

# Check dim_date
DIM_DATE_COUNT=$(docker exec propwise-postgres-dwh psql -U dwh_user -d propwise_dwh -t -c "SELECT COUNT(*) FROM dim_date" 2>/dev/null | tr -d ' ')
if [ "$DIM_DATE_COUNT" -gt 0 ]; then
    print_success "dim_date has $DIM_DATE_COUNT records"
else
    print_warn "dim_date is empty"
fi

# ============================================================
# TEST 9: Trino Connectivity
# ============================================================
print_header "TEST 9: Trino Connectivity"

TRINO_RESULT=$(docker exec propwise-trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -c "hive" || echo "0")

if [ "$TRINO_RESULT" -gt 0 ]; then
    print_success "Trino is accessible and hive catalog exists"
else
    print_fail "Trino connection failed"
fi

# Try query on staging
TRINO_STAGING=$(docker exec propwise-trino trino --execute "SHOW TABLES FROM hive.staging" 2>/dev/null | wc -l || echo "0")
print_info "Trino sees $TRINO_STAGING tables in staging schema"

# ============================================================
# TEST 10: Logs Bucket
# ============================================================
print_header "TEST 10: Logs Bucket"

LOGS_EXISTS=$(python3 << 'EOF'
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
buckets = [b.name for b in client.list_buckets()]
print("yes" if "logs" in buckets else "no")
EOF
)

if [ "$LOGS_EXISTS" = "yes" ]; then
    print_success "Logs bucket exists"
else
    print_warn "Logs bucket missing - creating..."
    docker exec propwise-minio mc mb local/logs --ignore-existing
fi

# ============================================================
# SUMMARY
# ============================================================
print_header "TEST SUMMARY"

TOTAL=$((PASSED + FAILED))
echo ""
echo -e "  ${GREEN}Passed: $PASSED${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"
echo -e "  Total:  $TOTAL"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ Some tests failed. Check output above.${NC}"
    exit 1
fi

