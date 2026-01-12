#!/usr/bin/env python3
"""
Script to load raw CSV data into MySQL source database.
This simulates having operational data in a source database.

IMPORTANT: No transformations, no deduplication - load data EXACTLY as-is.
All data quality handling happens in the ETL pipeline, not here.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "source_user",
    "password": "source123",
    "database": "propwise_source"
}

# File paths
LEADS_FILE = "/Users/mdalryah/Downloads/Case_Study_Data___Data_Engineering__1_ 1(DE LEADS).csv"
SALES_FILE = "/Users/mdalryah/Downloads/Case_Study_Data___Data_Engineering__1_ 1(DE SALES).csv"


def get_mysql_engine():
    """Create SQLAlchemy engine for MySQL."""
    connection_string = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    return create_engine(connection_string)


def load_leads_data(engine):
    """Load leads CSV into MySQL - NO CHANGES, exactly as-is."""
    print("Loading LEADS data...")
    
    # Read CSV exactly as-is
    df = pd.read_csv(LEADS_FILE, low_memory=False, dtype=str)
    print(f"  - Read {len(df)} rows from CSV")
    
    # Replace NaN with empty string (for MySQL compatibility)
    df = df.fillna('')
    
    # Truncate existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE leads"))
        conn.commit()
    
    # Load to MySQL - NO deduplication, NO transformation
    df.to_sql('leads', engine, if_exists='append', index=False, chunksize=5000)
    print(f"  ✓ Loaded {len(df)} rows into MySQL leads table (unchanged)")
    
    return len(df)


def load_sales_data(engine):
    """Load sales CSV into MySQL - NO CHANGES, exactly as-is."""
    print("\nLoading SALES data...")
    
    # Read CSV exactly as-is
    df = pd.read_csv(SALES_FILE, low_memory=False, dtype=str)
    print(f"  - Read {len(df)} rows from CSV")
    
    # Replace NaN with empty string (for MySQL compatibility)
    df = df.fillna('')
    
    # Truncate existing data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE sales"))
        conn.commit()
    
    # Load to MySQL - NO deduplication, NO transformation
    df.to_sql('sales', engine, if_exists='append', index=False, chunksize=1000)
    print(f"  ✓ Loaded {len(df)} rows into MySQL sales table (unchanged)")
    
    return len(df)


def verify_data(engine):
    """Verify data was loaded correctly."""
    print("\n" + "=" * 60)
    print("DATA VERIFICATION")
    print("=" * 60)
    
    with engine.connect() as conn:
        leads_count = conn.execute(text("SELECT COUNT(*) FROM leads")).scalar()
        sales_count = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
        
        print(f"\n  Leads table: {leads_count:,} rows")
        print(f"  Sales table: {sales_count:,} rows")
        
        # Check duplicates (just for reporting, NOT removing)
        dup_leads = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT id FROM leads GROUP BY id HAVING COUNT(*) > 1
            ) as dups
        """)).scalar()
        print(f"\n  Duplicate lead IDs in source: {dup_leads}")
        
        dup_sales = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT id FROM sales GROUP BY id HAVING COUNT(*) > 1
            ) as dups
        """)).scalar()
        print(f"  Duplicate sale IDs in source: {dup_sales}")


def main():
    print("=" * 60)
    print("Propwise - Loading RAW Source Data into MySQL")
    print("(No transformations - data loaded exactly as-is)")
    print("=" * 60)
    
    try:
        engine = get_mysql_engine()
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Connected to MySQL successfully\n")
        
        # Load data exactly as-is
        leads_count = load_leads_data(engine)
        sales_count = load_sales_data(engine)
        
        # Verify
        verify_data(engine)
        
        print("\n" + "=" * 60)
        print("✓ RAW DATA LOADING COMPLETE!")
        print("=" * 60)
        print(f"  Leads:  {leads_count:,} rows (raw, unchanged)")
        print(f"  Sales:  {sales_count:,} rows (raw, unchanged)")
        print(f"  Total:  {leads_count + sales_count:,} records")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
