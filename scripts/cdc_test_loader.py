#!/usr/bin/env python3
"""
CDC Test Loader
===============

Simple script to push test records to MySQL for CDC testing.
Simulates Insert (I), Update (U), and Delete (D) operations.
"""

import argparse
from datetime import datetime
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


def get_engine():
    """Create SQLAlchemy engine."""
    connection_string = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    return create_engine(connection_string)


def insert_lead(engine, lead_id: str = None):
    """Insert a new lead record."""
    lead_id = lead_id or f"CDC_TEST_{datetime.now().strftime('%H%M%S')}"
    now = datetime.now().strftime("%m/%d/%Y %H:%M")
    
    query = text("""
        INSERT INTO leads (id, date_of_last_request, buyer, seller, best_time_to_call, 
                          budget, created_at, updated_at, user_id, location, 
                          date_of_last_contact, status_name, commercial, merged, 
                          area_id, compound_id, developer_id, meeting_flag, do_not_call,
                          lead_type_id, customer_id, method_of_contact, lead_source, 
                          campaign, lead_type)
        VALUES (:id, :date, 'TRUE', 'FALSE', 'Morning', '500000', :created, :updated,
                '9999', 'CDC Test Location', :contact, 'New', 'FALSE', '', '', '', '',
                '', 'FALSE', '1', '999999', 'api_test', 'cdc_test', 'cdc_campaign', 'Primary')
    """)
    
    with engine.connect() as conn:
        conn.execute(query, {
            "id": lead_id,
            "date": now,
            "created": now,
            "updated": now,
            "contact": now
        })
        conn.commit()
    
    print(f"✓ INSERT: Lead '{lead_id}' created")
    return lead_id


def update_lead(engine, lead_id: str):
    """Update an existing lead record."""
    now = datetime.now().strftime("%m/%d/%Y %H:%M")
    
    query = text("""
        UPDATE leads 
        SET status_name = 'Updated via CDC',
            budget = '750000',
            updated_at = :updated,
            date_of_last_contact = :contact,
            location = 'CDC Updated Location'
        WHERE id = :id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            "id": lead_id,
            "updated": now,
            "contact": now
        })
        conn.commit()
    
    if result.rowcount > 0:
        print(f"✓ UPDATE: Lead '{lead_id}' updated (status='Updated via CDC', budget='750000')")
    else:
        print(f"✗ UPDATE: Lead '{lead_id}' not found")
    return result.rowcount


def delete_lead(engine, lead_id: str):
    """Delete a lead record (soft delete - we'll mark it)."""
    now = datetime.now().strftime("%m/%d/%Y %H:%M")
    
    # For CDC purposes, we'll do a soft delete by updating status
    # In real scenario, you might have an is_deleted flag
    query = text("""
        UPDATE leads 
        SET status_name = 'DELETED',
            updated_at = :updated,
            do_not_call = 'TRUE'
        WHERE id = :id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {
            "id": lead_id,
            "updated": now
        })
        conn.commit()
    
    if result.rowcount > 0:
        print(f"✓ DELETE (soft): Lead '{lead_id}' marked as DELETED")
    else:
        print(f"✗ DELETE: Lead '{lead_id}' not found")
    return result.rowcount


def run_cdc_test_scenario(engine):
    """
    Run a complete CDC test scenario with I, U, D operations.
    """
    print("=" * 60)
    print("CDC TEST SCENARIO")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Generate unique IDs for test
    timestamp = datetime.now().strftime("%H%M%S")
    
    # Test 1: INSERT - New record
    print("1. Testing INSERT operation...")
    insert_id = f"CDC_INSERT_{timestamp}"
    insert_lead(engine, insert_id)
    
    # Test 2: UPDATE - Modify existing record
    print("\n2. Testing UPDATE operation...")
    update_id = f"CDC_UPDATE_{timestamp}"
    insert_lead(engine, update_id)  # First create it
    update_lead(engine, update_id)  # Then update it
    
    # Test 3: DELETE - Soft delete record
    print("\n3. Testing DELETE operation...")
    delete_id = f"CDC_DELETE_{timestamp}"
    insert_lead(engine, delete_id)  # First create it
    delete_lead(engine, delete_id)  # Then delete it
    
    print("\n" + "=" * 60)
    print("CDC TEST RECORDS CREATED")
    print("=" * 60)
    print(f"  INSERT record: {insert_id}")
    print(f"  UPDATE record: {update_id}")
    print(f"  DELETE record: {delete_id}")
    print("\nNow run CDC ingestion to capture these changes!")
    print("Command: python ingestion/run_ingestion.py cdc")
    print("=" * 60)
    
    return {
        "insert_id": insert_id,
        "update_id": update_id,
        "delete_id": delete_id
    }


def show_cdc_records(engine):
    """Show the CDC test records in the database."""
    query = text("""
        SELECT id, status_name, budget, location, updated_at, do_not_call
        FROM leads 
        WHERE id LIKE 'CDC_%'
        ORDER BY updated_at DESC
        LIMIT 10
    """)
    
    print("\n" + "=" * 60)
    print("CDC TEST RECORDS IN DATABASE")
    print("=" * 60)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        if not rows:
            print("No CDC test records found")
            return
        
        print(f"{'ID':<25} {'Status':<20} {'Budget':<10} {'Deleted':<8}")
        print("-" * 70)
        for row in rows:
            print(f"{row[0]:<25} {row[1]:<20} {row[2] or 'N/A':<10} {row[5]:<8}")


def cleanup_cdc_records(engine):
    """Remove all CDC test records."""
    query = text("DELETE FROM leads WHERE id LIKE 'CDC_%'")
    
    with engine.connect() as conn:
        result = conn.execute(query)
        conn.commit()
    
    print(f"✓ Cleaned up {result.rowcount} CDC test records")


def main():
    parser = argparse.ArgumentParser(description="CDC Test Loader")
    parser.add_argument(
        "command",
        choices=["insert", "update", "delete", "test", "show", "cleanup"],
        help="Command to run"
    )
    parser.add_argument(
        "--id",
        type=str,
        help="Lead ID for insert/update/delete operations"
    )
    
    args = parser.parse_args()
    engine = get_engine()
    
    try:
        if args.command == "insert":
            insert_lead(engine, args.id)
        elif args.command == "update":
            if not args.id:
                print("Error: --id required for update")
                sys.exit(1)
            update_lead(engine, args.id)
        elif args.command == "delete":
            if not args.id:
                print("Error: --id required for delete")
                sys.exit(1)
            delete_lead(engine, args.id)
        elif args.command == "test":
            run_cdc_test_scenario(engine)
        elif args.command == "show":
            show_cdc_records(engine)
        elif args.command == "cleanup":
            cleanup_cdc_records(engine)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

