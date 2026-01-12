#!/usr/bin/env python3
"""
Ingestion Runner
================

CLI script to run the ingestion engine.

Usage:
    python run_ingestion.py full_load     # Run full load for all tables
    python run_ingestion.py cdc           # Run CDC (incremental) load
    python run_ingestion.py test          # Test connections only
"""

import argparse
import json
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.engine import IngestionEngine


def test_connections(use_docker: bool = False):
    """Test source and target connections."""
    print("=" * 60)
    print("TESTING CONNECTIONS")
    print("=" * 60)
    
    engine = IngestionEngine()
    
    try:
        engine.connect(use_docker_hosts=use_docker)
        
        # Test source
        print("\n✓ MySQL Source:")
        tables = engine.source_connector.get_tables()
        for table in tables:
            count = engine.source_connector.get_row_count(
                engine.task_settings["source"]["connection"]["database"],
                table
            )
            print(f"    - {table}: {count:,} rows")
        
        # Test target
        print("\n✓ MinIO Target:")
        objects = engine.target_connector.list_objects(prefix="propwise/")
        if objects:
            print(f"    Existing objects: {len(objects)}")
            for obj in objects[:5]:
                print(f"    - {obj}")
            if len(objects) > 5:
                print(f"    ... and {len(objects) - 5} more")
        else:
            print("    No existing data in raw-data bucket")
        
        engine.disconnect()
        print("\n✓ All connections successful!")
        return True
        
    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        return False


def run_full_load(use_docker: bool = False):
    """Execute full load ingestion."""
    print("=" * 60)
    print("FULL LOAD INGESTION")
    print("=" * 60)
    
    engine = IngestionEngine()
    
    try:
        engine.connect(use_docker_hosts=use_docker)
        results = engine.run_full_load()
        engine.disconnect()
        
        # Print results summary
        print("\n" + "=" * 60)
        print("RESULTS SUMMARY")
        print("=" * 60)
        for result in results:
            status_icon = "✓" if result["status"] == "success" else "✗"
            source = result.get('source_table', result.get('table', 'unknown'))
            target = result.get('target_table', '')
            print(f"\n{status_icon} {source} -> {target}")
            print(f"    Mode: {result['mode']}")
            print(f"    Status: {result['status']}")
            print(f"    Rows extracted: {result['rows_extracted']:,}")
            print(f"    Rows loaded: {result['rows_loaded']:,}")
            print(f"    Format: {result.get('file_format', 'N/A')}")
            if result.get("audit_columns"):
                print(f"    Audit columns: {', '.join(result['audit_columns'])}")
            if result.get("target_path"):
                print(f"    Target path: {result['target_path']}")
            if result.get("error"):
                print(f"    Error: {result['error']}")
        
        # Save results to file
        results_path = "logs/full_load_results.json"
        os.makedirs("logs", exist_ok=True)
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {results_path}")
        
        return all(r["status"] == "success" for r in results)
        
    except Exception as e:
        print(f"\n✗ Full load failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_cdc(use_docker: bool = False, watermarks_file: str = None):
    """Execute CDC ingestion."""
    print("=" * 60)
    print("CDC (INCREMENTAL) INGESTION")
    print("=" * 60)
    
    # Load watermarks from file if exists
    watermarks = {}
    if watermarks_file and os.path.exists(watermarks_file):
        with open(watermarks_file, "r") as f:
            watermarks = json.load(f)
        print(f"Loaded watermarks from: {watermarks_file}")
    
    engine = IngestionEngine()
    
    try:
        engine.connect(use_docker_hosts=use_docker)
        results = engine.run_cdc(watermarks=watermarks)
        engine.disconnect()
        
        # Print results summary
        print("\n" + "=" * 60)
        print("RESULTS SUMMARY")
        print("=" * 60)
        
        new_watermarks = {}
        for result in results:
            status_icon = "✓" if result["status"] == "success" else "✗"
            source = result.get('source_table', result.get('table', 'unknown'))
            target = result.get('target_table', '')
            table_name = source.split(".")[-1]
            
            print(f"\n{status_icon} {source} -> {target}")
            print(f"    Mode: {result['mode']}")
            print(f"    Status: {result['status']}")
            print(f"    Rows extracted: {result['rows_extracted']:,}")
            print(f"    Rows loaded: {result['rows_loaded']:,}")
            
            # Show operation counts for CDC
            if result.get("inserts") is not None:
                print(f"    Operations: I={result.get('inserts', 0)}, U={result.get('updates', 0)}, D={result.get('deletes', 0)}")
            
            print(f"    Previous watermark: {result.get('previous_watermark')}")
            print(f"    New watermark: {result.get('new_watermark')}")
            
            if result.get("target_path"):
                print(f"    Target path: {result['target_path']}")
            
            if result.get("new_watermark"):
                new_watermarks[table_name] = result["new_watermark"]
        
        # Save new watermarks
        watermarks_path = "logs/watermarks.json"
        os.makedirs("logs", exist_ok=True)
        with open(watermarks_path, "w") as f:
            json.dump(new_watermarks, f, indent=2)
        print(f"\nWatermarks saved to: {watermarks_path}")
        
        # Save results to file
        results_path = "logs/cdc_results.json"
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {results_path}")
        
        return all(r["status"] == "success" for r in results)
        
    except Exception as e:
        print(f"\n✗ CDC failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description="Propwise Data Ingestion Engine")
    parser.add_argument(
        "command",
        choices=["test", "full_load", "cdc"],
        help="Command to run"
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Use Docker internal hostnames"
    )
    parser.add_argument(
        "--watermarks",
        type=str,
        default="logs/watermarks.json",
        help="Path to watermarks file for CDC"
    )
    
    args = parser.parse_args()
    
    if args.command == "test":
        success = test_connections(use_docker=args.docker)
    elif args.command == "full_load":
        success = run_full_load(use_docker=args.docker)
    elif args.command == "cdc":
        success = run_cdc(use_docker=args.docker, watermarks_file=args.watermarks)
    else:
        print(f"Unknown command: {args.command}")
        success = False
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

