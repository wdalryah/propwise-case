"""
Propwise Data Quality DAG
=========================

Comprehensive data quality pipeline with:
1. Schema contract validation (soft contract)
2. Quality dimension metrics (completeness, integrity, uniqueness, validity, timeliness)
3. Rejection handling and quarantine
4. PII detection and classification
5. Great Expectations validation
6. Data profiling
7. Alert generation for all quality issues

Schedule: Daily at 8pm (after ETL at 7pm)

Components:
- Schema Contract Validator
- Dimension Metrics Calculator
- Rejection Handler (quarantine bucket)
- PII Detector
- Great Expectations validation
- Quality Alert Manager
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
import json

# Default args
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG definition
dag = DAG(
    'propwise_quality_checks',
    default_args=default_args,
    description='Data Quality Checks with Great Expectations',
    schedule_interval='0 20 * * *',  # 8pm daily (after ETL at 7pm)
    catchup=False,
    tags=['quality', 'great-expectations', 'monitoring'],
)


# ==========================================
# QUALITY CHECK FUNCTIONS
# ==========================================

# ==========================================
# SCHEMA CONTRACT VALIDATION
# ==========================================

def validate_schema_contract(**context):
    """Validate source schema against expected staging schema."""
    sys.path.insert(0, '/opt/airflow')
    
    from quality_framework import SchemaContract, QualityAlertManager
    from great_expectations.gx_profiler import DataProfiler
    
    # Load data from MinIO
    profiler = DataProfiler(minio_endpoint='minio:9000')
    
    contract = SchemaContract(
        staging_config_dir='/opt/airflow/processing/common_code/staging/configs'
    )
    alert_manager = QualityAlertManager()
    
    results = {}
    all_alerts = []
    
    for table in ["leads", "sales"]:
        # Load raw data
        df = profiler.load_from_minio("raw-data", f"raw_{table}/")
        
        if df.empty:
            results[table] = {"error": "No data found"}
            continue
        
        # Validate schema
        validation = contract.validate_schema(df, table)
        results[table] = {
            "has_drift": validation["has_drift"],
            "integrity_score": validation["integrity_score"],
            "source_columns": len(validation["source_columns"]),
            "expected_columns": validation["expected_column_count"],
            "new_columns": validation["drift_details"]["new_columns"],
            "missing_columns": validation["drift_details"]["missing_columns"]
        }
        
        # Generate alerts
        alerts = alert_manager.check_schema_drift(validation)
        all_alerts.extend(alerts)
        
        status = "⚠️ DRIFT" if validation["has_drift"] else "✅ OK"
        print(f"Schema {table}: {status} | Integrity: {validation['integrity_score']:.1f}%")
    
    profiler.close()
    contract.close()
    alert_manager.close()
    
    context['ti'].xcom_push(key='schema_results', value=results)
    context['ti'].xcom_push(key='schema_alerts', value=all_alerts)
    
    return results


def calculate_dimension_metrics(**context):
    """Calculate quality dimension metrics for all tables."""
    sys.path.insert(0, '/opt/airflow')
    
    from quality_framework import DimensionMetrics, QualityAlertManager
    from great_expectations.gx_profiler import DataProfiler
    
    profiler = DataProfiler(minio_endpoint='minio:9000')
    metrics = DimensionMetrics()
    alert_manager = QualityAlertManager()
    
    results = {"raw": {}, "staging": {}}
    all_alerts = []
    
    for table in ["leads", "sales"]:
        # Raw data metrics
        raw_df = profiler.load_from_minio("raw-data", f"raw_{table}/")
        if not raw_df.empty:
            raw_metrics = metrics.calculate_all_dimensions(raw_df, table, "raw")
            results["raw"][table] = {
                "overall_score": raw_metrics["overall_quality_score"],
                "completeness": raw_metrics["dimensions"]["completeness"]["score"],
                "uniqueness": raw_metrics["dimensions"]["uniqueness"]["score"],
                "validity": raw_metrics["dimensions"]["validity"]["score"]
            }
            all_alerts.extend(alert_manager.check_dimension_scores(raw_metrics))
        
        # Staging data metrics
        staging_df = profiler.load_from_minio("staging", f"processed_{table}/")
        if not staging_df.empty:
            staging_metrics = metrics.calculate_all_dimensions(staging_df, table, "staging")
            results["staging"][table] = {
                "overall_score": staging_metrics["overall_quality_score"],
                "completeness": staging_metrics["dimensions"]["completeness"]["score"],
                "uniqueness": staging_metrics["dimensions"]["uniqueness"]["score"],
                "validity": staging_metrics["dimensions"]["validity"]["score"]
            }
            all_alerts.extend(alert_manager.check_dimension_scores(staging_metrics))
        
        print(f"Dimensions {table}: Raw={results['raw'].get(table, {}).get('overall_score', 'N/A')}% | "
              f"Staging={results['staging'].get(table, {}).get('overall_score', 'N/A')}%")
    
    profiler.close()
    metrics.close()
    alert_manager.close()
    
    context['ti'].xcom_push(key='dimension_results', value=results)
    context['ti'].xcom_push(key='dimension_alerts', value=all_alerts)
    
    return results


def run_rejection_scan(**context):
    """Scan data for violations and quarantine rejected records."""
    sys.path.insert(0, '/opt/airflow')
    
    from quality_framework import RejectionHandler, QualityAlertManager
    from great_expectations.gx_profiler import DataProfiler
    from datetime import datetime
    
    profiler = DataProfiler(minio_endpoint='minio:9000')
    handler = RejectionHandler(minio_endpoint='minio:9000')
    alert_manager = QualityAlertManager()
    
    results = {}
    all_alerts = []
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for table in ["leads", "sales"]:
        df = profiler.load_from_minio("raw-data", f"raw_{table}/")
        
        if df.empty:
            continue
        
        # Process and separate clean/rejected
        clean_df, rejected_df, summary = handler.process_dataframe(df, table)
        
        results[table] = {
            "total": summary["total_records"],
            "clean": summary["clean_records"],
            "rejected": summary["rejected_records"],
            "rejection_rate": summary["rejection_rate"],
            "violations": list(summary["violations"].keys())
        }
        
        # Quarantine rejected records
        if not rejected_df.empty:
            quarantine_path = handler.quarantine_records(rejected_df, table, batch_id)
            results[table]["quarantine_path"] = quarantine_path
        
        # Check alerts
        all_alerts.extend(alert_manager.check_rejection_rate(summary))
        
        print(f"Rejection {table}: {summary['rejected_records']}/{summary['total_records']} "
              f"({summary['rejection_rate']:.2f}%)")
    
    profiler.close()
    handler.close()
    alert_manager.close()
    
    context['ti'].xcom_push(key='rejection_results', value=results)
    context['ti'].xcom_push(key='rejection_alerts', value=all_alerts)
    
    return results


def scan_for_pii(**context):
    """Scan data for PII and update data classification."""
    sys.path.insert(0, '/opt/airflow')
    
    from quality_framework import PIIDetector, QualityAlertManager
    from great_expectations.gx_profiler import DataProfiler
    
    profiler = DataProfiler(minio_endpoint='minio:9000')
    detector = PIIDetector()
    alert_manager = QualityAlertManager()
    
    results = {}
    all_alerts = []
    
    for table in ["leads", "sales"]:
        df = profiler.load_from_minio("raw-data", f"raw_{table}/")
        
        if df.empty:
            continue
        
        # Scan for PII
        classification = detector.scan_dataframe(df, table)
        
        results[table] = {
            "columns_scanned": classification["summary"]["total_columns"],
            "columns_with_pii": classification["summary"]["columns_with_pii"],
            "pii_types": classification["summary"]["pii_types_found"]
        }
        
        # Check alerts
        all_alerts.extend(alert_manager.check_pii_detection(classification))
        
        pii_status = "⚠️ PII FOUND" if classification["summary"]["columns_with_pii"] > 0 else "✅ CLEAN"
        print(f"PII Scan {table}: {pii_status} | Types: {classification['summary']['pii_types_found']}")
    
    profiler.close()
    detector.close()
    alert_manager.close()
    
    context['ti'].xcom_push(key='pii_results', value=results)
    context['ti'].xcom_push(key='pii_alerts', value=all_alerts)
    
    return results


# ==========================================
# PROFILING FUNCTIONS
# ==========================================

def profile_raw_data(**context):
    """Profile raw data tables."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_profiler import DataProfiler
    
    profiler = DataProfiler(minio_endpoint='minio:9000')
    
    results = {}
    for table in ["leads", "sales"]:
        profile = profiler.profile_raw_table(table, persist=True)
        results[table] = {
            "row_count": profile.get("row_count", 0),
            "column_count": profile.get("column_count", 0),
            "completeness": profile.get("summary", {}).get("completeness_score", 0),
            "columns_with_nulls": profile.get("summary", {}).get("columns_with_nulls", 0)
        }
        print(f"📊 Profiled {table}: {results[table]['row_count']} rows, {results[table]['completeness']}% complete")
    
    profiler.close()
    context['ti'].xcom_push(key='raw_profiles', value=results)
    return results


def profile_staging_data(**context):
    """Profile staging data tables."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_profiler import DataProfiler
    
    profiler = DataProfiler(minio_endpoint='minio:9000')
    
    results = {}
    for table in ["leads", "sales"]:
        profile = profiler.profile_staging_table(table, persist=True)
        results[table] = {
            "row_count": profile.get("row_count", 0),
            "column_count": profile.get("column_count", 0),
            "completeness": profile.get("summary", {}).get("completeness_score", 0)
        }
        print(f"📊 Profiled staging {table}: {results[table]['row_count']} rows")
    
    profiler.close()
    context['ti'].xcom_push(key='staging_profiles', value=results)
    return results


# ==========================================
# QUALITY CHECK FUNCTIONS
# ==========================================

def validate_raw_leads(**context):
    """Validate leads data in raw bucket."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_runner import GXRunner
    
    runner = GXRunner(
        gx_root_dir='/opt/airflow/great_expectations',
        minio_endpoint='minio:9000'
    )
    
    success, result = runner.run_checkpoint(
        table_name='leads',
        stage='raw',
        fail_on_error=False
    )
    
    # Store results
    context['ti'].xcom_push(key='leads_raw_result', value=result)
    context['ti'].xcom_push(key='leads_raw_success', value=success)
    
    # Log summary
    stats = result.get('statistics', {})
    print(f"Leads Raw Validation:")
    print(f"  Rows: {result.get('row_count', 0)}")
    print(f"  Expectations: {stats.get('evaluated_expectations', 0)}")
    print(f"  Passed: {stats.get('successful_expectations', 0)}")
    print(f"  Success Rate: {stats.get('success_percent', 0)}%")
    
    return result


def validate_raw_sales(**context):
    """Validate sales data in raw bucket."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_runner import GXRunner
    
    runner = GXRunner(
        gx_root_dir='/opt/airflow/great_expectations',
        minio_endpoint='minio:9000'
    )
    
    success, result = runner.run_checkpoint(
        table_name='sales',
        stage='raw',
        fail_on_error=False
    )
    
    context['ti'].xcom_push(key='sales_raw_result', value=result)
    context['ti'].xcom_push(key='sales_raw_success', value=success)
    
    stats = result.get('statistics', {})
    print(f"Sales Raw Validation:")
    print(f"  Rows: {result.get('row_count', 0)}")
    print(f"  Expectations: {stats.get('evaluated_expectations', 0)}")
    print(f"  Passed: {stats.get('successful_expectations', 0)}")
    print(f"  Success Rate: {stats.get('success_percent', 0)}%")
    
    return result


def validate_staging_leads(**context):
    """Validate leads data in staging bucket."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_runner import GXRunner
    
    runner = GXRunner(
        gx_root_dir='/opt/airflow/great_expectations',
        minio_endpoint='minio:9000'
    )
    
    success, result = runner.run_checkpoint(
        table_name='leads',
        stage='staging',
        fail_on_error=False
    )
    
    context['ti'].xcom_push(key='leads_staging_result', value=result)
    context['ti'].xcom_push(key='leads_staging_success', value=success)
    
    return result


def validate_staging_sales(**context):
    """Validate sales data in staging bucket."""
    sys.path.insert(0, '/opt/airflow')
    
    from great_expectations.gx_runner import GXRunner
    
    runner = GXRunner(
        gx_root_dir='/opt/airflow/great_expectations',
        minio_endpoint='minio:9000'
    )
    
    success, result = runner.run_checkpoint(
        table_name='sales',
        stage='staging',
        fail_on_error=False
    )
    
    context['ti'].xcom_push(key='sales_staging_result', value=result)
    context['ti'].xcom_push(key='sales_staging_success', value=success)
    
    return result


# ==========================================
# OBSERVABILITY FUNCTIONS
# ==========================================

def record_quality_metrics(**context):
    """Record quality metrics from validation results."""
    sys.path.insert(0, '/opt/airflow')
    
    try:
        from observability import MetricsCollector
        metrics = MetricsCollector(backend='postgres')
    except Exception as e:
        print(f"Could not initialize metrics collector: {e}")
        metrics = None
    
    # Collect all results
    tables = ['leads', 'sales']
    stages = ['raw', 'staging']
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "tables": {}
    }
    
    for table in tables:
        summary["tables"][table] = {}
        for stage in stages:
            key = f'{table}_{stage}_result'
            result = context['ti'].xcom_pull(key=key)
            
            if result:
                stats = result.get('statistics', {})
                row_count = result.get('row_count', 0)
                success_rate = stats.get('success_percent', 0)
                
                summary["tables"][table][stage] = {
                    "row_count": row_count,
                    "success_rate": success_rate,
                    "passed": stats.get('successful_expectations', 0),
                    "failed": stats.get('unsuccessful_expectations', 0)
                }
                
                # Record metrics
                if metrics:
                    metrics.record_row_count(table, stage, row_count)
                    metrics.record_gauge(
                        'dq_completeness_score',
                        success_rate,
                        {'table_name': table}
                    )
    
    print("\n" + "=" * 60)
    print("QUALITY METRICS SUMMARY")
    print("=" * 60)
    print(json.dumps(summary, indent=2))
    
    context['ti'].xcom_push(key='quality_summary', value=summary)
    
    if metrics:
        metrics.close()
    
    return summary


def check_for_alerts(**context):
    """Check results and create alerts for failures."""
    sys.path.insert(0, '/opt/airflow')
    
    try:
        from observability import AlertManager
        alerts = AlertManager()
    except Exception as e:
        print(f"Could not initialize alert manager: {e}")
        alerts = None
    
    tables = ['leads', 'sales']
    stages = ['raw', 'staging']
    alerts_created = []
    
    for table in tables:
        for stage in stages:
            success_key = f'{table}_{stage}_success'
            result_key = f'{table}_{stage}_result'
            
            success = context['ti'].xcom_pull(key=success_key)
            result = context['ti'].xcom_pull(key=result_key)
            
            if result and not success:
                blocking_failures = result.get('blocking_failures', [])
                stats = result.get('statistics', {})
                
                if blocking_failures and alerts:
                    alert_id = alerts.alert_quality_violation(
                        table_name=table,
                        check_name=f"{stage}_validation",
                        details=f"Failed {len(blocking_failures)} blocking checks. "
                               f"Success rate: {stats.get('success_percent', 0)}%",
                        severity="critical" if stage == "staging" else "warning"
                    )
                    alerts_created.append({
                        "table": table,
                        "stage": stage,
                        "alert_id": alert_id
                    })
                    print(f"⚠️ Alert created for {table}/{stage}")
    
    if not alerts_created:
        print("✅ No quality alerts - all validations passed or non-blocking")
    
    if alerts:
        alerts.close()
    
    return alerts_created


def run_schema_crawler(**context):
    """Run schema crawler to discover and register schemas from staging."""
    sys.path.insert(0, '/opt/airflow')
    
    from quality_framework import SchemaRegistry
    
    registry = SchemaRegistry(minio_endpoint='minio:9000')
    
    results = registry.run_crawler(bucket="staging", auto_register=True)
    
    print("\n" + "=" * 60)
    print("SCHEMA CRAWLER RESULTS (Glue-like Catalog)")
    print("=" * 60)
    print(f"Tables discovered: {results['tables_discovered']}")
    print(f"Tables updated: {results['tables_updated']}")
    print(f"Tables unchanged: {results['tables_unchanged']}")
    print(f"Duration: {results['duration_seconds']:.2f}s")
    
    for table in results.get("tables", []):
        status_icon = {"new": "🆕", "updated": "📝", "unchanged": "✓"}.get(table["status"], "?")
        print(f"  {status_icon} {table['table_name']}: v{table['version']} ({table['column_count']} columns)")
    
    if results.get("errors"):
        print("\nErrors:")
        for err in results["errors"]:
            print(f"  ❌ {err['table_name']}: {err['error']}")
    
    registry.close()
    context['ti'].xcom_push(key='crawler_results', value=results)
    
    return results


def generate_quality_report(**context):
    """Generate and log quality report."""
    summary = context['ti'].xcom_pull(key='quality_summary', task_ids='record_metrics')
    alerts = context['ti'].xcom_pull(task_ids='check_alerts')
    
    print("\n" + "=" * 70)
    print("PROPWISE DATA QUALITY REPORT")
    print(f"Date: {context['ds']}")
    print("=" * 70)
    
    if summary:
        for table, stages in summary.get('tables', {}).items():
            print(f"\n📊 {table.upper()}")
            for stage, stats in stages.items():
                status = "✅" if stats.get('failed', 0) == 0 else "⚠️"
                print(f"   {stage}: {status} {stats.get('row_count', 0):,} rows, "
                      f"{stats.get('success_rate', 0):.1f}% pass rate")
    
    if alerts:
        print(f"\n🚨 ALERTS CREATED: {len(alerts)}")
        for alert in alerts:
            print(f"   - {alert.get('table')}/{alert.get('stage')} (ID: {alert.get('alert_id')})")
    else:
        print("\n✅ No alerts created")
    
    print("\n" + "=" * 70)
    
    return {
        "date": context['ds'],
        "summary": summary,
        "alerts": alerts
    }


# ==========================================
# TASK DEFINITIONS
# ==========================================

# Schema Contract Validation
schema_contract_task = PythonOperator(
    task_id='validate_schema_contract',
    python_callable=validate_schema_contract,
    dag=dag,
)

# Dimension Metrics
dimension_metrics_task = PythonOperator(
    task_id='calculate_dimension_metrics',
    python_callable=calculate_dimension_metrics,
    dag=dag,
)

# Rejection Scan
rejection_scan_task = PythonOperator(
    task_id='run_rejection_scan',
    python_callable=run_rejection_scan,
    dag=dag,
)

# PII Detection
pii_scan_task = PythonOperator(
    task_id='scan_for_pii',
    python_callable=scan_for_pii,
    dag=dag,
)

# Profiling tasks
profile_raw_task = PythonOperator(
    task_id='profile_raw_data',
    python_callable=profile_raw_data,
    dag=dag,
)

profile_staging_task = PythonOperator(
    task_id='profile_staging_data',
    python_callable=profile_staging_data,
    dag=dag,
)

# Raw data validation
validate_leads_raw_task = PythonOperator(
    task_id='validate_leads_raw',
    python_callable=validate_raw_leads,
    dag=dag,
)

validate_sales_raw_task = PythonOperator(
    task_id='validate_sales_raw',
    python_callable=validate_raw_sales,
    dag=dag,
)

# Staging data validation
validate_leads_staging_task = PythonOperator(
    task_id='validate_leads_staging',
    python_callable=validate_staging_leads,
    dag=dag,
)

validate_sales_staging_task = PythonOperator(
    task_id='validate_staging_sales',
    python_callable=validate_staging_sales,
    dag=dag,
)

# Observability
record_metrics_task = PythonOperator(
    task_id='record_metrics',
    python_callable=record_quality_metrics,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

check_alerts_task = PythonOperator(
    task_id='check_alerts',
    python_callable=check_for_alerts,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_quality_report,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Schema Crawler (Glue-like)
schema_crawler_task = PythonOperator(
    task_id='run_schema_crawler',
    python_callable=run_schema_crawler,
    dag=dag,
)


# ==========================================
# TASK DEPENDENCIES
# ==========================================

# Phase 1: Schema Contract & Initial Scans (parallel)
[schema_contract_task, pii_scan_task, rejection_scan_task] >> profile_raw_task

# Phase 2: Profile raw data, then run GX validations
profile_raw_task >> [validate_leads_raw_task, validate_sales_raw_task]

# Phase 3: Calculate dimension metrics after raw validation
[validate_leads_raw_task, validate_sales_raw_task] >> dimension_metrics_task

# Phase 4: Profile staging and validate
dimension_metrics_task >> profile_staging_task
profile_staging_task >> [validate_leads_staging_task, validate_sales_staging_task]

# Phase 5: Run Schema Crawler to update catalog (after staging validation)
[validate_leads_staging_task, validate_sales_staging_task] >> schema_crawler_task

# Phase 6: Record all metrics
schema_crawler_task >> record_metrics_task

# Phase 7: Check alerts and generate report
record_metrics_task >> check_alerts_task >> generate_report_task

