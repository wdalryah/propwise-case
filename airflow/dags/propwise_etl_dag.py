"""
Propwise ETL Pipeline DAG
==========================

Complete ETL pipeline with two transformation stages:

1. LOG COMPACTION (Python/Pandas)
   - Reads: raw-data bucket (MinIO)
   - Writes: staging bucket (MinIO Parquet)
   - Deduplication, formatting, partitioning

2. CURATED/DWH LOAD (Pure SQL via Trino)
   - Reads: staging bucket (via Trino Hive connector)
   - Writes: PostgreSQL DWH (via Trino PostgreSQL connector)
   - Dimension & Fact loading with surrogate keys

Schedule: Daily at 7pm
Orchestration: Stage 2 triggers only on Stage 1 success
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Default args
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'propwise_etl_pipeline',
    default_args=default_args,
    description='Propwise ETL: Raw → Staging (Python) → DWH (Trino SQL)',
    schedule_interval='0 19 * * *',  # 7pm daily
    catchup=False,
    tags=['etl', 'propwise', 'dwh', 'trino'],
)


# ==========================================
# STAGE 1: LOG COMPACTION (Raw → Staging)
# Python/Pandas transformation
# ==========================================

def run_log_compaction(**context):
    """Execute log compaction for all tables."""
    import sys
    sys.path.insert(0, '/opt/airflow/processing')
    
    from common_code.staging.scripts.log_compaction_tables import run_all
    
    results = run_all(
        config_dir='/opt/airflow/processing/common_code/staging/configs',
        minio_endpoint='minio:9000'
    )
    
    # Log results
    total_rows = 0
    for result in results:
        print(f"Table: {result.get('table')}")
        print(f"  Status: {result.get('status')}")
        print(f"  Rows: {result.get('rows_written', 0)}")
        total_rows += result.get('rows_written', 0)
    
    context['ti'].xcom_push(key='compaction_results', value=results)
    context['ti'].xcom_push(key='compaction_total_rows', value=total_rows)
    
    # Fail if any table failed
    failed = [r for r in results if r.get('status') != 'success']
    if failed:
        raise Exception(f"Log compaction failed for: {[r.get('table') for r in failed]}")
    
    return results


def check_compaction_success(**context):
    """Verify log compaction was successful before proceeding."""
    results = context['ti'].xcom_pull(key='compaction_results', task_ids='log_compaction')
    
    if not results:
        raise Exception("No compaction results found")
    
    success_count = sum(1 for r in results if r.get('status') == 'success')
    total_rows = sum(r.get('rows_written', 0) for r in results)
    
    print(f"Compaction check: {success_count}/{len(results)} tables successful")
    print(f"Total rows in staging: {total_rows}")
    
    if total_rows == 0:
        print("Warning: No rows processed, but continuing...")
    
    return True


# ==========================================
# STAGE 2: CURATED/DWH LOAD (Staging → DWH)
# Pure SQL via Trino
# ==========================================

def run_curated_sql_load(**context):
    """Execute curated layer SQL scripts via Trino."""
    import sys
    sys.path.insert(0, '/opt/airflow/processing')
    
    from common_code.curated.scripts.run_curated_sql import run_curated_load
    
    results = run_curated_load(
        trino_host='trino',
        trino_port=8080,
        config_dir='/opt/airflow/processing/common_code/curated/configs',
        sql_dir='/opt/airflow/processing/common_code/curated/sql'
    )
    
    # Log results
    for result in results:
        print(f"Script: {result.get('script')}")
        print(f"  Status: {result.get('status')}")
        if result.get('error'):
            print(f"  Error: {result.get('error')}")
    
    context['ti'].xcom_push(key='curated_results', value=results)
    
    # Fail if any script failed
    failed = [r for r in results if r.get('status') == 'failed']
    if failed:
        raise Exception(f"Curated SQL failed: {[r.get('script') for r in failed]}")
    
    return results


def log_etl_completion(**context):
    """Log ETL pipeline completion metrics."""
    compaction_rows = context['ti'].xcom_pull(key='compaction_total_rows', task_ids='log_compaction') or 0
    curated_results = context['ti'].xcom_pull(key='curated_results', task_ids='load_curated_dwh') or []
    
    success_scripts = sum(1 for r in curated_results if r.get('status') == 'success')
    
    print("=" * 60)
    print("PROPWISE ETL PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Execution date: {context['ds']}")
    print(f"  Stage 1 (Log Compaction): {compaction_rows:,} rows to staging")
    print(f"  Stage 2 (Curated/DWH): {success_scripts}/{len(curated_results)} SQL scripts executed")
    print("=" * 60)
    
    return {
        "execution_date": context['ds'],
        "staging_rows": compaction_rows,
        "curated_scripts": success_scripts
    }


# ==========================================
# TASK DEFINITIONS
# ==========================================

# Stage 1: Log Compaction (Python)
log_compaction_task = PythonOperator(
    task_id='log_compaction',
    python_callable=run_log_compaction,
    dag=dag,
)

check_compaction_task = PythonOperator(
    task_id='check_compaction',
    python_callable=check_compaction_success,
    dag=dag,
)

# Stage 2: Curated/DWH Load (Trino SQL)
load_curated_task = PythonOperator(
    task_id='load_curated_dwh',
    python_callable=run_curated_sql_load,
    dag=dag,
)

# Completion logging
log_completion_task = PythonOperator(
    task_id='log_completion',
    python_callable=log_etl_completion,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)


# ==========================================
# TASK DEPENDENCIES
# ==========================================

# Stage 1: Raw → Staging
log_compaction_task >> check_compaction_task

# Stage 2: Staging → DWH (triggered by Stage 1 success)
check_compaction_task >> load_curated_task

# Completion
load_curated_task >> log_completion_task
