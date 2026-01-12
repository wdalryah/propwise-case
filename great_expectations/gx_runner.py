"""
Great Expectations Runner
=========================

Provides easy-to-use functions for running GX validations against
MinIO data, designed for Airflow integration.

Features:
- Load data from MinIO raw/staging buckets
- Run expectation suites
- Return validation results for Airflow decisions
- Persist results to metadata store
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import pandas as pd
from io import BytesIO

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import FileDataContext
from great_expectations.core.expectation_suite import ExpectationSuite

from minio import Minio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GXRunner:
    """
    Runner for Great Expectations validations.
    
    Integrates with MinIO for data loading and provides
    Airflow-friendly validation methods.
    """
    
    def __init__(
        self,
        gx_root_dir: Optional[str] = None,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin"
    ):
        """
        Initialize GX Runner.
        
        Args:
            gx_root_dir: Path to great_expectations directory
            minio_endpoint: MinIO endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
        """
        self.gx_root_dir = gx_root_dir or str(
            Path(__file__).parent
        )
        
        # Initialize MinIO client
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Initialize GX context
        self._context = None
    
    @property
    def context(self) -> FileDataContext:
        """Get or create GX Data Context."""
        if self._context is None:
            try:
                self._context = gx.get_context(
                    context_root_dir=self.gx_root_dir
                )
            except Exception as e:
                logger.warning(f"Could not load existing context: {e}")
                # Create a new ephemeral context
                self._context = gx.get_context(mode="ephemeral")
        return self._context
    
    # =========================================
    # DATA LOADING FROM MINIO
    # =========================================
    
    def load_from_minio(
        self,
        bucket: str,
        prefix: str,
        file_format: str = "csv"
    ) -> pd.DataFrame:
        """
        Load data from MinIO bucket.
        
        Args:
            bucket: MinIO bucket name
            prefix: Object prefix/path
            file_format: 'csv' or 'parquet'
            
        Returns:
            DataFrame with loaded data
        """
        logger.info(f"Loading data from minio://{bucket}/{prefix}")
        
        # List objects with prefix
        objects = list(self.minio_client.list_objects(
            bucket, prefix=prefix, recursive=True
        ))
        
        if not objects:
            logger.warning(f"No objects found at {bucket}/{prefix}")
            return pd.DataFrame()
        
        dfs = []
        for obj in objects:
            if obj.is_dir:
                continue
            
            obj_name = obj.object_name
            
            # Skip non-data files
            if not (obj_name.endswith('.csv') or obj_name.endswith('.parquet')):
                continue
            
            try:
                response = self.minio_client.get_object(bucket, obj_name)
                data = response.read()
                response.close()
                response.release_conn()
                
                if obj_name.endswith('.parquet'):
                    df = pd.read_parquet(BytesIO(data))
                else:
                    df = pd.read_csv(BytesIO(data))
                
                dfs.append(df)
                logger.debug(f"  Loaded {len(df)} rows from {obj_name}")
                
            except Exception as e:
                logger.error(f"  Failed to load {obj_name}: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(combined)} total rows from {len(dfs)} files")
        
        return combined
    
    def load_raw_data(self, table_name: str) -> pd.DataFrame:
        """Load raw data for a table from the raw-data bucket."""
        return self.load_from_minio(
            bucket="raw-data",
            prefix=f"raw_{table_name}/",
            file_format="csv"
        )
    
    def load_staging_data(self, table_name: str) -> pd.DataFrame:
        """Load staging data for a table from the staging bucket."""
        return self.load_from_minio(
            bucket="staging",
            prefix=f"processed_{table_name}/",
            file_format="parquet"
        )
    
    # =========================================
    # EXPECTATION SUITE MANAGEMENT
    # =========================================
    
    def load_expectation_suite(self, suite_name: str) -> ExpectationSuite:
        """Load an expectation suite from JSON file."""
        suite_path = Path(self.gx_root_dir) / "expectations" / f"{suite_name}.json"
        
        if suite_path.exists():
            with open(suite_path, 'r') as f:
                suite_dict = json.load(f)
            
            # Create ExpectationSuite from dict
            suite = ExpectationSuite(
                expectation_suite_name=suite_dict.get("expectation_suite_name", suite_name),
                expectations=suite_dict.get("expectations", []),
                meta=suite_dict.get("meta", {})
            )
            return suite
        else:
            raise FileNotFoundError(f"Suite not found: {suite_path}")
    
    # =========================================
    # VALIDATION METHODS
    # =========================================
    
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        suite_name: str,
        run_name: Optional[str] = None
    ) -> Dict:
        """
        Validate a DataFrame against an expectation suite.
        
        Args:
            df: DataFrame to validate
            suite_name: Name of expectation suite
            run_name: Optional run name for tracking
            
        Returns:
            Dict with validation results
        """
        if df.empty:
            return {
                "success": False,
                "error": "Empty DataFrame",
                "statistics": {"evaluated_expectations": 0}
            }
        
        run_name = run_name or f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Validating {len(df)} rows against {suite_name}")
        
        try:
            # Load suite
            suite = self.load_expectation_suite(suite_name)
            
            # Create a validator using the context
            validator = self.context.sources.pandas_default.read_dataframe(df)
            
            # Run each expectation manually and collect results
            results = []
            for expectation in suite.expectations:
                exp_type = expectation.get("expectation_type")
                kwargs = expectation.get("kwargs", {})
                meta = expectation.get("meta", {})
                
                try:
                    # Get the expectation method from validator
                    exp_method = getattr(validator, exp_type, None)
                    if exp_method:
                        result = exp_method(**kwargs)
                        results.append({
                            "expectation_type": exp_type,
                            "success": result.success,
                            "kwargs": kwargs,
                            "meta": meta,
                            "result": {
                                "observed_value": getattr(result.result, 'observed_value', None),
                                "element_count": getattr(result.result, 'element_count', len(df)),
                                "unexpected_count": getattr(result.result, 'unexpected_count', 0)
                            }
                        })
                except Exception as e:
                    results.append({
                        "expectation_type": exp_type,
                        "success": False,
                        "kwargs": kwargs,
                        "meta": meta,
                        "error": str(e)
                    })
            
            # Calculate statistics
            passed = sum(1 for r in results if r.get("success", False))
            failed = len(results) - passed
            
            # Check for blocking failures
            blocking_failures = [
                r for r in results 
                if not r.get("success", False) and r.get("meta", {}).get("is_blocking", False)
            ]
            
            return {
                "success": len(blocking_failures) == 0 and failed == 0,
                "run_name": run_name,
                "suite_name": suite_name,
                "timestamp": datetime.now().isoformat(),
                "statistics": {
                    "evaluated_expectations": len(results),
                    "successful_expectations": passed,
                    "unsuccessful_expectations": failed,
                    "success_percent": round(passed / len(results) * 100, 2) if results else 0
                },
                "results": results,
                "blocking_failures": blocking_failures,
                "has_blocking_failures": len(blocking_failures) > 0
            }
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return {
                "success": False,
                "error": str(e),
                "suite_name": suite_name,
                "timestamp": datetime.now().isoformat()
            }
    
    def validate_raw_table(
        self,
        table_name: str,
        suite_name: Optional[str] = None
    ) -> Dict:
        """
        Validate a raw table from MinIO.
        
        Args:
            table_name: Table name (e.g., 'leads', 'sales')
            suite_name: Suite name (defaults to {table_name}_suite)
            
        Returns:
            Validation results dict
        """
        suite_name = suite_name or f"{table_name}_suite"
        
        # Load data
        df = self.load_raw_data(table_name)
        
        if df.empty:
            return {
                "success": False,
                "error": f"No data found for {table_name}",
                "table_name": table_name
            }
        
        # Validate
        result = self.validate_dataframe(df, suite_name)
        result["table_name"] = table_name
        result["row_count"] = len(df)
        
        return result
    
    def validate_staging_table(
        self,
        table_name: str,
        suite_name: Optional[str] = None
    ) -> Dict:
        """Validate a staging table from MinIO."""
        suite_name = suite_name or f"{table_name}_suite"
        
        df = self.load_staging_data(table_name)
        
        if df.empty:
            return {
                "success": False,
                "error": f"No staging data found for {table_name}",
                "table_name": table_name
            }
        
        result = self.validate_dataframe(df, suite_name)
        result["table_name"] = table_name
        result["row_count"] = len(df)
        result["stage"] = "staging"
        
        return result
    
    # =========================================
    # AIRFLOW INTEGRATION
    # =========================================
    
    def run_checkpoint(
        self,
        table_name: str,
        stage: str = "raw",
        fail_on_error: bool = True
    ) -> Tuple[bool, Dict]:
        """
        Run a checkpoint for Airflow.
        
        Args:
            table_name: Table to validate
            stage: 'raw' or 'staging'
            fail_on_error: Whether to raise exception on blocking failures
            
        Returns:
            Tuple of (success, results_dict)
        """
        logger.info(f"Running checkpoint for {table_name} ({stage})")
        
        if stage == "raw":
            result = self.validate_raw_table(table_name)
        else:
            result = self.validate_staging_table(table_name)
        
        # Log summary
        stats = result.get("statistics", {})
        logger.info(
            f"Checkpoint complete: {stats.get('successful_expectations', 0)}/"
            f"{stats.get('evaluated_expectations', 0)} passed "
            f"({stats.get('success_percent', 0)}%)"
        )
        
        # Handle blocking failures
        if result.get("has_blocking_failures") and fail_on_error:
            blocking = result.get("blocking_failures", [])
            error_msg = f"Blocking validation failures: {[b['expectation_type'] for b in blocking]}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return result.get("success", False), result
    
    def generate_data_docs(self):
        """Generate Data Docs HTML reports."""
        try:
            self.context.build_data_docs()
            logger.info("Data Docs generated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to generate Data Docs: {e}")
            return False


# =========================================
# AIRFLOW TASK FUNCTIONS
# =========================================

def validate_leads_raw(**context) -> bool:
    """Airflow task to validate raw leads data."""
    runner = GXRunner()
    success, result = runner.run_checkpoint("leads", stage="raw")
    
    # Push results to XCom
    context['ti'].xcom_push(key='validation_result', value=result)
    
    return success


def validate_sales_raw(**context) -> bool:
    """Airflow task to validate raw sales data."""
    runner = GXRunner()
    success, result = runner.run_checkpoint("sales", stage="raw")
    
    context['ti'].xcom_push(key='validation_result', value=result)
    
    return success


def validate_leads_staging(**context) -> bool:
    """Airflow task to validate staging leads data."""
    runner = GXRunner()
    success, result = runner.run_checkpoint("leads", stage="staging")
    
    context['ti'].xcom_push(key='validation_result', value=result)
    
    return success


def validate_sales_staging(**context) -> bool:
    """Airflow task to validate staging sales data."""
    runner = GXRunner()
    success, result = runner.run_checkpoint("sales", stage="staging")
    
    context['ti'].xcom_push(key='validation_result', value=result)
    
    return success


def run_all_validations(stage: str = "raw") -> Dict:
    """
    Run all validations and return combined results.
    
    Useful for testing outside of Airflow.
    """
    runner = GXRunner()
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "tables": {}
    }
    
    for table in ["leads", "sales"]:
        try:
            success, result = runner.run_checkpoint(
                table, stage=stage, fail_on_error=False
            )
            results["tables"][table] = result
        except Exception as e:
            results["tables"][table] = {
                "success": False,
                "error": str(e)
            }
    
    # Overall success
    results["overall_success"] = all(
        r.get("success", False) for r in results["tables"].values()
    )
    
    return results


# =========================================
# CLI INTERFACE
# =========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run Great Expectations validations")
    parser.add_argument("--table", choices=["leads", "sales", "all"], default="all")
    parser.add_argument("--stage", choices=["raw", "staging"], default="raw")
    parser.add_argument("--fail-on-error", action="store_true")
    
    args = parser.parse_args()
    
    runner = GXRunner()
    
    if args.table == "all":
        results = run_all_validations(args.stage)
        print(json.dumps(results, indent=2, default=str))
        
        if not results["overall_success"] and args.fail_on_error:
            exit(1)
    else:
        success, result = runner.run_checkpoint(
            args.table, 
            stage=args.stage,
            fail_on_error=args.fail_on_error
        )
        print(json.dumps(result, indent=2, default=str))
        
        if not success and args.fail_on_error:
            exit(1)

