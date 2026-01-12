"""
Unified Schema Registry (Glue-like Data Catalog)
=================================================

Unified catalog for ALL data assets across the platform:

DATA SOURCES CATALOGED:
├── MinIO Staging (Parquet files)     → Logical schema in catalog
├── PostgreSQL DWH (dim_*, fact_*)    → Logical schema in catalog
└── Future: Other sources...

PHYSICAL DATA REMAINS WHERE IT IS:
├── MinIO: staging bucket (Parquet)
└── PostgreSQL: DWH tables

CATALOG PROVIDES:
- Single view of all data assets
- Schema versioning and evolution tracking
- Data lineage (source → target)
- Unified metadata queries

Like AWS Glue Data Catalog which catalogs both S3 and RDS/Redshift.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
from io import BytesIO

import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

import psycopg2
from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)


# =========================================
# DATA TYPES MAPPING
# =========================================

# PyArrow to standard type mapping
PYARROW_TYPE_MAP = {
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "bigint",
    "uint8": "integer",
    "uint16": "integer",
    "uint32": "integer",
    "uint64": "bigint",
    "float16": "float",
    "float32": "float",
    "float64": "double",
    "bool": "boolean",
    "string": "string",
    "large_string": "string",
    "binary": "binary",
    "large_binary": "binary",
    "date32": "date",
    "date64": "date",
    "timestamp": "timestamp",
    "time32": "time",
    "time64": "time",
    "decimal128": "decimal",
    "list": "array",
    "struct": "struct",
    "map": "map"
}

# Pandas dtype to standard type mapping
PANDAS_TYPE_MAP = {
    "int64": "bigint",
    "int32": "integer",
    "float64": "double",
    "float32": "float",
    "object": "string",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
    "timedelta64[ns]": "interval",
    "category": "string"
}


class SchemaRegistry:
    """
    Unified Schema Registry (Data Catalog).
    
    Catalogs ALL data assets:
    - MinIO staging tables (Parquet)
    - PostgreSQL DWH tables (dim_*, fact_*)
    
    Data remains physically where it is.
    Catalog provides unified logical view.
    """
    
    # Asset types
    ASSET_TYPE_STAGING = "staging"
    ASSET_TYPE_DWH = "dwh"
    ASSET_TYPE_RAW = "raw"
    
    def __init__(
        self,
        minio_endpoint: str = "localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        postgres_config: Optional[Dict] = None,
        dwh_config: Optional[Dict] = None
    ):
        """
        Initialize Schema Registry.
        
        Args:
            minio_endpoint: MinIO endpoint for staging data
            postgres_config: PostgreSQL config for metadata store
            dwh_config: PostgreSQL config for DWH (if different)
        """
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Metadata store (catalog database)
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5433,
            "database": "propwise_metadata",
            "user": "metadata_user",
            "password": "metadata123"
        }
        
        # DWH connection (for crawling DWH tables)
        self.dwh_config = dwh_config or {
            "host": "localhost",
            "port": 5434,
            "database": "propwise_dwh",
            "user": "dwh_user",
            "password": "dwh123"
        }
        
        self._db_conn = None
        self._dwh_conn = None
    
    @property
    def db_conn(self):
        """Metadata store connection."""
        if self._db_conn is None or self._db_conn.closed:
            self._db_conn = psycopg2.connect(**self.postgres_config)
        return self._db_conn
    
    @property
    def dwh_conn(self):
        """DWH connection for schema discovery."""
        if self._dwh_conn is None or self._dwh_conn.closed:
            try:
                self._dwh_conn = psycopg2.connect(**self.dwh_config)
            except Exception as e:
                logger.warning(f"Could not connect to DWH: {e}")
                return None
        return self._dwh_conn
    
    def close(self):
        if self._db_conn and not self._db_conn.closed:
            self._db_conn.close()
        if self._dwh_conn and not self._dwh_conn.closed:
            self._dwh_conn.close()
    
    # =========================================
    # SCHEMA DISCOVERY (CRAWLER)
    # =========================================
    
    def crawl_bucket(
        self,
        bucket: str = "staging",
        prefix: str = ""
    ) -> List[Dict]:
        """
        Crawl a MinIO bucket and discover schemas.
        
        Like AWS Glue Crawler - finds all datasets and infers schemas.
        
        Returns:
            List of discovered tables with their schemas
        """
        logger.info(f"Crawling bucket: {bucket}/{prefix}")
        discovered = []
        
        # List all objects to find distinct table prefixes
        objects = list(self.minio_client.list_objects(bucket, prefix=prefix, recursive=True))
        
        # Group by table (first level directory)
        tables = {}
        for obj in objects:
            if obj.is_dir:
                continue
            
            # Parse path: processed_leads/year=2024/month=01/file.parquet
            parts = obj.object_name.split('/')
            if len(parts) >= 1:
                table_name = parts[0]
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append(obj.object_name)
        
        # Discover schema for each table
        for table_name, files in tables.items():
            try:
                schema = self._discover_table_schema(bucket, table_name, files)
                if schema:
                    discovered.append(schema)
                    logger.info(f"Discovered schema for {table_name}: {len(schema['columns'])} columns")
            except Exception as e:
                logger.error(f"Failed to discover schema for {table_name}: {e}")
        
        return discovered
    
    def _discover_table_schema(
        self,
        bucket: str,
        table_name: str,
        files: List[str]
    ) -> Optional[Dict]:
        """Discover schema from Parquet files."""
        # Find a Parquet file to read schema from
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        if not parquet_files:
            return None
        
        # Read schema from first file
        sample_file = parquet_files[0]
        
        try:
            response = self.minio_client.get_object(bucket, sample_file)
            data = response.read()
            response.close()
            response.release_conn()
            
            # Read Parquet schema
            parquet_file = pq.ParquetFile(BytesIO(data))
            arrow_schema = parquet_file.schema_arrow
            
            # Convert to standard format
            columns = []
            for i, field in enumerate(arrow_schema):
                col_type = self._arrow_type_to_standard(str(field.type))
                columns.append({
                    "name": field.name,
                    "type": col_type,
                    "nullable": field.nullable,
                    "ordinal_position": i + 1,
                    "arrow_type": str(field.type)
                })
            
            # Detect partitions from path
            partitions = self._detect_partitions(parquet_files[0])
            
            # Get file stats
            total_size = sum(
                self.minio_client.stat_object(bucket, f).size 
                for f in parquet_files[:10]  # Sample first 10
            )
            
            return {
                "table_name": table_name,
                "location": f"s3://{bucket}/{table_name}/",
                "format": "parquet",
                "columns": columns,
                "partitions": partitions,
                "file_count": len(parquet_files),
                "sample_size_bytes": total_size,
                "discovered_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to read schema from {sample_file}: {e}")
            return None
    
    def _arrow_type_to_standard(self, arrow_type: str) -> str:
        """Convert PyArrow type to standard type."""
        # Handle parameterized types
        base_type = arrow_type.split('[')[0].split('(')[0].lower()
        return PYARROW_TYPE_MAP.get(base_type, "string")
    
    def _detect_partitions(self, file_path: str) -> List[Dict]:
        """Detect partition columns from file path."""
        partitions = []
        parts = file_path.split('/')
        
        for part in parts[1:-1]:  # Skip table name and filename
            if '=' in part:
                key, value = part.split('=', 1)
                partitions.append({
                    "name": key,
                    "type": "string",
                    "sample_value": value
                })
        
        return partitions
    
    # =========================================
    # DWH SCHEMA DISCOVERY
    # =========================================
    
    def crawl_dwh(
        self,
        schema_name: str = "public",
        table_patterns: List[str] = None
    ) -> List[Dict]:
        """
        Crawl PostgreSQL DWH and discover table schemas.
        
        Like Glue Crawler for JDBC sources.
        
        Args:
            schema_name: PostgreSQL schema to crawl
            table_patterns: Table name patterns (e.g., ['dim_%', 'fact_%'])
        
        Returns:
            List of discovered tables with their schemas
        """
        if not self.dwh_conn:
            logger.error("Cannot connect to DWH")
            return []
        
        table_patterns = table_patterns or ['dim_%', 'fact_%', 'etl_%']
        logger.info(f"Crawling DWH schema: {schema_name}")
        
        discovered = []
        
        try:
            with self.dwh_conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Build pattern query
                pattern_conditions = " OR ".join(
                    f"table_name LIKE '{p}'" for p in table_patterns
                )
                
                # Get tables
                cur.execute(f"""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND ({pattern_conditions})
                    ORDER BY table_name
                """, (schema_name,))
                
                tables = cur.fetchall()
                
                for table in tables:
                    table_name = table['table_name']
                    schema = self._discover_dwh_table_schema(cur, schema_name, table_name)
                    if schema:
                        discovered.append(schema)
                        logger.info(f"Discovered DWH table {table_name}: {len(schema['columns'])} columns")
        
        except Exception as e:
            logger.error(f"Failed to crawl DWH: {e}")
        
        return discovered
    
    def _discover_dwh_table_schema(
        self,
        cursor,
        schema_name: str,
        table_name: str
    ) -> Optional[Dict]:
        """Discover schema for a DWH table."""
        try:
            # Get columns
            cursor.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_name))
            
            columns = []
            for col in cursor.fetchall():
                columns.append({
                    "name": col["column_name"],
                    "type": self._pg_type_to_standard(col["data_type"]),
                    "pg_type": col["data_type"],
                    "nullable": col["is_nullable"] == "YES",
                    "default": col["column_default"],
                    "ordinal_position": col["ordinal_position"],
                    "max_length": col["character_maximum_length"],
                    "precision": col["numeric_precision"],
                    "scale": col["numeric_scale"]
                })
            
            # Get primary key
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (f"{schema_name}.{table_name}",))
            
            pk_columns = [row["attname"] for row in cursor.fetchall()]
            
            # Get row count estimate
            cursor.execute(f"""
                SELECT reltuples::bigint as row_count
                FROM pg_class
                WHERE relname = %s
            """, (table_name,))
            
            row_count_result = cursor.fetchone()
            row_count = row_count_result["row_count"] if row_count_result else 0
            
            # Determine table type (dimension vs fact)
            if table_name.startswith("dim_"):
                table_type = "dimension"
            elif table_name.startswith("fact_"):
                table_type = "fact"
            else:
                table_type = "other"
            
            return {
                "table_name": table_name,
                "asset_type": self.ASSET_TYPE_DWH,
                "location": f"postgresql://{self.dwh_config['host']}:{self.dwh_config['port']}/{self.dwh_config['database']}/{schema_name}.{table_name}",
                "format": "postgresql",
                "schema_name": schema_name,
                "table_type": table_type,
                "columns": columns,
                "primary_key": pk_columns,
                "row_count_estimate": row_count,
                "partitions": [],  # PostgreSQL partitions would need separate query
                "discovered_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to discover schema for {table_name}: {e}")
            return None
    
    def _pg_type_to_standard(self, pg_type: str) -> str:
        """Convert PostgreSQL type to standard type."""
        pg_type_map = {
            "integer": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "numeric": "decimal",
            "real": "float",
            "double precision": "double",
            "character varying": "string",
            "varchar": "string",
            "character": "string",
            "char": "string",
            "text": "string",
            "boolean": "boolean",
            "date": "date",
            "timestamp without time zone": "timestamp",
            "timestamp with time zone": "timestamp",
            "time without time zone": "time",
            "time with time zone": "time",
            "bytea": "binary",
            "json": "json",
            "jsonb": "json",
            "uuid": "string",
            "array": "array"
        }
        return pg_type_map.get(pg_type.lower(), "string")
    
    # =========================================
    # SCHEMA REGISTRATION
    # =========================================
    
    def register_schema(
        self,
        table_name: str,
        schema: Dict,
        created_by: str = "crawler"
    ) -> Tuple[int, int]:
        """
        Register a schema in the registry.
        
        If schema differs from current, creates new version.
        
        Returns:
            Tuple of (schema_id, version)
        """
        # Get current schema
        current = self.get_schema(table_name)
        
        # Check if schema changed
        if current:
            if self._schemas_equal(current["schema_definition"], schema):
                logger.debug(f"Schema unchanged for {table_name}")
                return current["schema_id"], current["schema_version"]
        
        # Register new version
        try:
            with self.db_conn.cursor() as cur:
                # Get next version
                cur.execute("""
                    SELECT COALESCE(MAX(schema_version), 0) + 1 
                    FROM pipeline.schema_registry 
                    WHERE table_name = %s
                """, (table_name,))
                next_version = cur.fetchone()[0]
                
                # Deactivate previous
                cur.execute("""
                    UPDATE pipeline.schema_registry 
                    SET is_active = FALSE 
                    WHERE table_name = %s AND is_active = TRUE
                """, (table_name,))
                
                # Insert new schema
                cur.execute("""
                    INSERT INTO pipeline.schema_registry 
                    (table_name, schema_version, schema_definition, is_active, created_by)
                    VALUES (%s, %s, %s, TRUE, %s)
                    RETURNING schema_id
                """, (table_name, next_version, Json(schema), created_by))
                
                schema_id = cur.fetchone()[0]
                
                # Insert field definitions
                for col in schema.get("columns", []):
                    cur.execute("""
                        INSERT INTO pipeline.schema_fields 
                        (schema_id, field_name, field_type, is_nullable, ordinal_position, description)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        schema_id,
                        col["name"],
                        col["type"],
                        col.get("nullable", True),
                        col.get("ordinal_position", 0),
                        col.get("description")
                    ))
                
                self.db_conn.commit()
                
                logger.info(f"Registered schema v{next_version} for {table_name}")
                
                # Log schema change if not first version
                if next_version > 1:
                    self._log_schema_evolution(table_name, current, schema)
                
                return schema_id, next_version
                
        except Exception as e:
            logger.error(f"Failed to register schema: {e}")
            self.db_conn.rollback()
            return 0, 0
    
    def _schemas_equal(self, schema1: Dict, schema2: Dict) -> bool:
        """Check if two schemas are equivalent."""
        cols1 = {c["name"]: c["type"] for c in schema1.get("columns", [])}
        cols2 = {c["name"]: c["type"] for c in schema2.get("columns", [])}
        return cols1 == cols2
    
    def _log_schema_evolution(
        self,
        table_name: str,
        old_schema: Dict,
        new_schema: Dict
    ):
        """Log schema changes for evolution tracking."""
        old_cols = {c["name"]: c for c in old_schema.get("schema_definition", {}).get("columns", [])}
        new_cols = {c["name"]: c for c in new_schema.get("columns", [])}
        
        try:
            with self.db_conn.cursor() as cur:
                # New columns
                for name in set(new_cols.keys()) - set(old_cols.keys()):
                    cur.execute("""
                        INSERT INTO pipeline.schema_changes 
                        (table_name, change_type, new_value)
                        VALUES (%s, 'column_added', %s)
                    """, (table_name, Json(new_cols[name])))
                
                # Removed columns
                for name in set(old_cols.keys()) - set(new_cols.keys()):
                    cur.execute("""
                        INSERT INTO pipeline.schema_changes 
                        (table_name, change_type, old_value)
                        VALUES (%s, 'column_removed', %s)
                    """, (table_name, Json(old_cols[name])))
                
                # Type changes
                for name in set(old_cols.keys()) & set(new_cols.keys()):
                    if old_cols[name]["type"] != new_cols[name]["type"]:
                        cur.execute("""
                            INSERT INTO pipeline.schema_changes 
                            (table_name, change_type, old_value, new_value)
                            VALUES (%s, 'type_changed', %s, %s)
                        """, (
                            table_name,
                            Json({"column": name, "type": old_cols[name]["type"]}),
                            Json({"column": name, "type": new_cols[name]["type"]})
                        ))
                
                self.db_conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to log schema evolution: {e}")
            self.db_conn.rollback()
    
    # =========================================
    # CATALOG QUERIES
    # =========================================
    
    def get_schema(
        self,
        table_name: str,
        version: Optional[int] = None
    ) -> Optional[Dict]:
        """
        Get schema for a table.
        
        Args:
            table_name: Table name
            version: Specific version (None = active/latest)
        """
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                if version:
                    cur.execute("""
                        SELECT * FROM pipeline.schema_registry
                        WHERE table_name = %s AND schema_version = %s
                    """, (table_name, version))
                else:
                    cur.execute("""
                        SELECT * FROM pipeline.schema_registry
                        WHERE table_name = %s AND is_active = TRUE
                        ORDER BY schema_version DESC
                        LIMIT 1
                    """, (table_name,))
                
                result = cur.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            logger.error(f"Failed to get schema: {e}")
            return None
    
    def get_schema_fields(self, schema_id: int) -> List[Dict]:
        """Get field definitions for a schema."""
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM pipeline.schema_fields
                    WHERE schema_id = %s
                    ORDER BY ordinal_position
                """, (schema_id,))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get schema fields: {e}")
            return []
    
    def list_tables(
        self,
        asset_type: Optional[str] = None
    ) -> List[Dict]:
        """
        List all registered tables with their active schemas.
        
        Args:
            asset_type: Filter by 'staging', 'dwh', or None for all
        """
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        table_name,
                        schema_version,
                        schema_definition->>'asset_type' as asset_type,
                        schema_definition->>'format' as format,
                        schema_definition->>'location' as location,
                        schema_definition->>'table_type' as table_type,
                        jsonb_array_length(schema_definition->'columns') as column_count,
                        (schema_definition->>'row_count_estimate')::bigint as row_count,
                        created_at
                    FROM pipeline.schema_registry
                    WHERE is_active = TRUE
                    ORDER BY 
                        schema_definition->>'asset_type',
                        table_name
                """)
                
                tables = [dict(row) for row in cur.fetchall()]
                
                # Filter by asset type if specified
                if asset_type:
                    tables = [t for t in tables if t.get("asset_type") == asset_type]
                
                return tables
                
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def list_staging_tables(self) -> List[Dict]:
        """List only staging (MinIO) tables."""
        return self.list_tables(asset_type=self.ASSET_TYPE_STAGING)
    
    def list_dwh_tables(self) -> List[Dict]:
        """List only DWH (PostgreSQL) tables."""
        return self.list_tables(asset_type=self.ASSET_TYPE_DWH)
    
    def get_catalog_summary(self) -> Dict:
        """Get summary of all cataloged assets."""
        all_tables = self.list_tables()
        
        staging_tables = [t for t in all_tables if t.get("asset_type") == self.ASSET_TYPE_STAGING]
        dwh_tables = [t for t in all_tables if t.get("asset_type") == self.ASSET_TYPE_DWH]
        
        dim_tables = [t for t in dwh_tables if t.get("table_type") == "dimension"]
        fact_tables = [t for t in dwh_tables if t.get("table_type") == "fact"]
        
        return {
            "total_tables": len(all_tables),
            "staging": {
                "count": len(staging_tables),
                "tables": [t["table_name"] for t in staging_tables]
            },
            "dwh": {
                "count": len(dwh_tables),
                "dimensions": {
                    "count": len(dim_tables),
                    "tables": [t["table_name"] for t in dim_tables]
                },
                "facts": {
                    "count": len(fact_tables),
                    "tables": [t["table_name"] for t in fact_tables]
                }
            }
        }
    
    def get_schema_history(self, table_name: str) -> List[Dict]:
        """Get version history for a table."""
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        schema_id,
                        schema_version,
                        is_active,
                        created_at,
                        created_by
                    FROM pipeline.schema_registry
                    WHERE table_name = %s
                    ORDER BY schema_version DESC
                """, (table_name,))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get schema history: {e}")
            return []
    
    def get_schema_changes(
        self,
        table_name: str,
        since: Optional[datetime] = None
    ) -> List[Dict]:
        """Get schema change history for a table."""
        try:
            with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                if since:
                    cur.execute("""
                        SELECT * FROM pipeline.schema_changes
                        WHERE table_name = %s AND detected_at >= %s
                        ORDER BY detected_at DESC
                    """, (table_name, since))
                else:
                    cur.execute("""
                        SELECT * FROM pipeline.schema_changes
                        WHERE table_name = %s
                        ORDER BY detected_at DESC
                        LIMIT 100
                    """, (table_name,))
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get schema changes: {e}")
            return []
    
    # =========================================
    # CRAWLER JOB
    # =========================================
    
    def run_crawler(
        self,
        bucket: str = "staging",
        include_dwh: bool = True,
        auto_register: bool = True
    ) -> Dict:
        """
        Run the unified crawler job (like Glue Crawler).
        
        Discovers schemas from:
        - MinIO staging bucket (Parquet files)
        - PostgreSQL DWH (dim_*, fact_* tables)
        
        Args:
            bucket: MinIO bucket to crawl
            include_dwh: Also crawl DWH tables
            auto_register: Register discovered schemas
        
        Returns:
            Summary of crawl results
        """
        logger.info(f"Starting unified crawler...")
        start_time = datetime.now()
        
        results = {
            "start_time": start_time.isoformat(),
            "sources_crawled": [],
            "tables_discovered": 0,
            "tables_updated": 0,
            "tables_unchanged": 0,
            "tables_new": 0,
            "errors": [],
            "staging": {"tables": []},
            "dwh": {"tables": []}
        }
        
        # =========================================
        # Crawl MinIO Staging
        # =========================================
        logger.info(f"Crawling MinIO bucket: {bucket}")
        results["sources_crawled"].append(f"minio://{bucket}")
        
        staging_discovered = self.crawl_bucket(bucket)
        
        for schema in staging_discovered:
            schema["asset_type"] = self.ASSET_TYPE_STAGING
        
        results["staging"]["discovered"] = len(staging_discovered)
        
        # =========================================
        # Crawl DWH Tables
        # =========================================
        dwh_discovered = []
        if include_dwh:
            logger.info("Crawling PostgreSQL DWH...")
            results["sources_crawled"].append(f"postgresql://{self.dwh_config['host']}:{self.dwh_config['port']}/{self.dwh_config['database']}")
            
            dwh_discovered = self.crawl_dwh()
            results["dwh"]["discovered"] = len(dwh_discovered)
        
        # Combine all discovered
        all_discovered = staging_discovered + dwh_discovered
        results["tables_discovered"] = len(all_discovered)
        
        # =========================================
        # Register All Schemas
        # =========================================
        if auto_register:
            for schema in all_discovered:
                table_name = schema["table_name"]
                asset_type = schema.get("asset_type", self.ASSET_TYPE_STAGING)
                
                try:
                    # Get current schema
                    current = self.get_schema(table_name)
                    
                    # Register
                    schema_id, version = self.register_schema(table_name, schema)
                    
                    if current and version > current.get("schema_version", 0):
                        results["tables_updated"] += 1
                        status = "updated"
                    elif not current:
                        results["tables_new"] += 1
                        status = "new"
                    else:
                        results["tables_unchanged"] += 1
                        status = "unchanged"
                    
                    table_info = {
                        "table_name": table_name,
                        "asset_type": asset_type,
                        "status": status,
                        "version": version,
                        "column_count": len(schema.get("columns", []))
                    }
                    
                    if asset_type == self.ASSET_TYPE_DWH:
                        table_info["table_type"] = schema.get("table_type", "other")
                        results["dwh"]["tables"].append(table_info)
                    else:
                        results["staging"]["tables"].append(table_info)
                    
                except Exception as e:
                    results["errors"].append({
                        "table_name": table_name,
                        "asset_type": asset_type,
                        "error": str(e)
                    })
        
        results["end_time"] = datetime.now().isoformat()
        results["duration_seconds"] = (datetime.now() - start_time).total_seconds()
        
        logger.info(
            f"Unified crawler complete: {results['tables_discovered']} discovered "
            f"({len(staging_discovered)} staging, {len(dwh_discovered)} dwh), "
            f"{results['tables_new']} new, {results['tables_updated']} updated"
        )
        
        return results


# =========================================
# AIRFLOW TASK FUNCTIONS
# =========================================

def run_schema_crawler(**context) -> Dict:
    """
    Airflow task to run the unified schema crawler.
    
    Crawls both:
    - MinIO staging (Parquet files)
    - PostgreSQL DWH (dim_*, fact_* tables)
    """
    registry = SchemaRegistry(
        minio_endpoint='minio:9000',
        dwh_config={
            "host": "postgres-dwh",
            "port": 5432,
            "database": "propwise_dwh",
            "user": "dwh_user",
            "password": "dwh123"
        }
    )
    
    results = registry.run_crawler(bucket="staging", include_dwh=True, auto_register=True)
    
    print("\n" + "=" * 70)
    print("UNIFIED SCHEMA CATALOG - CRAWLER RESULTS")
    print("=" * 70)
    print(f"Sources crawled: {', '.join(results['sources_crawled'])}")
    print(f"Total tables discovered: {results['tables_discovered']}")
    print(f"  - New: {results['tables_new']}")
    print(f"  - Updated: {results['tables_updated']}")
    print(f"  - Unchanged: {results['tables_unchanged']}")
    print(f"Duration: {results['duration_seconds']:.2f}s")
    
    # Staging tables
    print("\n📦 STAGING TABLES (MinIO)")
    print("-" * 40)
    for table in results.get("staging", {}).get("tables", []):
        status_icon = {"new": "🆕", "updated": "📝", "unchanged": "✓"}.get(table["status"], "?")
        print(f"  {status_icon} {table['table_name']}: v{table['version']} ({table['column_count']} columns)")
    
    # DWH tables
    print("\n🏛️  DWH TABLES (PostgreSQL)")
    print("-" * 40)
    for table in results.get("dwh", {}).get("tables", []):
        status_icon = {"new": "🆕", "updated": "📝", "unchanged": "✓"}.get(table["status"], "?")
        type_icon = {"dimension": "📊", "fact": "📈"}.get(table.get("table_type"), "📋")
        print(f"  {status_icon} {type_icon} {table['table_name']}: v{table['version']} ({table['column_count']} columns)")
    
    if results.get("errors"):
        print("\n⚠️  ERRORS:")
        for err in results["errors"]:
            print(f"  ❌ {err['table_name']}: {err['error']}")
    
    registry.close()
    context['ti'].xcom_push(key='crawler_results', value=results)
    
    return results


def list_catalog_tables(**context) -> List[Dict]:
    """Airflow task to list all tables in the unified catalog."""
    registry = SchemaRegistry(
        minio_endpoint='minio:9000',
        dwh_config={
            "host": "postgres-dwh",
            "port": 5432,
            "database": "propwise_dwh",
            "user": "dwh_user",
            "password": "dwh123"
        }
    )
    
    summary = registry.get_catalog_summary()
    
    print("\n" + "=" * 70)
    print("UNIFIED DATA CATALOG")
    print("=" * 70)
    print(f"Total cataloged tables: {summary['total_tables']}")
    
    print("\n📦 STAGING (MinIO - Data Lake)")
    print("-" * 40)
    print(f"Tables: {summary['staging']['count']}")
    for t in summary['staging']['tables']:
        print(f"  • {t}")
    
    print("\n🏛️  DWH (PostgreSQL - Data Warehouse)")
    print("-" * 40)
    print(f"Total: {summary['dwh']['count']}")
    print(f"\n  📊 Dimensions ({summary['dwh']['dimensions']['count']}):")
    for t in summary['dwh']['dimensions']['tables']:
        print(f"    • {t}")
    print(f"\n  📈 Facts ({summary['dwh']['facts']['count']}):")
    for t in summary['dwh']['facts']['tables']:
        print(f"    • {t}")
    
    # Also get full table list
    tables = registry.list_tables()
    
    registry.close()
    context['ti'].xcom_push(key='catalog_summary', value=summary)
    context['ti'].xcom_push(key='catalog_tables', value=tables)
    
    return tables


# =========================================
# CLI
# =========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified Schema Registry CLI")
    parser.add_argument("command", choices=["crawl", "list", "show", "history", "summary"])
    parser.add_argument("--bucket", default="staging", help="MinIO bucket to crawl")
    parser.add_argument("--table", help="Table name for show/history")
    parser.add_argument("--asset-type", choices=["staging", "dwh"], help="Filter by asset type")
    parser.add_argument("--no-dwh", action="store_true", help="Skip DWH crawling")
    
    args = parser.parse_args()
    
    registry = SchemaRegistry()
    
    if args.command == "crawl":
        print("\n" + "=" * 60)
        print("UNIFIED SCHEMA CRAWLER")
        print("=" * 60)
        
        results = registry.run_crawler(
            bucket=args.bucket,
            include_dwh=not args.no_dwh
        )
        
        print(f"\nSources: {', '.join(results['sources_crawled'])}")
        print(f"Total discovered: {results['tables_discovered']}")
        print(f"  - New: {results['tables_new']}")
        print(f"  - Updated: {results['tables_updated']}")
        print(f"  - Unchanged: {results['tables_unchanged']}")
        
        print("\n📦 Staging Tables:")
        for t in results.get("staging", {}).get("tables", []):
            print(f"  • {t['table_name']} (v{t['version']}, {t['column_count']} cols)")
        
        print("\n🏛️  DWH Tables:")
        for t in results.get("dwh", {}).get("tables", []):
            icon = "📊" if t.get("table_type") == "dimension" else "📈"
            print(f"  {icon} {t['table_name']} (v{t['version']}, {t['column_count']} cols)")
    
    elif args.command == "list":
        tables = registry.list_tables(asset_type=args.asset_type)
        
        print("\n" + "=" * 60)
        print(f"CATALOG TABLES" + (f" ({args.asset_type})" if args.asset_type else " (all)"))
        print("=" * 60)
        
        for t in tables:
            asset_icon = "📦" if t.get("asset_type") == "staging" else "🏛️"
            print(f"{asset_icon} {t['table_name']}")
            print(f"   Version: {t['schema_version']}, Columns: {t['column_count']}")
            print(f"   Location: {t['location']}")
            print()
    
    elif args.command == "summary":
        summary = registry.get_catalog_summary()
        
        print("\n" + "=" * 60)
        print("CATALOG SUMMARY")
        print("=" * 60)
        print(f"Total Tables: {summary['total_tables']}")
        
        print(f"\n📦 Staging: {summary['staging']['count']} tables")
        for t in summary['staging']['tables']:
            print(f"   • {t}")
        
        print(f"\n🏛️  DWH: {summary['dwh']['count']} tables")
        print(f"   📊 Dimensions: {summary['dwh']['dimensions']['count']}")
        for t in summary['dwh']['dimensions']['tables']:
            print(f"      • {t}")
        print(f"   📈 Facts: {summary['dwh']['facts']['count']}")
        for t in summary['dwh']['facts']['tables']:
            print(f"      • {t}")
    
    elif args.command == "show" and args.table:
        schema = registry.get_schema(args.table)
        if schema:
            print(json.dumps(schema, indent=2, default=str))
        else:
            print(f"Table not found: {args.table}")
    
    elif args.command == "history" and args.table:
        history = registry.get_schema_history(args.table)
        for h in history:
            active = "✓" if h["is_active"] else " "
            print(f"[{active}] v{h['schema_version']} - {h['created_at']} by {h['created_by']}")
    
    registry.close()

