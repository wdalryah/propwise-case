"""
MySQL Source Connector
======================

Connector for extracting data from MySQL databases.
Supports full table extraction and incremental (CDC) extraction.
"""

import logging
from typing import Dict, Optional, Any
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class MySQLConnector:
    """
    MySQL database connector for data extraction.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize MySQL connector.
        
        Args:
            config: Connection configuration dict with host, port, database, username, password
        """
        self.config = config
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Establish connection to MySQL database."""
        connection_string = (
            f"mysql+pymysql://{self.config['username']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
        self.engine = create_engine(connection_string)
        
        # Test connection
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Connected to MySQL: {self.config['host']}:{self.config['port']}/{self.config['database']}")
    
    def disconnect(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("MySQL connection closed")
    
    def get_tables(self, schema: str = None) -> list:
        """
        Get list of tables in the database.
        
        Args:
            schema: Database/schema name (optional, uses connection database if not specified)
            
        Returns:
            List of table names
        """
        schema = schema or self.config['database']
        query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE'
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]
    
    def get_table_schema(self, schema: str, table: str) -> pd.DataFrame:
        """
        Get table schema/column information.
        
        Args:
            schema: Database/schema name
            table: Table name
            
        Returns:
            DataFrame with column information
        """
        query = f"""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_key,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            ORDER BY ordinal_position
        """
        return pd.read_sql(query, self.engine)
    
    def get_row_count(self, schema: str, table: str) -> int:
        """
        Get row count for a table.
        
        Args:
            schema: Database/schema name
            table: Table name
            
        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) FROM `{schema}`.`{table}`"
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()
    
    def extract_table(self, schema: str, table: str, batch_size: int = None) -> pd.DataFrame:
        """
        Extract entire table data.
        
        Args:
            schema: Database/schema name
            table: Table name
            batch_size: Optional batch size for chunked reading
            
        Returns:
            DataFrame with table data
        """
        query = f"SELECT * FROM `{schema}`.`{table}`"
        
        logger.info(f"Extracting full table: {schema}.{table}")
        
        if batch_size:
            # Read in chunks for large tables
            chunks = []
            for chunk in pd.read_sql(query, self.engine, chunksize=batch_size):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_sql(query, self.engine)
        
        logger.info(f"Extracted {len(df)} rows from {schema}.{table}")
        return df
    
    def extract_incremental(
        self, 
        schema: str, 
        table: str, 
        tracking_column: str,
        watermark: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract incremental data based on tracking column.
        
        Args:
            schema: Database/schema name
            table: Table name
            tracking_column: Column to track changes (e.g., updated_at)
            watermark: Last processed value of tracking column
            
        Returns:
            DataFrame with changed rows
        """
        if watermark:
            # Handle date strings stored as VARCHAR (M/d/yyyy H:mm format)
            # Use STR_TO_DATE for proper date comparison instead of string comparison
            # Note: Use %% to escape % in f-strings for SQL date format
            query = f"""
                SELECT * FROM `{schema}`.`{table}` 
                WHERE STR_TO_DATE(`{tracking_column}`, '%%m/%%d/%%Y %%H:%%i') > STR_TO_DATE('{watermark}', '%%m/%%d/%%Y %%H:%%i')
                ORDER BY STR_TO_DATE(`{tracking_column}`, '%%m/%%d/%%Y %%H:%%i')
            """
        else:
            # No watermark = full extraction
            query = f"SELECT * FROM `{schema}`.`{table}` ORDER BY `{tracking_column}`"
        
        logger.info(f"Extracting incremental: {schema}.{table} (watermark: {watermark})")
        df = pd.read_sql(query, self.engine)
        logger.info(f"Extracted {len(df)} changed rows from {schema}.{table}")
        
        return df
    
    def extract_with_query(self, query: str) -> pd.DataFrame:
        """
        Extract data using custom SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        return pd.read_sql(query, self.engine)

