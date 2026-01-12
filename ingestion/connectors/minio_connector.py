"""
MinIO Target Connector
======================

Connector for writing data to MinIO object storage (S3-compatible).
Supports Parquet and CSV file formats.
"""

import io
import logging
from typing import Dict, Optional
from datetime import datetime
import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOConnector:
    """
    MinIO object storage connector for data lake operations.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize MinIO connector.
        
        Args:
            config: Connection configuration dict with endpoint, access_key, secret_key, bucket
        """
        self.config = config
        self.client = None
        self.bucket = config.get("bucket", "raw-data")
    
    def connect(self):
        """Establish connection to MinIO."""
        self.client = Minio(
            endpoint=self.config["endpoint"],
            access_key=self.config["access_key"],
            secret_key=self.config["secret_key"],
            secure=self.config.get("secure", False)
        )
        
        # Test connection and ensure bucket exists
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")
        
        logger.info(f"Connected to MinIO: {self.config['endpoint']}, bucket: {self.bucket}")
    
    def list_objects(self, prefix: str = "", recursive: bool = True) -> list:
        """
        List objects in bucket.
        
        Args:
            prefix: Object prefix filter
            recursive: Include nested objects
            
        Returns:
            List of object names
        """
        objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=recursive)
        return [obj.object_name for obj in objects]
    
    def write_dataframe(
        self, 
        df: pd.DataFrame, 
        path: str, 
        file_format: str = "csv",
        compression: str = "snappy"
    ) -> str:
        """
        Write DataFrame to MinIO.
        
        Args:
            df: Pandas DataFrame to write
            path: Target path in bucket (without file extension)
            file_format: Output format ('parquet' or 'csv')
            compression: Compression type for parquet
            
        Returns:
            Full object path
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, compression=compression)
            buffer.seek(0)
            object_name = f"{path}/data_{timestamp}.parquet"
            content_type = "application/octet-stream"
            
        elif file_format == "csv":
            buffer = io.BytesIO()
            # Write CSV with UTF-8 encoding
            csv_data = df.to_csv(index=False)
            buffer.write(csv_data.encode('utf-8'))
            buffer.seek(0)
            object_name = f"{path}/data_{timestamp}.csv"
            content_type = "text/csv"
            
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Upload to MinIO
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type=content_type
        )
        
        logger.info(f"Written {len(df)} rows to s3://{self.bucket}/{object_name}")
        return f"s3://{self.bucket}/{object_name}"
    
    def read_parquet(self, object_name: str) -> pd.DataFrame:
        """
        Read Parquet file from MinIO.
        
        Args:
            object_name: Object path in bucket
            
        Returns:
            DataFrame with file contents
        """
        response = self.client.get_object(self.bucket, object_name)
        df = pd.read_parquet(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        return df
    
    def read_csv(self, object_name: str) -> pd.DataFrame:
        """
        Read CSV file from MinIO.
        
        Args:
            object_name: Object path in bucket
            
        Returns:
            DataFrame with file contents
        """
        response = self.client.get_object(self.bucket, object_name)
        df = pd.read_csv(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        return df
    
    def delete_object(self, object_name: str):
        """
        Delete object from bucket.
        
        Args:
            object_name: Object path to delete
        """
        self.client.remove_object(self.bucket, object_name)
        logger.info(f"Deleted: s3://{self.bucket}/{object_name}")
    
    def object_exists(self, object_name: str) -> bool:
        """
        Check if object exists.
        
        Args:
            object_name: Object path to check
            
        Returns:
            True if exists, False otherwise
        """
        try:
            self.client.stat_object(self.bucket, object_name)
            return True
        except S3Error:
            return False

