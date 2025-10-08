"""
Data Ingestion Module

Handles data ingestion from various sources including CSV files,
databases, and APIs with robust error handling and validation.
"""

import csv
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
import uuid

from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config


@dataclass
class IngestionMetrics:
    """Metrics for data ingestion operations"""
    source_name: str
    records_read: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def records_per_second(self) -> float:
        """Calculate ingestion rate"""
        if self.duration_seconds > 0:
            return self.records_read / self.duration_seconds
        return 0.0


class DataSource(ABC):
    """Abstract base class for data sources"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.logger = ETLLogger(f"ingestion.{name}")
        self.metrics = IngestionMetrics(source_name=name)
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate source configuration"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to data source"""
        pass
    
    @abstractmethod
    def read_data(self) -> Iterator[Dict[str, Any]]:
        """Read data from source and yield records"""
        pass
    
    def get_metrics(self) -> IngestionMetrics:
        """Get ingestion metrics"""
        return self.metrics


class CSVDataSource(DataSource):
    """Data source for CSV files"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.file_path = Path(config['file_path'])
        self.encoding = config.get('encoding', 'utf-8')
        self.delimiter = config.get('delimiter', ',')
        self.has_header = config.get('has_header', True)
        self.chunk_size = config.get('chunk_size', 1000)
        
    def validate_config(self) -> bool:
        """Validate CSV configuration"""
        try:
            if not self.file_path.exists():
                self.logger.error(f"CSV file not found: {self.file_path}")
                return False
            
            if not self.file_path.is_file():
                self.logger.error(f"Path is not a file: {self.file_path}")
                return False
            
            # Check file size
            file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"CSV file size: {file_size_mb:.2f} MB")
            
            return True
            
        except Exception as e:
            self.logger.error(f"CSV configuration validation failed: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test CSV file accessibility"""
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                f.readline()
            return True
        except Exception as e:
            self.logger.error(f"CSV connection test failed: {e}")
            return False
    
    def read_data(self) -> Iterator[Dict[str, Any]]:
        """Read data from CSV file in chunks"""
        self.metrics.start_time = datetime.utcnow()
        
        try:
            # Use pandas for efficient CSV reading
            chunk_iterator = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                chunksize=self.chunk_size,
                dtype=str,  
                keep_default_na=False, 
                na_values=[]  
            )
            
            for chunk_num, chunk in enumerate(chunk_iterator):
                self.logger.debug(f"Processing chunk {chunk_num + 1}, size: {len(chunk)}")
                
                for idx, row in chunk.iterrows():
                    record = row.to_dict()
                    self.metrics.records_read += 1
                    
                    # Basic validation
                    if self._validate_record(record):
                        self.metrics.records_valid += 1
                        yield record
                    else:
                        self.metrics.records_invalid += 1
                        self.logger.warning(f"Invalid record at row {idx + 1}", record=record)
                
                # Log progress periodically
                if chunk_num > 0 and chunk_num % 10 == 0:
                    self.logger.info(f"Processed {chunk_num + 1} chunks, "
                                   f"{self.metrics.records_read} records")
        
        except Exception as e:
            self.logger.error(f"Error reading CSV data: {e}")
            raise
        finally:
            self.metrics.end_time = datetime.utcnow()
            self.logger.info("CSV ingestion completed", 
                           records_read=self.metrics.records_read,
                           records_valid=self.metrics.records_valid,
                           records_invalid=self.metrics.records_invalid,
                           duration=self.metrics.duration_seconds,
                           rate=self.metrics.records_per_second)
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate individual record"""

        required_fields = ['InvoiceNo', 'StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice']
        
        for field in required_fields:
            if field not in record or not record[field] or record[field].strip() == '':
                return False
        
        # Basic data type validation
        try:
            float(record['Quantity'])
            float(record['UnitPrice'])
            # Basic date format check
            if not record['InvoiceDate']:
                return False
        except (ValueError, TypeError):
            return False
        
        return True


class DatabaseDataSource(DataSource):
    """Data source for database queries"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.connection_string = config['connection_string']
        self.query = config['query']
        self.batch_size = config.get('batch_size', 1000)
    
    def validate_config(self) -> bool:
        """Validate database configuration"""
        if not self.connection_string or not self.query:
            self.logger.error("Database connection string and query are required")
            return False
        return True
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            from sqlalchemy import create_engine
            engine = create_engine(self.connection_string)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def read_data(self) -> Iterator[Dict[str, Any]]:
        """Read data from database"""
        self.metrics.start_time = datetime.utcnow()
        
        try:
            # Use pandas for database reading
            chunk_iterator = pd.read_sql(
                self.query,
                self.connection_string,
                chunksize=self.batch_size
            )
            
            for chunk in chunk_iterator:
                for _, row in chunk.iterrows():
                    record = row.to_dict()
                    self.metrics.records_read += 1
                    
                    if self._validate_record(record):
                        self.metrics.records_valid += 1
                        yield record
                    else:
                        self.metrics.records_invalid += 1
                        
        except Exception as e:
            self.logger.error(f"Error reading database data: {e}")
            raise
        finally:
            self.metrics.end_time = datetime.utcnow()
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate database record"""

        return len(record) > 0 and any(v is not None for v in record.values())


class DataIngestionManager:
    """
    Manages data ingestion from multiple sources with orchestration
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = ETLLogger("ingestion.manager")
        self.sources: Dict[str, DataSource] = {}
        self.batch_id = str(uuid.uuid4())
        
    def register_source(self, source: DataSource) -> None:
        """Register a data source"""
        if source.validate_config() and source.test_connection():
            self.sources[source.name] = source
            self.logger.info(f"Registered data source: {source.name}")
        else:
            raise ValueError(f"Failed to register source: {source.name}")
    
    def register_csv_source(self, name: str, file_path: str, **kwargs) -> None:
        """Convenience method to register CSV source"""
        config = {'file_path': file_path, **kwargs}
        source = CSVDataSource(name, config)
        self.register_source(source)
    
    def register_database_source(self, name: str, connection_string: str, 
                                query: str, **kwargs) -> None:
        """Convenience method to register database source"""
        config = {
            'connection_string': connection_string,
            'query': query,
            **kwargs
        }
        source = DatabaseDataSource(name, config)
        self.register_source(source)
    
    def ingest_from_source(self, source_name: str) -> Iterator[Dict[str, Any]]:
        """Ingest data from a specific source"""
        if source_name not in self.sources:
            raise ValueError(f"Source not found: {source_name}")
        
        source = self.sources[source_name]
        self.logger.set_context(
            source_name=source_name,
            batch_id=self.batch_id
        )
        
        self.logger.info(f"Starting ingestion from source: {source_name}")
        
        try:
            for record in source.read_data():
                # Add ingestion metadata to each record
                record['_ingestion_batch_id'] = self.batch_id
                record['_ingestion_source'] = source_name
                record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
                
                yield record
                
        except Exception as e:
            self.logger.error(f"Ingestion failed for source {source_name}: {e}")
            raise
        finally:
            metrics = source.get_metrics()
            self.logger.log_performance(
                f"ingestion_{source_name}",
                metrics.duration_seconds,
                metrics.records_read
            )
    
    def ingest_from_all_sources(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """Ingest data from all registered sources"""
        for source_name in self.sources:
            self.logger.info(f"Processing source: {source_name}")
            for record in self.ingest_from_source(source_name):
                yield source_name, record
    
    def get_source_metrics(self, source_name: str) -> Optional[IngestionMetrics]:
        """Get metrics for a specific source"""
        if source_name in self.sources:
            return self.sources[source_name].get_metrics()
        return None
    
    def get_all_metrics(self) -> Dict[str, IngestionMetrics]:
        """Get metrics for all sources"""
        return {name: source.get_metrics() for name, source in self.sources.items()}


# Factory function for easy instantiation
def create_ingestion_manager() -> DataIngestionManager:
    """Create and return a configured data ingestion manager"""
    return DataIngestionManager()