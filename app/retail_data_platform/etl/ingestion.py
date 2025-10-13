"""
Ingestion module 
"""
import uuid
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, Iterator, Optional
import pandas as pd

from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config


@dataclass
class IngestionMetrics:
    source_name: str
    records_read: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def records_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.records_read / self.duration_seconds
        return 0.0


class CSVDataSource:
    """Tiny CSV reader used by pipeline (chunked via pandas)"""

    def __init__(self, name: str, file_path: str, chunk_size: int = 1000, encoding: str = "utf-8", delimiter: str = ","):
        self.name = name
        self.file_path = Path(file_path)
        self.chunk_size = int(chunk_size)
        self.encoding = encoding
        self.delimiter = delimiter
        self.logger = ETLLogger(f"ingestion.csv.{name}")
        self.metrics = IngestionMetrics(source_name=name)

    def validate_config(self) -> bool:
        try:
            if not self.file_path.exists() or not self.file_path.is_file():
                self.logger.error(f"CSV file not found or not a file: {self.file_path}")
                return False
            return True
        except Exception as e:
            self.logger.error(f"CSV validation error: {e}")
            return False

    def test_connection(self) -> bool:
        try:
            with open(self.file_path, "r", encoding=self.encoding) as f:
                f.readline()
            return True
        except Exception as e:
            self.logger.error(f"CSV connection failed: {e}")
            return False

    def read_data(self) -> Iterator[Dict[str, Any]]:
        """Yield validated records from CSV file (each record is a dict)."""
        self.metrics.start_time = datetime.utcnow()
        try:
            for chunk in pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                chunksize=self.chunk_size,
                dtype=str,
                keep_default_na=False,
                na_values=[]
            ):
                for _, row in chunk.iterrows():
                    rec = row.to_dict()
                    self.metrics.records_read += 1
                    if self._validate_record(rec):
                        self.metrics.records_valid += 1
                        yield rec
                    else:
                        self.metrics.records_invalid += 1
        finally:
            self.metrics.end_time = datetime.utcnow()

    def _validate_record(self, record: Dict[str, Any]) -> bool:
        required = ['InvoiceNo', 'StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice']
        for f in required:
            if f not in record or record[f] is None or str(record[f]).strip() == "":
                return False
        try:
            float(record.get('Quantity', 0))
            float(record.get('UnitPrice', 0))
        except Exception:
            return False
        return True


class DataIngestionManager:
    """Minimal manager used by ETLPipeline"""

    def __init__(self):
        self.logger = ETLLogger("ingestion.manager")
        self.sources: Dict[str, CSVDataSource] = {}
        self.batch_id = str(uuid.uuid4())
        self.config = get_config()

    def register_csv_source(self, name: str, file_path: str, **options) -> None:
        src = CSVDataSource(name=name, file_path=file_path, **options)
        if not src.validate_config() or not src.test_connection():
            raise ValueError(f"Failed to register CSV source: {name}")
        self.sources[name] = src
        self.logger.info(f"Registered CSV source: {name}")

    def ingest_from_source(self, source_name: str) -> Iterator[Dict[str, Any]]:
        if source_name not in self.sources:
            raise ValueError(f"Source not registered: {source_name}")
        src = self.sources[source_name]
        self.logger.info(f"Starting ingestion from {source_name} (batch {self.batch_id})")
        try:
            for rec in src.read_data():
                rec.setdefault('batch_id', self.batch_id)
                rec.setdefault('data_source', 'CSV')
                rec.setdefault('created_at', datetime.utcnow())
                yield rec
        finally:
            m = src.metrics
            try:
                self.logger.log_performance(f"ingestion_{source_name}", m.duration_seconds, m.records_read)
            except TypeError:
                self.logger.info(f"ingestion_{source_name} completed: duration={m.duration_seconds}s records={m.records_read}")


def create_ingestion_manager() -> DataIngestionManager:
    return DataIngestionManager()