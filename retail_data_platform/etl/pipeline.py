"""
ETL Pipeline Orchestrator

Main ETL pipeline that orchestrates the complete Extract, Transform, Load process
with error handling, monitoring, scheduling, recovery capabilities, and data quality checks.
"""

import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Iterator
from dataclasses import dataclass
from enum import Enum
from contextlib import contextmanager
import os

from .ingestion import create_ingestion_manager, DataIngestionManager
from .cleaning import create_cleaning_pipeline, DataCleaningPipeline
from .transformation import create_transformation_pipeline, DataTransformationPipeline
from ..database.connection import get_db_session
from ..database.models import FactSales, DataLineage
from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config
from ..monitoring.quality import create_quality_monitor


class ETLStatus(Enum):
    """ETL job status enumeration"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"


@dataclass
class ETLJobConfig:
    """Configuration for ETL job execution"""
    job_name: str
    source_config: Dict[str, Any]
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    enable_monitoring: bool = True
    quality_threshold: float = 0.95
    parallel_processing: bool = False
    checkpoint_interval: int = 5000


@dataclass
class ETLMetrics:
    """Comprehensive ETL execution metrics"""
    job_id: str
    job_name: str
    status: ETLStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    
    # Records metrics
    records_extracted: int = 0
    records_cleaned: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_rejected: int = 0
    
    # Error metrics
    extraction_errors: int = 0
    cleaning_errors: int = 0
    transformation_errors: int = 0
    loading_errors: int = 0
    
    # Performance metrics
    extraction_duration: float = 0.0
    cleaning_duration: float = 0.0
    transformation_duration: float = 0.0
    loading_duration: float = 0.0
    
    # Quality metrics
    quality_metrics: Optional[Dict[str, Any]] = None
    quality_duration: float = 0.0
    
    # Versioning info
    version_id: Optional[int] = None
    version_number: Optional[str] = None
    source_file: Optional[str] = None
    
    @property
    def total_duration(self) -> float:
        """Calculate total ETL duration"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate overall success rate"""
        if self.records_extracted > 0:
            return (self.records_loaded / self.records_extracted) * 100
        return 0.0
    
    @property
    def records_per_second(self) -> float:
        """Calculate processing rate"""
        if self.total_duration > 0:
            return self.records_extracted / self.total_duration
        return 0.0
    

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary including version info"""
        return {
            'job_id': self.job_id,
            'job_name': self.job_name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_duration': self.total_duration,
            'records_extracted': self.records_extracted,
            'records_cleaned': self.records_cleaned,
            'records_transformed': self.records_transformed,
            'records_loaded': self.records_loaded,
            'records_rejected': self.records_rejected,
            'success_rate': self.success_rate,
            'records_per_second': self.records_per_second,
            'version_id': self.version_id,
            'version_number': self.version_number,
            'source_file': self.source_file,
            'quality_metrics': self.quality_metrics
        }


class ETLCheckpoint:
    """Manages ETL checkpointing for recovery"""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.logger = ETLLogger("etl.checkpoint")
        self.checkpoint_data = {}
    
    def save_checkpoint(self, stage: str, records_processed: int, 
                       additional_data: Dict[str, Any] = None) -> None:
        """Save checkpoint data"""
        checkpoint = {
            'job_id': self.job_id,
            'stage': stage,
            'records_processed': records_processed,
            'timestamp': datetime.utcnow().isoformat(),
            'additional_data': additional_data or {}
        }
        
        self.checkpoint_data[stage] = checkpoint
        self.logger.info(f"Checkpoint saved for stage {stage}", 
                        records_processed=records_processed)
    
    def get_checkpoint(self, stage: str) -> Optional[Dict[str, Any]]:
        """Get checkpoint data for a stage"""
        return self.checkpoint_data.get(stage)
    
    def clear_checkpoints(self) -> None:
        """Clear all checkpoint data"""
        self.checkpoint_data.clear()


class ETLPipeline:
    """
    Main ETL pipeline orchestrator
    """
    
    def __init__(self, config: ETLJobConfig):
        self.config = config
        self.app_config = get_config()
        self.logger = ETLLogger(f"etl.pipeline.{config.job_name}")
        
        # Initialize components
        self.ingestion_manager = create_ingestion_manager()
        self.cleaning_pipeline = create_cleaning_pipeline()
        self.transformation_pipeline = create_transformation_pipeline()
        
        self.source_file = config.source_config.get('file_path')
        # Job tracking
        self.job_id = str(uuid.uuid4())
        self.metrics = ETLMetrics(
            job_id=self.job_id,
            job_name=config.job_name,
            status=ETLStatus.PENDING,
            start_time=datetime.utcnow()
        )
        self.checkpoint = ETLCheckpoint(self.job_id)
        

        self.loaded_records_sample = []
        
        # Configure ingestion sources
        self._configure_sources()
        
        self.logger.set_context(
            job_id=self.job_id,
            job_name=config.job_name
        )
    
    def _configure_sources(self) -> None:
        """Configure data sources based on configuration"""
        source_config = self.config.source_config
        
        if source_config['type'] == 'csv':
            self.ingestion_manager.register_csv_source(
                name=source_config['name'],
                file_path=source_config['file_path'],
                **source_config.get('options', {})
            )
        elif source_config['type'] == 'database':
            self.ingestion_manager.register_database_source(
                name=source_config['name'],
                connection_string=source_config['connection_string'],
                query=source_config['query'],
                **source_config.get('options', {})
            )
        else:
            raise ValueError(f"Unsupported source type: {source_config['type']}")
    
    def execute(self) -> ETLMetrics:
        """Execute the complete ETL pipeline"""
        self.logger.info(f"Starting ETL job: {self.config.job_name}")
        self.metrics.status = ETLStatus.RUNNING
        
        try:
            # Record lineage start
            lineage_id = self._start_lineage_tracking()
            

            version_id = self._create_version_for_job()
            
            # Execute ETL stages
            self._execute_extract_stage()
            

            if version_id:
                self._tag_data_with_version(version_id)
                self._update_version_record_count(version_id)
            
            # Execute quality checks after data loading
            self._execute_quality_checks()
            
            # Complete lineage tracking
            self._complete_lineage_tracking(lineage_id, ETLStatus.SUCCESS)
            
            self.metrics.status = ETLStatus.SUCCESS
            self.metrics.end_time = datetime.utcnow()
            
            self.logger.info("ETL job completed successfully", 
                        **self._get_metrics_dict())
            
        except Exception as e:
            self.metrics.status = ETLStatus.FAILED
            self.metrics.end_time = datetime.utcnow()
            
            self.logger.error(f"ETL job failed: {e}", 
                            **self._get_metrics_dict(), exc_info=True)
            
            # Try to complete lineage tracking with failure status
            try:
                lineage_id = getattr(self, '_current_lineage_id', None)
                if lineage_id:
                    self._complete_lineage_tracking(lineage_id, ETLStatus.FAILED, str(e))
            except:
                pass 
        
        finally:
            self._cleanup()
        
        return self.metrics
    
    def _execute_extract_stage(self) -> None:
        """Execute the extraction stage"""
        self.logger.info("Starting extraction stage")
        extraction_start = datetime.utcnow()
        
        try:
            source_name = self.config.source_config['name']
            
            # Process records in batches
            batch = []
            records_processed = 0
            
            for record in self.ingestion_manager.ingest_from_source(source_name):
                batch.append(record)
                records_processed += 1
                
                # Process batch when it reaches configured size
                if len(batch) >= self.config.batch_size:
                    self._process_batch(batch)
                    batch = []
                    
                    # Save checkpoint periodically
                    if records_processed % self.config.checkpoint_interval == 0:
                        self.checkpoint.save_checkpoint('extraction', records_processed)
                
                self.metrics.records_extracted += 1
            
            # Process remaining records
            if batch:
                self._process_batch(batch)
            
            # Final checkpoint
            self.checkpoint.save_checkpoint('extraction', records_processed)
            
        except Exception as e:
            self.metrics.extraction_errors += 1
            raise e
        finally:
            self.metrics.extraction_duration = \
                (datetime.utcnow() - extraction_start).total_seconds()
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of records through cleaning, transformation, and loading"""
        cleaned_records = []
        
        # Cleaning stage
        cleaning_start = datetime.utcnow()
        for record in batch:
            try:
                cleaned_record = self.cleaning_pipeline.clean_record(record)
                if cleaned_record:
                    cleaned_records.append(cleaned_record)
                    self.metrics.records_cleaned += 1
                else:
                    self.metrics.records_rejected += 1
            except Exception as e:
                self.metrics.cleaning_errors += 1
                self.logger.warning(f"Cleaning failed for record: {e}")
        
        self.metrics.cleaning_duration += \
            (datetime.utcnow() - cleaning_start).total_seconds()
        
        # Transformation stage
        transformation_start = datetime.utcnow()
        transformed_records = []
        
        for record in cleaned_records:
            try:
                transformed_record = self.transformation_pipeline.transform_record(record)
                if transformed_record:
                    transformed_records.append(transformed_record)
                    self.metrics.records_transformed += 1
                else:
                    self.metrics.records_rejected += 1
            except Exception as e:
                self.metrics.transformation_errors += 1
                self.logger.warning(f"Transformation failed for record: {e}")
        
        self.metrics.transformation_duration += \
            (datetime.utcnow() - transformation_start).total_seconds()
        
        # Loading stage
        loading_start = datetime.utcnow()
        self._load_batch_to_warehouse(transformed_records)
        self.metrics.loading_duration += \
            (datetime.utcnow() - loading_start).total_seconds()
    
    def _load_batch_to_warehouse(self, records: List[Dict[str, Any]]) -> None:
        """Load transformed records to the data warehouse"""
        if not records:
            return
        
        try:
            with get_db_session() as session:
                fact_records = []
                
                for record in records:
                    try:
                        # Create fact sale record
                        fact_sale = FactSales(
                            date_key=record.get('date_key'),
                            customer_key=record.get('customer_key'),
                            product_key=record.get('product_key'),
                            invoice_no=record.get('invoice_no'),
                            transaction_type=record.get('transaction_type'),
                            quantity=record.get('quantity'),
                            unit_price=record.get('unit_price'),
                            line_total=record.get('line_total'),
                            transaction_datetime=record.get('transaction_datetime'),
                            created_at=record.get('created_at', datetime.utcnow()),
                            batch_id=record.get('batch_id', self.job_id),
                            data_source=record.get('data_source', 'CSV')
                        )
                        
                        fact_records.append(fact_sale)
                        
                        if len(self.loaded_records_sample) < 1000:  # Keep max 1000 for quality
                            self.loaded_records_sample.append(record)
                        
                    except Exception as e:
                        self.metrics.loading_errors += 1
                        self.logger.warning(f"Failed to create fact record: {e}")
                
                # Bulk insert
                if fact_records:
                    session.add_all(fact_records)
                    session.commit()
                    self.metrics.records_loaded += len(fact_records)
                    
                    self.logger.debug(f"Loaded {len(fact_records)} records to warehouse")
                
        except Exception as e:
            self.metrics.loading_errors += 1
            self.logger.error(f"Batch loading failed: {e}")
            raise


    def _execute_quality_checks(self) -> None:
        """Execute data quality checks on loaded data"""
        self.logger.info("Starting data quality checks")
        quality_start = datetime.utcnow()
        
        try:
            # Generate batch ID for quality tracking
            batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{self.job_id[:8]}"
            
            # Create quality monitor
            quality_monitor = create_quality_monitor(batch_id)
            
            if self.loaded_records_sample and len(self.loaded_records_sample) > 0:
                self.logger.info(f"Running quality checks on {len(self.loaded_records_sample)} records")
                
                # Run quality checks
                quality_results = quality_monitor.check_data_quality(self.loaded_records_sample, 'fact_sales')
                
                # Persist quality metrics to database
                quality_monitor.persist_quality_metrics()
                
                # Get quality summary
                quality_summary = quality_monitor.get_quality_summary()
                
                # Store quality metrics in ETL metrics
                self.metrics.quality_metrics = {
                    'total_checks': quality_summary.get('total_checks', 0),
                    'passed_checks': quality_summary.get('passed_checks', 0),
                    'failed_checks': quality_summary.get('failed_checks', 0),
                    'success_rate': quality_summary.get('success_rate', 0),
                    'overall_score': quality_summary.get('overall_score', 0),
                    'batch_id': batch_id
                }
                
                # Quality monitoring and alerts
                from ..monitoring.alerts import quality_alert_manager
                
                # Check for immediate quality issues
                quality_alert_manager.check_and_alert(self.metrics.quality_metrics, 'fact_sales')
                
                # Check for anomalies 
                anomalies = quality_monitor.detect_quality_anomalies()
                if anomalies:
                    quality_alert_manager.check_anomalies(anomalies)
                    self.logger.warning(f"Quality anomalies detected: {len(anomalies)} issues")
                
                self.logger.info(f"Quality checks completed - Success rate: {quality_summary.get('success_rate', 0):.1f}%, Overall score: {quality_summary.get('overall_score', 0):.1f}%")
                
                # Log quality warnings if needed
                if quality_summary.get('failed_checks', 0) > 0:
                    self.logger.warning(f"Quality issues detected: {quality_summary.get('failed_checks', 0)} checks failed")
                
                # Check if quality meets threshold
                overall_score = quality_summary.get('overall_score', 0)
                if overall_score < (self.config.quality_threshold * 100):
                    self.logger.warning(f"Quality score {overall_score:.1f}% below threshold {self.config.quality_threshold * 100}%")
                        
            else:
                self.logger.warning("No data available for quality checks")
                self.metrics.quality_metrics = {
                    'total_checks': 0,
                    'passed_checks': 0,
                    'failed_checks': 0,
                    'success_rate': 0,
                    'overall_score': 0,
                    'batch_id': batch_id,
                    'warning': 'No data available for quality checks'
                }
                    
        except Exception as e:
            self.logger.error(f"Quality checks failed: {e}")
            # Don't fail the entire ETL job if quality checks fail
            self.metrics.quality_metrics = {
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0,
                'success_rate': 0,
                'overall_score': 0,
                'error': str(e)
            }
        finally:
            self.metrics.quality_duration = (datetime.utcnow() - quality_start).total_seconds()

    def _start_lineage_tracking(self) -> str:
        """Start data lineage tracking"""
        try:
            with get_db_session() as session:
                lineage = DataLineage(
                    source_system=self.config.source_config.get('type', 'Unknown'),
                    source_table=self.config.source_config.get('name', 'Unknown'),
                    source_file=self.config.source_config.get('file_path'),
                    target_table='fact_sales',
                    etl_job_name=self.config.job_name,
                    batch_id=self.job_id,
                    records_processed=0,
                    records_inserted=0,
                    records_updated=0,
                    records_rejected=0,
                    start_time=self.metrics.start_time,
                    end_time=self.metrics.start_time,  
                    duration_seconds=0,
                    status='RUNNING',
                    metadata={'job_config': self.config.__dict__}
                )
                
                session.add(lineage)
                session.commit()
                
                lineage_id = str(lineage.lineage_id)
                self._current_lineage_id = lineage_id
                
                return lineage_id
                
        except Exception as e:
            self.logger.warning(f"Failed to start lineage tracking: {e}")
            return ""
    
    def _complete_lineage_tracking(self, lineage_id: str, status: ETLStatus, 
                                  error_message: str = None) -> None:
        """Complete data lineage tracking"""
        if not lineage_id:
            return
        
        try:
            with get_db_session() as session:
                lineage = session.query(DataLineage).filter_by(
                    lineage_id=lineage_id
                ).first()
                
                if lineage:
                    lineage.records_processed = self.metrics.records_extracted
                    lineage.records_inserted = self.metrics.records_loaded
                    lineage.records_updated = 0  
                    lineage.records_rejected = self.metrics.records_rejected
                    lineage.end_time = datetime.utcnow()
                    lineage.duration_seconds = int(self.metrics.total_duration)
                    lineage.status = status.value
                    lineage.error_message = error_message
                    
                    session.commit()
                    
        except Exception as e:
            self.logger.warning(f"Failed to complete lineage tracking: {e}")
    

    def _get_metrics_dict(self) -> Dict[str, Any]:
        """Get metrics as dictionary for logging"""
        metrics_dict = {
            'job_id': self.metrics.job_id,
            'records_extracted': self.metrics.records_extracted,
            'records_cleaned': self.metrics.records_cleaned,
            'records_transformed': self.metrics.records_transformed,
            'records_loaded': self.metrics.records_loaded,
            'records_rejected': self.metrics.records_rejected,
            'success_rate': self.metrics.success_rate,
            'total_duration': self.metrics.total_duration,
            'records_per_second': self.metrics.records_per_second,
            'version_id': self.metrics.version_id,
            'version_number': self.metrics.version_number
        }
        
        # Add quality metrics if available
        if self.metrics.quality_metrics:
            metrics_dict['quality_score'] = self.metrics.quality_metrics.get('overall_score', 0)
            metrics_dict['quality_success_rate'] = self.metrics.quality_metrics.get('success_rate', 0)
            metrics_dict['quality_checks_total'] = self.metrics.quality_metrics.get('total_checks', 0)
            metrics_dict['quality_checks_passed'] = self.metrics.quality_metrics.get('passed_checks', 0)
        
        return metrics_dict
    
    def _cleanup(self) -> None:
        """Cleanup resources after ETL completion"""
        try:
            # Clear caches
            if hasattr(self.transformation_pipeline.transformer, 'cache'):
                self.transformation_pipeline.transformer.cache.clear_all()
            
            # Clear checkpoints
            self.checkpoint.clear_checkpoints()
            
            # Clear loaded records sample
            self.loaded_records_sample.clear()
            
            self.logger.debug("ETL cleanup completed")
            
        except Exception as e:
            self.logger.warning(f"Cleanup failed: {e}")

    # Add these versioning methods
    def _create_version_for_job(self) -> int:
        """Create a new data version for this ETL job"""
        import hashlib
        from datetime import datetime
        from sqlalchemy import text
        
        # Generate version number based on timestamp
        version_number = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Calculate file hash if source file exists
        file_hash = None
        if self.source_file and os.path.exists(self.source_file):
            try:
                with open(self.source_file, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()[:16]
            except:
                file_hash = None
        
        # Create version record
        try:
            with get_db_session() as session:
                query = text("""
                    INSERT INTO retail_dw.data_versions (
                        version_number, version_type, description, 
                        source_file, file_hash, etl_job_id, status
                    ) VALUES (
                        :version_number, 'INCREMENTAL', :description,
                        :source_file, :file_hash, :job_name, 'ACTIVE'
                    ) RETURNING version_id
                """)
                
                result = session.execute(query, {
                    'version_number': version_number,
                    'description': f'ETL load from {os.path.basename(self.source_file) if self.source_file else "pipeline"}',
                    'source_file': self.source_file,
                    'file_hash': file_hash,
                    'job_name': self.config.job_name
                })
                
                version_id = result.scalar()
                session.commit()
                
                # Store version info in metrics
                self.metrics.version_id = version_id
                self.metrics.version_number = version_number
                self.metrics.source_file = self.source_file
                
                self.logger.info(f"✅ Created data version {version_number} (ID: {version_id}) for job {self.config.job_name}")
                return version_id
                
        except Exception as e:
            self.logger.error(f"Failed to create version: {e}")
            return None

    def _tag_data_with_version(self, version_id: int):
        """Tag newly loaded data with version ID"""
        if not version_id:
            return
            
        try:
            with get_db_session() as session:
                from sqlalchemy import text
                
                queries = [
                    "UPDATE retail_dw.fact_sales SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL",
                    "UPDATE retail_dw.dim_customer SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL AND is_current = true",
                    "UPDATE retail_dw.dim_product SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL"
                ]
                
                total_updated = 0
                for query in queries:
                    result = session.execute(text(query), {'version_id': version_id})
                    total_updated += result.rowcount
                
                session.commit()
                self.logger.info(f"✅ Tagged {total_updated} records with version {version_id}")
                
        except Exception as e:
            self.logger.error(f"Failed to tag data with version: {e}")

    def _update_version_record_count(self, version_id: int):
        """Update the record count in the version table"""
        if not version_id:
            return
            
        try:
            with get_db_session() as session:
                from sqlalchemy import text
                
                # Count records for this version
                count_query = text("SELECT COUNT(*) FROM retail_dw.fact_sales WHERE version_id = :version_id")
                result = session.execute(count_query, {'version_id': version_id})
                record_count = result.scalar()
                
                # Update version record
                update_query = text("UPDATE retail_dw.data_versions SET records_count = :count WHERE version_id = :version_id")
                session.execute(update_query, {'count': record_count, 'version_id': version_id})
                session.commit()
                
                self.logger.info(f"✅ Updated version {version_id} with {record_count} records")
                
        except Exception as e:
            self.logger.error(f"Failed to update version record count: {e}")

class ETLOrchestrator:
    """
    High-level ETL orchestrator for managing multiple pipelines
    """
    
    def __init__(self):
        self.logger = ETLLogger("etl.orchestrator")
        self.running_jobs: Dict[str, ETLPipeline] = {}
        self.job_history: List[ETLMetrics] = []
    
    def create_retail_csv_job(self, csv_file_path: str, job_name: str = None) -> ETLJobConfig:
        """Create a job configuration for retail CSV processing"""
        if not job_name:
            job_name = f"retail_csv_import_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        return ETLJobConfig(
            job_name=job_name,
            source_config={
                'type': 'csv',
                'name': 'retail_sales_csv',
                'file_path': csv_file_path,
                'options': {
                    'chunk_size': 1000,
                    'encoding': 'utf-8'
                }
            },
            batch_size=1000,
            max_retries=3,
            enable_monitoring=True,
            quality_threshold=0.95  # 95% quality threshold
        )
    
    def execute_job(self, job_config: ETLJobConfig) -> ETLMetrics:
        """Execute an ETL job"""
        self.logger.info(f"Starting ETL job: {job_config.job_name}")
        
        try:
            pipeline = ETLPipeline(job_config)
            self.running_jobs[pipeline.job_id] = pipeline
            
            # Execute the pipeline
            metrics = pipeline.execute()
            
            # Store in history
            self.job_history.append(metrics)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Job execution failed: {e}")
            raise
        finally:
            # Remove from running jobs
            if pipeline.job_id in self.running_jobs:
                del self.running_jobs[pipeline.job_id]
    
    def get_job_status(self, job_id: str) -> Optional[ETLStatus]:
        """Get the status of a running job"""
        if job_id in self.running_jobs:
            return self.running_jobs[job_id].metrics.status
        
        # Check history
        for metrics in self.job_history:
            if metrics.job_id == job_id:
                return metrics.status
        
        return None
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        if job_id in self.running_jobs:
            self.logger.warning(f"Job cancellation requested: {job_id}")
            self.running_jobs[job_id].metrics.status = ETLStatus.CANCELLED
            return True
        
        return False
    
    def get_job_history(self, limit: int = 50) -> List[ETLMetrics]:
        """Get job execution history"""
        return self.job_history[-limit:]


# Global orchestrator instance
etl_orchestrator = ETLOrchestrator()

def run_retail_csv_etl(csv_file_path: str, job_name: str = None) -> ETLMetrics:
    """Convenience function to run retail CSV ETL with quality checks and versioning"""
    job_config = etl_orchestrator.create_retail_csv_job(csv_file_path, job_name)
    metrics = etl_orchestrator.execute_job(job_config)
    
    # Enhanced logging with quality and version information
    logger = ETLLogger("retail_csv_etl")
    log_data = {
        'job_name': metrics.job_name,
        'records_processed': metrics.records_extracted,
        'records_loaded': metrics.records_loaded,
        'success_rate': f"{metrics.success_rate:.2f}%",
        'duration': f"{metrics.total_duration:.2f}s",
        'records_per_second': f"{metrics.records_per_second:.2f}",
        'version_id': metrics.version_id,
        'version_number': metrics.version_number
    }
    
    if metrics.quality_metrics:
        log_data['quality_score'] = f"{metrics.quality_metrics.get('overall_score', 0):.1f}%"
        log_data['quality_checks'] = f"{metrics.quality_metrics.get('passed_checks', 0)}/{metrics.quality_metrics.get('total_checks', 0)} passed"
    
    logger.info("ETL job completed with quality checks and versioning", **log_data)
    
    return metrics