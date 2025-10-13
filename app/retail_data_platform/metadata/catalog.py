"""
Data Catalog and Metadata Management
"""
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy import text
import json
import os

from ..database.connection import get_db_session
from ..utils.logging_config import ETLLogger

class DataCatalog:
    """Simple data catalog for documenting tables and columns"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)

    def get_table_info(self, table_name: str = None) -> List[Dict]:
        """Get information about tables"""
        with get_db_session() as session:
            if table_name:
                query = """
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'retail_dw' AND table_name = :table_name
                    ORDER BY ordinal_position
                """
                result = session.execute(text(query), {'table_name': table_name})
            else:
                query = """
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_schema = 'retail_dw'
                    ORDER BY table_name, ordinal_position
                """
                result = session.execute(text(query))
            
            table_info = []
            for row in result:
                table_info.append({
                    'table_name': row.table_name,
                    'column_name': row.column_name,
                    'data_type': row.data_type,
                    'is_nullable': row.is_nullable,
                    'column_default': row.column_default
                })
            
            return table_info
    
    def get_table_sizes(self) -> List[Dict]:
        """Get table sizes and row counts"""
        with get_db_session() as session:
            query = """
                SELECT 
                    t.table_name,
                    pg_size_pretty(pg_total_relation_size(c.oid)) as table_size,
                    s.n_live_tup as estimated_rows,
                    s.n_tup_ins as total_inserts,
                    s.n_tup_upd as total_updates,
                    s.n_tup_del as total_deletes
                FROM information_schema.tables t
                LEFT JOIN pg_class c ON c.relname = t.table_name
                LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
                WHERE t.table_schema = 'retail_dw'
                ORDER BY pg_total_relation_size(c.oid) DESC
            """
            result = session.execute(text(query))
            
            table_sizes = []
            for row in result:
                table_sizes.append({
                    'table_name': row.table_name,
                    'table_size': row.table_size,
                    'estimated_rows': row.estimated_rows or 0,
                    'total_inserts': row.total_inserts or 0,
                    'total_updates': row.total_updates or 0,
                    'total_deletes': row.total_deletes or 0
                })
            
            return table_sizes
    
    def get_relationships(self) -> List[Dict]:
        """Get foreign key relationships"""
        with get_db_session() as session:
            query = """
                SELECT 
                    tc.table_name as child_table,
                    kcu.column_name as child_column,
                    ccu.table_name as parent_table,
                    ccu.column_name as parent_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'retail_dw'
                ORDER BY tc.table_name
            """
            result = session.execute(text(query))

            relationships = []
            for row in result:
                relationships.append({
                    'child_table': row.child_table,
                    'child_column': row.child_column,
                    'parent_table': row.parent_table,
                    'parent_column': row.parent_column
                })
            
            return relationships

class LineageTracker:
    """Simple data lineage tracking"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
    
    def get_recent_lineage(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent lineage records"""
        try:
            with get_db_session() as session:
                query = text("""
                    SELECT
                        lineage_id,
                        source_file,
                        target_table,
                        etl_job_name,
                        batch_id,
                        records_processed,
                        records_inserted,
                        start_time,
                        end_time,
                        status
                    FROM retail_dw.data_lineage
                    ORDER BY start_time DESC
                    LIMIT :limit
                """)
                
                result = session.execute(query, {'limit': limit})
                
                lineage_records = []
                for row in result:
                    lineage_records.append({
                        'lineage_id': str(row.lineage_id),
                        'source_file': row.source_file,
                        'target_table': row.target_table,
                        'etl_job_name': row.etl_job_name,
                        'batch_id': str(row.batch_id),
                        'records_processed': row.records_processed or 0,
                        'records_inserted': row.records_inserted or 0,
                        'start_time': row.start_time.isoformat() if row.start_time else None,
                        'end_time': row.end_time.isoformat() if row.end_time else None,
                        'status': row.status
                    })
                
                return lineage_records
                
        except Exception as e:
            self.logger.error(f"Failed to get recent lineage: {e}")
            return []
    
    def get_batch_lineage(self, batch_id: str) -> List[Dict]:
        """Get lineage for specific batch"""
        with get_db_session() as session:
            query = """
                SELECT 
                    source_system,
                    source_file,
                    target_table,
                    records_processed,
                    records_inserted,
                    records_updated,
                    start_time,
                    end_time,
                    duration_seconds,
                    status,
                    error_message
                FROM retail_dw.data_lineage 
                WHERE batch_id = :batch_id
            """
            result = session.execute(text(query), {'batch_id': batch_id})
            
            batch_lineage = []
            for row in result:
                batch_lineage.append({
                    'source_system': row.source_system,
                    'source_file': row.source_file,
                    'target_table': row.target_table,
                    'records_processed': row.records_processed or 0,
                    'records_inserted': row.records_inserted or 0,
                    'records_updated': row.records_updated or 0,
                    'start_time': row.start_time.isoformat() if row.start_time else None,
                    'end_time': row.end_time.isoformat() if row.end_time else None,
                    'duration_seconds': row.duration_seconds or 0,
                    'status': row.status,
                    'error_message': row.error_message
                })
            
            return batch_lineage

class MetadataManager:
    """Simple metadata management system"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
        self.catalog = DataCatalog()
        self.lineage = LineageTracker()
    
    def generate_data_dictionary(self) -> Dict[str, Any]:
        """Generate simple data dictionary"""
        self.logger.info("Generating data dictionary")
        
        dictionary = {
            'generated_at': datetime.utcnow().isoformat(),
            'schema': 'retail_dw',
            'tables': {},
            'relationships': self.catalog.get_relationships(),
            'table_sizes': self.catalog.get_table_sizes()
        }
        
        # Get table info
        all_tables = self.catalog.get_table_info()
        
        # Group by table
        for table_info in all_tables:
            table_name = table_info['table_name']
            if table_name not in dictionary['tables']:
                dictionary['tables'][table_name] = {
                    'name': table_name,
                    'description': self._get_table_description(table_name),
                    'columns': []
                }
            
            dictionary['tables'][table_name]['columns'].append({
                'name': table_info['column_name'],
                'type': table_info['data_type'],
                'nullable': table_info['is_nullable'] == 'YES',
                'default': table_info['column_default'],
                'description': self._get_column_description(table_name, table_info['column_name'])
            })
        
        return dictionary
    
    def _get_table_description(self, table_name: str) -> str:
        """Get description for table"""
        descriptions = {
            'fact_sales': 'Core sales transactions with foreign keys to dimension tables',
            'dim_customer': 'Customer dimension with SCD Type 2 for tracking changes',
            'dim_product': 'Product master data with SCD Type 1 implementation',
            'dim_date': 'Date dimension for time-based analysis',
            'data_lineage': 'ETL job execution tracking and audit trail',
            'data_quality_metrics': 'Data quality measurements and monitoring'
        }
        return descriptions.get(table_name, f'Table: {table_name}')
    
    def _get_column_description(self, table_name: str, column_name: str) -> str:
        """Get description for column"""
        descriptions = {
            'fact_sales': {
                'sales_key': 'Surrogate primary key',
                'date_key': 'Foreign key to dim_date',
                'customer_key': 'Foreign key to dim_customer',
                'product_key': 'Foreign key to dim_product',
                'invoice_no': 'Business invoice number',
                'quantity': 'Number of items sold',
                'unit_price': 'Price per unit',
                'line_total': 'Total line amount (quantity * unit_price)',
                'transaction_type': 'SALE or RETURN',
                'batch_id': 'ETL batch identifier'
            },
            'dim_customer': {
                'customer_key': 'Surrogate primary key',
                'customer_id': 'Business customer identifier',
                'country': 'Customer country',
                'is_current': 'Current record flag for SCD Type 2'
            },
            'dim_product': {
                'product_key': 'Surrogate primary key',
                'stock_code': 'Business product identifier',
                'description': 'Product description'
            }
        }
        return descriptions.get(table_name, {}).get(column_name, f'Column: {column_name}')
    
    def export_dictionary(self, filename: str = None) -> str:
        """Export data dictionary to JSON file"""
        if not filename:
            filename = f"data_dictionary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        dictionary = self.generate_data_dictionary()
        
        docs_dir = "docs"
        if not os.path.exists(docs_dir):
            os.makedirs(docs_dir)
        
        filepath = os.path.join(docs_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(dictionary, f, indent=2, default=str)
        
        self.logger.info(f"Data dictionary exported to: {filepath}")
        return filepath

    def document_data_sources(self) -> Dict[str, Any]:
        """Document data sources for the dataset"""
        return {
            'sources': {
                'online_retail_csv': {
                    'type': 'CSV File',
                    'location': 'data/online_retail.csv',
                    'description': 'Historical online retail sales transactions',
                    'format': 'CSV',
                    'size_mb': self._get_file_size('data/online_retail.csv'),
                    'columns': [
                        'InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                        'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'
                    ],
                    'update_frequency': 'Daily',
                    'owner': 'Data Engineering Team',
                    'last_updated': datetime.now().isoformat()
                }
            },
            'external_systems': {
                'retail_erp': {
                    'type': 'ERP System',
                    'description': 'Source system for sales transactions',
                    'connection_type': 'File Export',
                    'data_freshness': '24 hours'
                }
            }
        }

    def document_transformations(self) -> Dict[str, Any]:
        """Document data transformations applied"""
        return {
            'transformations': {
                'data_cleaning': {
                    'process': 'Remove invalid records and standardize formats',
                    'rules': [
                        'Remove rows with negative quantities (returns handled separately)',
                        'Remove rows with zero or negative unit prices',
                        'Remove rows with missing customer IDs',
                        'Standardize date formats to ISO 8601',
                        'Trim whitespace from text fields'
                    ],
                    'implemented_in': 'etl/cleaning.py'
                },
                'dimension_creation': {
                    'process': 'Extract and create dimension tables',
                    'dimensions': {
                        'dim_customer': {
                            'source_columns': ['CustomerID', 'Country'],
                            'scd_type': 'Type 2',
                            'business_key': 'customer_id'
                        },
                        'dim_product': {
                            'source_columns': ['StockCode', 'Description'],
                            'scd_type': 'Type 1',
                            'business_key': 'stock_code'
                        },
                        'dim_date': {
                            'source_columns': ['InvoiceDate'],
                            'generated_attributes': ['year', 'month', 'quarter', 'day_of_week']
                        }
                    },
                    'implemented_in': 'etl/transformation.py'
                },
                'fact_table_creation': {
                    'process': 'Create fact table with foreign key lookups',
                    'measures': ['quantity', 'unit_price', 'line_total'],
                    'dimensions': ['date_key', 'customer_key', 'product_key'],
                    'grain': 'One row per invoice line item',
                    'implemented_in': 'etl/transformation.py'
                }
            }
        }

    def document_storage_locations(self) -> Dict[str, Any]:
        """Document storage locations and schema"""
        return {
            'storage': {
                'database': {
                    'type': 'PostgreSQL',
                    'host': 'localhost',
                    'database': 'ors',
                    'schema': 'retail_dw',
                    'connection_info': 'See development.yaml for connection details'
                },
                'tables': {
                    'fact_sales': {
                        'type': 'Fact Table',
                        'location': 'retail_dw.fact_sales',
                        'purpose': 'Store sales transaction facts',
                        'partitioning': 'None (could be partitioned by date)',
                        'indexes': ['sales_key (PK)', 'date_key', 'customer_key', 'product_key'],
                        'estimated_size': 'Variable based on data volume'
                    },
                    'dim_customer': {
                        'type': 'Dimension Table',
                        'location': 'retail_dw.dim_customer',
                        'purpose': 'Customer master data with SCD Type 2',
                        'scd_type': 'Type 2',
                        'indexes': ['customer_key (PK)', 'customer_id']
                    },
                    'dim_product': {
                        'type': 'Dimension Table',
                        'location': 'retail_dw.dim_product',
                        'purpose': 'Product master data',
                        'scd_type': 'Type 1',
                        'indexes': ['product_key (PK)', 'stock_code']
                    },
                    'dim_date': {
                        'type': 'Dimension Table',
                        'location': 'retail_dw.dim_date',
                        'purpose': 'Date dimension for time-based analysis',
                        'indexes': ['date_key (PK)', 'date_value']
                    }
                },
                'audit_tables': {
                    'data_lineage': {
                        'location': 'retail_dw.data_lineage',
                        'purpose': 'Track ETL job execution and data lineage'
                    },
                    'data_quality_metrics': {
                        'location': 'retail_dw.data_quality_metrics',
                        'purpose': 'Store data quality measurements'
                    }
                }
            }
        }

    def generate_complete_metadata_repository(self) -> Dict[str, Any]:
        """Generate complete metadata repository"""
        self.logger.info("Generating complete metadata repository")
        
        repository = {
            'metadata_repository': {
                'generated_at': datetime.now().isoformat(),
                'version': '1.0',
                'description': 'Complete metadata repository for Online Retail Sales Data Platform'
            },
            'data_dictionary': self.generate_data_dictionary(),
            'data_sources': self.document_data_sources(),
            'transformations': self.document_transformations(),
            'storage_locations': self.document_storage_locations(),
            'data_lineage': {
                'recent_jobs': self.lineage.get_recent_lineage(5),
                'tracking_method': 'Automated via ETL pipeline'
            }
        }
        
        return repository

    def _get_file_size(self, filepath: str) -> float:
        """Get file size in MB"""
        try:
            size_bytes = os.path.getsize(filepath)
            return round(size_bytes / (1024 * 1024), 2)
        except:
            return 0.0

    def export_complete_repository(self, filename: str = None) -> str:
        """Export complete metadata repository"""
        if not filename:
            filename = f"metadata_repository_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        repository = self.generate_complete_metadata_repository()
        
        docs_dir = "docs"
        if not os.path.exists(docs_dir):
            os.makedirs(docs_dir)
        
        filepath = os.path.join(docs_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(repository, f, indent=2, default=str)
        
        self.logger.info(f"Complete metadata repository exported to: {filepath}")
        return filepath

# Global instance
metadata_manager = MetadataManager()