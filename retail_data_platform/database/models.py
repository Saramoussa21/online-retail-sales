"""
Data Warehouse Schema Models

Defines the dimensional model for the retail data warehouse
using SQLAlchemy ORM with star schema design.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Boolean, 
    ForeignKey, Index, UniqueConstraint, CheckConstraint,
    text, Date, BigInteger, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.postgresql import UUID, JSONB
from enum import Enum as PyEnum

import uuid

Base = declarative_base()


class DimDate(Base):
    """Date dimension table for time-based analysis"""
    
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'retail_dw'}
    
    date_key = Column(Integer, primary_key=True, doc="Date key in YYYYMMDD format")
    date_value = Column(Date, nullable=False, unique=True, doc="Actual date value")
    
    # Date components
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    day_of_year = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    
    # Date names
    month_name = Column(String(20), nullable=False)
    day_name = Column(String(20), nullable=False)
    quarter_name = Column(String(10), nullable=False)
    
    # Business flags
    is_weekend = Column(Boolean, nullable=False, default=False)
    is_holiday = Column(Boolean, nullable=False, default=False)
    
    __table_args__ = (
        Index('idx_dim_date_year_month', 'year', 'month'),
        {'schema': 'retail_dw'}
    )


class DimCustomer(Base):
    """Customer dimension table with SCD Type 2 support"""
    
    __tablename__ = 'dim_customer'
    __table_args__ = {'schema': 'retail_dw'}
    
    customer_key = Column(BigInteger, primary_key=True, autoincrement=True, 
                         doc="Surrogate key for customer dimension")
    customer_id = Column(String(50), nullable=False, 
                        doc="Natural key from source system")
    
    # Customer attributes
    country = Column(String(100), nullable=False)
    
    # SCD Type 2 fields
    effective_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    expiry_date = Column(DateTime, nullable=True, 
                        doc="NULL for current record, date for historical")
    is_current = Column(Boolean, nullable=False, default=True)
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, 
                       onupdate=datetime.utcnow)
    data_source = Column(String(50), nullable=False, default='CSV')
    
    __table_args__ = (
        Index('idx_dim_customer_id_current', 'customer_id', 'is_current'),  
        {'schema': 'retail_dw'}
    )


class DimProduct(Base):
    """Product dimension table with SCD Type 1 support"""
    
    __tablename__ = 'dim_product'
    __table_args__ = {'schema': 'retail_dw'}
    
    product_key = Column(BigInteger, primary_key=True, autoincrement=True,
                        doc="Surrogate key for product dimension")
    stock_code = Column(String(50), nullable=False, unique=True,
                       doc="Natural key from source system")
    
    # Product attributes
    description = Column(String(255), nullable=False)
    category = Column(String(100), nullable=True)
    subcategory = Column(String(100), nullable=True)
    
    # Product flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_gift = Column(Boolean, nullable=False, default=False)
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow,
                       onupdate=datetime.utcnow)
    data_source = Column(String(50), nullable=False, default='CSV')
    
    __table_args__ = (
        Index('idx_dim_product_stock_code', 'stock_code'),  
        {'schema': 'retail_dw'}
    )

class TransactionType(PyEnum):
    SALE = "SALE"
    RETURN = "RETURN"

class FactSales(Base):
    """Sales fact table - core of the star schema"""
    
    __tablename__ = 'fact_sales'
    __table_args__ = {'schema': 'retail_dw'}
    
    # Surrogate key
    sales_key = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Foreign keys to dimensions
    date_key = Column(Integer, ForeignKey('retail_dw.dim_date.date_key'), 
                     nullable=False)
    customer_key = Column(BigInteger, ForeignKey('retail_dw.dim_customer.customer_key'), 
                         nullable=False)
    product_key = Column(BigInteger, ForeignKey('retail_dw.dim_product.product_key'), 
                        nullable=False)
    
    # Degenerate dimensions (no separate dimension table needed)
    invoice_no = Column(BigInteger, nullable=False, doc="Invoice number as integer")  

    transaction_type = Column(
        Enum('SALE', 'RETURN', name='transactiontype', schema='retail_dw'),
        nullable=False, 
        default='SALE'
    )    
    # Measures (facts)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    line_total = Column(Numeric(15, 2), nullable=False, 
                       doc="Calculated: quantity * unit_price")
    
    # Transaction details
    transaction_datetime = Column(DateTime, nullable=False,
                                doc="Original transaction timestamp")
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    batch_id = Column(String(100), nullable=True, 
                     doc="ETL batch identifier for lineage")
    data_source = Column(String(50), nullable=False, default='CSV')
    
    # Relationships
    date_dim = relationship("DimDate", backref="sales_facts")
    customer_dim = relationship("DimCustomer", backref="sales_facts")
    product_dim = relationship("DimProduct", backref="sales_facts")
    
    # Constraints
    __table_args__ = (
        CheckConstraint('quantity > 0', name='chk_quantity_positive'),
        CheckConstraint('unit_price >= 0', name='chk_unit_price_non_negative'),
        # Partitioning index on date_key for performance
        Index('idx_fact_sales_date_key', 'date_key'),
        Index('idx_fact_sales_customer_key', 'customer_key'),
        Index('idx_fact_sales_product_key', 'product_key'),
        Index('idx_fact_sales_invoice_no', 'invoice_no'),
        Index('idx_fact_sales_transaction_datetime', 'transaction_datetime'),
       
        {'schema': 'retail_dw'}
    )


class DataLineage(Base):
    """Data lineage tracking table"""
    
    __tablename__ = 'data_lineage'
    __table_args__ = {'schema': 'retail_dw'}
    
    lineage_id = Column(UUID(as_uuid=True), primary_key=True, 
                       default=uuid.uuid4)
    
    # Source information
    source_system = Column(String(100), nullable=False)
    source_table = Column(String(100), nullable=False)
    source_file = Column(String(500), nullable=True)
    
    # Target information
    target_table = Column(String(100), nullable=False)
    
    # ETL information
    etl_job_name = Column(String(100), nullable=False)
    batch_id = Column(String(100), nullable=False)
    records_processed = Column(BigInteger, nullable=False)
    records_inserted = Column(BigInteger, nullable=False)
    records_updated = Column(BigInteger, nullable=False)
    records_rejected = Column(BigInteger, nullable=False, default=0)
    
    # Timing
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    duration_seconds = Column(Integer, nullable=False)
    
    # Status
    status = Column(String(20), nullable=False)  # SUCCESS, FAILED, PARTIAL
    error_message = Column(String(1000), nullable=True)
    
    # Additional metadata - RENAMED from 'metadata' to avoid SQLAlchemy conflict
    job_metadata = Column(JSONB, nullable=True, 
                         doc="Additional job metadata in JSON format")
    
    # Indexing
    __table_args__ = (
        Index('idx_lineage_batch_id', 'batch_id'),
        Index('idx_lineage_target_table', 'target_table'),
        Index('idx_lineage_start_time', 'start_time'),
        Index('idx_lineage_status', 'status'),
        {'schema': 'retail_dw'}
    )


class DataQualityMetrics(Base):
    """Data quality metrics tracking"""
    
    __tablename__ = 'data_quality_metrics'
    __table_args__ = {'schema': 'retail_dw'}
    
    metric_id = Column(UUID(as_uuid=True), primary_key=True, 
                      default=uuid.uuid4)
    
    # Table and column information
    table_name = Column(String(100), nullable=False)
    column_name = Column(String(100), nullable=True)
    
    # Metric information
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Numeric(15, 4), nullable=False)
    threshold_value = Column(Numeric(15, 4), nullable=True)
    is_threshold_met = Column(Boolean, nullable=True)
    
    # Context
    batch_id = Column(String(100), nullable=False)
    measured_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Additional details
    details = Column(JSONB, nullable=True)
    
    # Indexing
    __table_args__ = (
        Index('idx_dq_metrics_table_metric', 'table_name', 'metric_name'),
        Index('idx_dq_metrics_batch_id', 'batch_id'),
        Index('idx_dq_metrics_measured_at', 'measured_at'),
        {'schema': 'retail_dw'}
    )