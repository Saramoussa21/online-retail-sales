# ... ...
from datetime import datetime
from decimal import Decimal
from typing import Optional
from enum import Enum as PyEnum
import uuid

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Boolean,
    ForeignKey, Index, UniqueConstraint, CheckConstraint,
    text, Date, BigInteger
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB

Base = declarative_base()


class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'retail_dw'}

    date_key = Column(Integer, primary_key=True, doc="Date key in YYYYMMDD format")
    date_value = Column(Date, nullable=False, unique=True, doc="Actual date value")

    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    day_of_year = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)

    month_name = Column(String(20), nullable=False)
    day_name = Column(String(20), nullable=False)
    quarter_name = Column(String(10), nullable=False)

    is_weekend = Column(Boolean, nullable=False, default=False)
    is_holiday = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index('idx_dim_date_year_month', 'year', 'month'),
        {'schema': 'retail_dw'}
    )


class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    __table_args__ = {'schema': 'retail_dw'}

    customer_key = Column(BigInteger, primary_key=True, autoincrement=True)
    customer_id = Column(String(50), nullable=False)

    country = Column(String(100), nullable=False)

    effective_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    expiry_date = Column(DateTime, nullable=True)
    # versioning fields
    version_id = Column(Integer, nullable=True)
    version_created_at = Column(DateTime, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_source = Column(String(50), nullable=False, default='CSV')

    __table_args__ = (
        Index('idx_dim_customer_id_current', 'customer_id', 'is_current'),
        {'schema': 'retail_dw'}
    )


class DimProduct(Base):
    __tablename__ = 'dim_product'
    __table_args__ = {'schema': 'retail_dw'}

    product_key = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_code = Column(String(50), nullable=False, unique=True)

    description = Column(String(255), nullable=False)
    # versioning fields
    version_id = Column(Integer, nullable=True)
    version_created_at = Column(DateTime, nullable=True)
    category = Column(String(100), nullable=True)
    subcategory = Column(String(100), nullable=True)

    is_active = Column(Boolean, nullable=False, default=True)
    is_gift = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_source = Column(String(50), nullable=False, default='CSV')

    __table_args__ = (
        Index('idx_dim_product_stock_code', 'stock_code'),
        {'schema': 'retail_dw'}
    )


class TransactionType(PyEnum):
    SALE = "SALE"
    RETURN = "RETURN"


class FactSales(Base):
    __tablename__ = 'fact_sales'
    __table_args__ = (
        CheckConstraint('quantity > 0', name='chk_quantity_positive'),
        CheckConstraint('unit_price >= 0', name='chk_unit_price_non_negative'),
        Index('idx_fact_sales_date_key', 'date_key'),
        Index('idx_fact_sales_customer_key', 'customer_key'),
        Index('idx_fact_sales_product_key', 'product_key'),
        Index('idx_fact_sales_invoice_no', 'invoice_no'),
        Index('idx_fact_sales_transaction_datetime', 'transaction_datetime'),
        {
            'schema': 'retail_dw',
            # Align partition key with schema SQL (use transaction_datetime)
            'postgresql_partition_by': 'RANGE (transaction_datetime)'
        }
    )

    # Composite primary key includes partition column date_key to satisfy Postgres
    sales_key = Column(BigInteger, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('retail_dw.dim_date.date_key'), primary_key=True, nullable=False)

    customer_key = Column(BigInteger, ForeignKey('retail_dw.dim_customer.customer_key'), nullable=False)
    product_key = Column(BigInteger, ForeignKey('retail_dw.dim_product.product_key'), nullable=False)

    invoice_no = Column(BigInteger, nullable=False)

    transaction_type = Column(
        SAEnum(TransactionType, name='transactiontype', schema='retail_dw'),
        nullable=False,
        default=TransactionType.SALE
    )

    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    line_total = Column(Numeric(15, 2), nullable=False)

    transaction_datetime = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    # versioning fields
    version_id = Column(Integer, nullable=True)
    version_created_at = Column(DateTime, nullable=True)
    batch_id = Column(String(100), nullable=True)
    data_source = Column(String(50), nullable=False, default='CSV')

    date_dim = relationship("DimDate", backref="sales_facts")
    customer_dim = relationship("DimCustomer", backref="sales_facts")
    product_dim = relationship("DimProduct", backref="sales_facts")


class DataLineage(Base):
    """Data lineage tracking table"""
    __tablename__ = 'data_lineage'
    __table_args__ = {'schema': 'retail_dw'}

    lineage_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    source_system = Column(String(100), nullable=False)
    source_table = Column(String(100), nullable=False)
    source_file = Column(String(500), nullable=True)

    target_table = Column(String(100), nullable=False)

    etl_job_name = Column(String(100), nullable=False)
    batch_id = Column(String(100), nullable=False)
    records_processed = Column(BigInteger, nullable=False)
    records_inserted = Column(BigInteger, nullable=False)
    records_updated = Column(BigInteger, nullable=False)
    records_rejected = Column(BigInteger, nullable=False, default=0)

    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    duration_seconds = Column(Integer, nullable=False)

    status = Column(String(20), nullable=False)
    error_message = Column(String(1000), nullable=True)

    job_metadata = Column(JSONB, nullable=True)

    __table_args__ = (
        Index('idx_lineage_batch_id', 'batch_id'),
        Index('idx_lineage_target_table', 'target_table'),
        Index('idx_lineage_start_time', 'start_time'),
        Index('idx_lineage_status', 'status'),
        {'schema': 'retail_dw'}
    )


class DataQualityMetrics(Base):
    __tablename__ = 'data_quality_metrics'
    __table_args__ = {'schema': 'retail_dw'}

    metric_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    table_name = Column(String(100), nullable=False)
    column_name = Column(String(100), nullable=True)

    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Numeric(15, 4), nullable=False)
    threshold_value = Column(Numeric(15, 4), nullable=True)
    is_threshold_met = Column(Boolean, nullable=True)

    batch_id = Column(String(100), nullable=False)
    measured_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    details = Column(JSONB, nullable=True)

    __table_args__ = (
        Index('idx_dq_metrics_table_metric', 'table_name', 'metric_name'),
        Index('idx_dq_metrics_batch_id', 'batch_id'),
        Index('idx_dq_metrics_measured_at', 'measured_at'),
        {'schema': 'retail_dw'}
    )