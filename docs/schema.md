# Data Warehouse Schema Documentation

## Overview

The retail data warehouse implements a star schema design optimized for analytical queries and reporting. The schema follows dimensional modeling best practices with separate fact and dimension tables.

## Schema Architecture

```
                    DIM_DATE
                        |
                        |
DIM_CUSTOMER ---- FACT_SALES ---- DIM_PRODUCT
                        |
                   (Degenerate Dimensions)
                   - invoice_no
                   - batch_id
```

## Tables

### Fact Tables

#### fact_sales
**Purpose**: Core fact table storing retail sales transactions
**Grain**: One row per line item on an invoice

| Column | Type | Description |
|--------|------|-------------|
| sales_key | BIGINT | Surrogate key (Primary Key) |
| date_key | INTEGER | Foreign key to dim_date |
| customer_key | BIGINT | Foreign key to dim_customer |
| product_key | BIGINT | Foreign key to dim_product |
| invoice_no | VARCHAR(50) | Degenerate dimension - invoice number |
| quantity | INTEGER | Quantity sold (negative for returns) |
| unit_price | NUMERIC(10,2) | Unit price at time of sale |
| line_total | NUMERIC(15,2) | Calculated total (quantity × unit_price) |
| transaction_datetime | TIMESTAMP | Original transaction timestamp |
| created_at | TIMESTAMP | Record creation timestamp |
| batch_id | VARCHAR(100) | ETL batch identifier |
| data_source | VARCHAR(50) | Source system identifier |

**Key Business Rules**:
- `line_total` is automatically calculated via trigger
- Negative quantities indicate returns/cancellations
- `invoice_no` starting with 'C' indicates cancellations

### Dimension Tables

#### dim_date
**Purpose**: Date dimension for time-based analysis
**Type**: Type 1 SCD (Static)

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Surrogate key in YYYYMMDD format |
| date_value | DATE | Actual date value |
| year | INTEGER | Year (2010-2025) |
| quarter | INTEGER | Quarter (1-4) |
| month | INTEGER | Month (1-12) |
| week | INTEGER | Week of year |
| day_of_year | INTEGER | Day of year (1-366) |
| day_of_month | INTEGER | Day of month (1-31) |
| day_of_week | INTEGER | Day of week (0=Sunday, 6=Saturday) |
| month_name | VARCHAR(20) | Month name |
| day_name | VARCHAR(20) | Day name |
| quarter_name | VARCHAR(10) | Quarter name (Q1, Q2, etc.) |
| is_weekend | BOOLEAN | Weekend flag |
| is_holiday | BOOLEAN | Holiday flag |

#### dim_customer
**Purpose**: Customer dimension with SCD Type 2 support
**Type**: Type 2 SCD (Historical tracking)

| Column | Type | Description |
|--------|------|-------------|
| customer_key | BIGINT | Surrogate key (Primary Key) |
| customer_id | VARCHAR(50) | Natural key from source |
| country | VARCHAR(100) | Customer country |
| effective_date | TIMESTAMP | Record effective date |
| expiry_date | TIMESTAMP | Record expiry date (NULL for current) |
| is_current | BOOLEAN | Current record flag |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |
| data_source | VARCHAR(50) | Source system identifier |

**SCD Type 2 Implementation**:
- Country changes create new records
- Only one record per customer_id can have `is_current = true`
- Historical records have `expiry_date` populated

#### dim_product
**Purpose**: Product dimension
**Type**: Type 1 SCD (Overwrite)

| Column | Type | Description |
|--------|------|-------------|
| product_key | BIGINT | Surrogate key (Primary Key) |
| stock_code | VARCHAR(50) | Natural key - unique stock code |
| description | VARCHAR(500) | Product description |
| category | VARCHAR(100) | Derived product category |
| subcategory | VARCHAR(100) | Derived subcategory |
| is_active | BOOLEAN | Active product flag |
| is_gift | BOOLEAN | Gift item flag |
| created_at | TIMESTAMP | Record creation timestamp |
| updated_at | TIMESTAMP | Last update timestamp |
| data_source | VARCHAR(50) | Source system identifier |

**Category Derivation Rules**:
- Categories derived from product descriptions using pattern matching
- Common categories: Home Decor, Drinkware, Bags & Storage, Lighting, etc.

### Metadata Tables

#### data_lineage
**Purpose**: Track ETL job execution and data lineage

| Column | Type | Description |
|--------|------|-------------|
| lineage_id | UUID | Primary key |
| source_system | VARCHAR(100) | Source system name |
| source_table | VARCHAR(100) | Source table name |
| source_file | VARCHAR(500) | Source file path |
| target_table | VARCHAR(100) | Target table name |
| etl_job_name | VARCHAR(100) | ETL job identifier |
| batch_id | VARCHAR(100) | Batch identifier |
| records_processed | BIGINT | Total records processed |
| records_inserted | BIGINT | Records successfully inserted |
| records_updated | BIGINT | Records updated |
| records_rejected | BIGINT | Records rejected |
| start_time | TIMESTAMP | Job start time |
| end_time | TIMESTAMP | Job end time |
| duration_seconds | INTEGER | Job duration |
| status | VARCHAR(20) | Job status |
| error_message | VARCHAR(1000) | Error details |
| metadata | JSONB | Additional job metadata |

#### data_quality_metrics
**Purpose**: Store data quality check results

| Column | Type | Description |
|--------|------|-------------|
| metric_id | UUID | Primary key |
| table_name | VARCHAR(100) | Table being checked |
| column_name | VARCHAR(100) | Column being checked |
| metric_name | VARCHAR(100) | Quality metric name |
| metric_value | NUMERIC(15,4) | Metric value |
| threshold_value | NUMERIC(15,4) | Threshold for comparison |
| is_threshold_met | BOOLEAN | Whether threshold was met |
| batch_id | VARCHAR(100) | ETL batch identifier |
| measured_at | TIMESTAMP | Measurement timestamp |
| details | JSONB | Additional metric details |

## Indexes

### Primary Indexes
- All surrogate keys have primary key constraints
- Unique constraints on natural keys where appropriate

### Performance Indexes
```sql
-- Fact table indexes
CREATE INDEX idx_fact_sales_date_key ON fact_sales (date_key);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales (product_key);
CREATE INDEX idx_fact_sales_invoice_no ON fact_sales (invoice_no);
CREATE INDEX idx_fact_sales_transaction_datetime ON fact_sales (transaction_datetime);

-- Composite indexes for common queries
CREATE INDEX idx_fact_sales_date_customer ON fact_sales (date_key, customer_key);
CREATE INDEX idx_fact_sales_date_product ON fact_sales (date_key, product_key);

-- Dimension indexes
CREATE INDEX idx_dim_customer_id_current ON dim_customer (customer_id, is_current);
CREATE INDEX idx_dim_customer_country ON dim_customer (country);
CREATE INDEX idx_dim_product_stock_code ON dim_product (stock_code);
CREATE INDEX idx_dim_product_category ON dim_product (category);

-- Date dimension indexes
CREATE INDEX idx_dim_date_year_month ON dim_date (year, month);
CREATE INDEX idx_dim_date_quarter ON dim_date (year, quarter);
```

### Completeness
- Invoice number: 95% threshold
- Product key: 95% threshold
- Customer key: 80% threshold (allows for guest purchases)

### Validity
- Quantity range: -1000 to 10000
- Unit price range: 0 to 1000
- Date validity: Must be valid date format

### Uniqueness
- Transaction uniqueness: 99% threshold
- Customer ID uniqueness: 100% threshold
- Stock code uniqueness: 100% threshold

## Performance Considerations

### Partitioning Strategy
- Consider partitioning `fact_sales` by year for large datasets
- Use constraint exclusion for query optimization

### Query Optimization
- Use materialized views for common aggregations
- Implement query result caching for frequently accessed data
- Monitor and optimize slow queries

### Maintenance
- Regular VACUUM and ANALYZE operations
- Monitor index usage and remove unused indexes
- Refresh materialized views during off-peak hours