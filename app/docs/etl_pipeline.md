# 🔄 **ETL Pipeline Documentation**

## Overview

The Retail Data Platform implements a comprehensive ETL (Extract, Transform, Load) pipeline designed for processing online retail sales data. The pipeline follows enterprise data engineering best practices with modular components, error handling, and monitoring.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   Extract   │────│   Cleaning   │────│ Transform   │────│     Load     │
│             │    │              │    │             │    │              │
│ • CSV Files │    │ • Validation │    │ • Type Conv │    │ • Fact Table │
│ • Databases │    │ • Dedup      │    │ • Bus Rules │    │ • Dimensions │
│ • APIs      │    │ • Outliers   │    │ • Lookups   │    │ • Metadata   │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
       │                   │                   │                   │
       └───────────────────┼───────────────────┼───────────────────┘
                           │                   │
                    ┌──────────────┐    ┌─────────────┐
                    │  Monitoring  │    │   Logging   │
                    │              │    │             │
                    │ • Quality    │    │ • Structured│
                    │ • Performance│    │ • Metrics   │
                    │ • Alerts     │    │ • Lineage   │
                    └──────────────┘    └─────────────┘
```

## Pipeline Components

### 1. Data Ingestion (`retail_data_platform/etl/ingestion.py`)

#### Purpose
Reads data from various sources and performs initial validation.

#### Supported Sources
- **CSV Files**: Primary source for retail sales data
- **Database Queries**: For integration with existing systems
- **API Endpoints**: Extensible for real-time data feeds

#### Key Functions
```python
# Main ingestion function
def ingest_csv_data(file_path: str, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
    """Reads CSV data in chunks for memory efficiency"""

# Validation function  
def validate_source_data(df: pd.DataFrame) -> ValidationResult:
    """Validates data format and basic structure"""
```

#### Configuration Example
```python
source_config = {
    'type': 'csv',
    'name': 'retail_sales_csv',
    'file_path': 'data/online_retail.csv',
    'options': {
        'chunk_size': 1000,
        'encoding': 'utf-8',
        'delimiter': ',',
        'has_header': True
    }
}
```

### 2. Data Cleaning (`retail_data_platform/etl/cleaning.py`)

#### Purpose
Validates data quality, removes duplicates, and handles missing values.

#### Cleaning Rules
1. **Missing Value Handling**
   - CustomerID: Fill with 'GUEST'
   - Description: Fill with 'Unknown'
   - Quantity/Price: Drop record (critical fields)

2. **Data Validation**
   - Invoice format: `^[C]?\d{5,7}[A-Z]?$`
   - Positive quantities for sales
   - Non-negative prices
   - Valid date ranges

3. **Duplicate Detection**
   - Composite key: InvoiceNo + StockCode
   - Configurable strategies: keep_latest, keep_first, remove_all

#### Key Functions
```python
def clean_retail_data(df: pd.DataFrame) -> CleaningResult:
    """Main cleaning function with comprehensive validation"""

def remove_duplicates(df: pd.DataFrame, strategy: str = 'keep_latest') -> pd.DataFrame:
    """Remove duplicate records based on business rules"""

def validate_data_quality(df: pd.DataFrame) -> QualityReport:
    """Generate data quality metrics"""
```

### 3. Data Transformation (`retail_data_platform/etl/transformation.py`)

#### Purpose
Applies business rules, creates dimensional lookups, and prepares data for loading.

#### Transformation Types
1. **Data Type Conversion**
   - String to numeric conversion
   - Date parsing and standardization
   - Decimal precision handling

2. **Business Rule Application**
   - Line total calculation: `Quantity * UnitPrice`
   - Transaction type determination
   - Market segment classification

3. **Dimensional Modeling**
   - Surrogate key lookup/generation
   - SCD Type 2 processing for customers
   - Category derivation for products

#### Key Functions
```python
def transform_retail_data(df: pd.DataFrame) -> TransformationResult:
    """Main transformation function"""

def lookup_customer_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Get or create customer dimension keys"""

def lookup_product_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Get or create product dimension keys"""

def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Apply retail-specific business logic"""
```

### 4. Data Loading (`retail_data_platform/etl/pipeline.py`)

#### Purpose
Loads transformed data into the data warehouse with proper error handling.

#### Load Strategy
- **Batch Processing**: Configurable batch sizes
- **Transactional**: ACID compliance with rollback
- **Upsert Logic**: Insert new, update existing
- **Constraint Validation**: Foreign key and check constraints

#### Key Functions
```python
def load_to_warehouse(df: pd.DataFrame, batch_size: int = 1000) -> LoadResult:
    """Load data to warehouse tables"""

def load_fact_sales(df: pd.DataFrame) -> int:
    """Load fact table with proper key relationships"""
```

## ETL Job Execution

### Command Line Interface
```bash
# Basic ETL execution
python main.py etl --source data/online_retail.csv

# With custom configuration
python main.py etl --source data/online_retail.csv --job-name "daily_import" --batch-size 2000

# Dry run for validation
python main.py etl --source data/online_retail.csv --dry-run

# With quality validation
python main.py etl --source data/online_retail.csv --validate
```

### Programmatic Execution
```python
from retail_data_platform.etl.pipeline import ETLPipeline

# Create pipeline instance
pipeline = ETLPipeline()

# Execute ETL with configuration
result = pipeline.run_etl_job(
    source_file='data/online_retail.csv',
    job_name='manual_import',
    batch_size=1000,
    enable_validation=True
)

print(f"Records processed: {result.records_processed}")
print(f"Success rate: {result.success_rate}%")
print(f"Quality score: {result.quality_score}%")
```

### Job Configuration
```python
from retail_data_platform.etl.pipeline import ETLJobConfig

job_config = ETLJobConfig(
    job_name="retail_csv_import",
    source_config={
        'type': 'csv',
        'file_path': 'data/online_retail.csv',
        'encoding': 'utf-8'
    },
    batch_size=1000,
    max_retries=3,
    enable_monitoring=True,
    quality_threshold=0.95
)
```

## Pipeline Flow

### Step-by-Step Process
1. **Initialize Job**: Create unique job ID and start logging
2. **Extract Data**: Read source file in configurable chunks
3. **Validate Source**: Check file format and basic structure
4. **Clean Data**: Apply cleaning rules and quality checks
5. **Transform Data**: Apply business rules and dimensional lookups
6. **Load Data**: Insert/update warehouse tables
7. **Update Metadata**: Record lineage and job metrics
8. **Generate Report**: Create quality and performance report

### Data Flow Diagram
```
CSV File (541K records)
    ↓
Ingestion (chunked reading)
    ↓
Cleaning (validation + deduplication)
    ↓ (98.5% pass rate)
Transformation (business rules + lookups)
    ↓
Loading (batch insert to warehouse)
    ↓
Metadata Recording (lineage + metrics)
    ↓
Quality Report (completeness + accuracy)
```

## Error Handling

### Error Types
1. **Connection Errors**: Database connectivity issues
2. **Validation Errors**: Data quality rule violations
3. **Transformation Errors**: Business logic failures
4. **Loading Errors**: Database constraint violations

### Error Recovery
- **Retry Logic**: Exponential backoff for transient errors
- **Checkpointing**: Resume from last successful batch
- **Partial Success**: Continue processing valid records
- **Error Logging**: Comprehensive error documentation

### Monitoring Commands
```bash
# Check recent jobs
python main.py monitor --jobs 5

# Monitor specific job
python main.py monitor --job-id etl_20241008_123456

# Check job status
python main.py versions list
```

## Performance Optimization

### Batch Size Tuning
```python
# Small files: 500-1000 records per batch
# Large files: 2000-5000 records per batch
# Memory constrained: 100-500 records per batch

# Optimal for 541K retail dataset:
batch_size = 1000  # Good balance of memory and performance
```

### Database Optimization
```sql
-- Index recommendations for fact_sales
CREATE INDEX idx_fact_sales_date ON retail_dw.fact_sales (transaction_datetime);
CREATE INDEX idx_fact_sales_customer ON retail_dw.fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product ON retail_dw.fact_sales (product_key);

-- Performance monitoring
python main.py performance audit
```

## Data Quality Integration

### Quality Checks During ETL
- **Completeness**: Check for required fields
- **Validity**: Validate data types and formats
- **Uniqueness**: Detect and handle duplicates
- **Accuracy**: Apply business rule validation
- **Consistency**: Cross-reference dimensional data

### Quality Thresholds
```python
quality_thresholds = {
    'completeness': 0.95,    # 95% of records must be complete
    'validity': 0.90,        # 90% must pass validation rules
    'uniqueness': 0.98,      # 98% must be unique (allow some duplicates)
    'accuracy': 0.85         # 85% must pass business rules
}
```

## Scheduling Integration

### Automated ETL Jobs
```bash
# Schedule daily ETL at 2 AM
python main.py schedule add --name daily_etl --cron "0 2 * * *" --command "etl --source data/online_retail.csv"

# View scheduled jobs
python main.py schedule list

# Start scheduler
python main.py schedule start
```

### Job Dependencies
```python
# Schedule with dependencies
python main.py schedule add \
    --name weekly_etl \
    --cron "0 1 * * SUN" \
    --command "etl --source data/weekly_data.csv" \
    --depends-on daily_etl
```

## Troubleshooting

### Common Issues
1. **Memory Errors**: Reduce batch size or chunk size
2. **Connection Timeouts**: Increase connection pool size
3. **Data Quality Failures**: Review validation rules and thresholds
4. **Performance Issues**: Analyze query execution plans

### Debug Commands
```bash
# Test system connectivity
python main.py test --verbose

# Check data quality
python main.py quality check --table fact_sales

# Performance analysis
python main.py performance audit

# View recent job logs
python main.py monitor --jobs 5 --verbose
```

### Log Analysis
```bash
# Check ETL logs for errors
grep "ERROR" logs/etl_*.log

# Monitor data quality trends
python main.py quality report --days 7

# Performance metrics
python main.py query --sql "SELECT AVG(processing_time_seconds) FROM retail_dw.etl_job_runs WHERE job_date >= CURRENT_DATE - 7"
```

## Best Practices

### Development
1. **Test with small datasets** before processing large files
2. **Use dry-run mode** to validate transformations
3. **Monitor quality metrics** continuously
4. **Set appropriate batch sizes** for your environment

### Production
1. **Schedule ETL jobs** during low-usage periods
2. **Monitor disk space** for large datasets
3. **Set up alerting** for job failures
4. **Backup data** before major processing

### Performance
1. **Tune batch sizes** based on available memory
2. **Use database indexes** for lookup operations
3. **Monitor connection pools** for bottlenecks
4. **Archive old data** to maintain performance

---

**This ETL pipeline processes 541,909 retail records with 98.5% success rate and comprehensive monitoring.**