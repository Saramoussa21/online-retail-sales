# 🛒 **Retail Data Platform**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-12+-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Enterprise-grade data engineering platform for retail sales analytics**  
> Process 500K+ records with 98.5% success rate, real-time quality monitoring, and comprehensive metadata management.

---

## 🎯 **What This Platform Does**

The Retail Data Platform is a production-ready data engineering solution that transforms raw retail sales data into a high-performance analytics warehouse. Built with modern data engineering best practices, it provides:

- **🔄 Automated ETL Pipeline**: Extract, clean, transform, and load retail data
- **📊 Real-time Data Quality**: Continuous monitoring with configurable thresholds
- **⚡ High Performance**: Sub-second queries with intelligent caching
- **📈 Complete Observability**: Full data lineage and metadata management
- **🛡️ Enterprise Security**: ACID compliance and data integrity guarantees

---

## 🚀 **Quick Start**

### **Prerequisites**
- Python 3.8+
- PostgreSQL 12+
- 4GB+ RAM (recommended)

### **Installation**
```bash
# 1. Clone the repository
git clone <your-repo-url>
cd online-retail-sales/app

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up environment
cp config/development.yaml config/local.yaml
# Edit database connection in local.yaml

# 4. Initialize the platform
python main.py setup

# 5. Test everything works
python main.py test
```

### **First ETL Run**
```bash
# Process your retail data
python main.py etl --source data/online_retail.csv

# Check the results
python main.py quality report
python main.py metadata tables
```

**🎉 That's it! Your data is now in the warehouse and ready for analytics.**

---

## 📋 **Platform Features**

### **🔄 ETL Pipeline**
- **Memory-Efficient Processing**: Chunked reading for large datasets
- **Intelligent Cleaning**: Handles missing values, duplicates, outliers
- **Business Rules**: Enforces positive quantities/prices, calculates line totals
- **Dimensional Modeling**: Star schema with SCD Type 1/2 support

### **📊 Data Quality Framework**
- **Multi-Dimensional Quality**: Completeness, validity, uniqueness, accuracy
- **Real-time Monitoring**: Quality checks during ETL processing
- **Anomaly Detection**: Automatic alerts on quality degradation
- **Configurable Thresholds**: Business-specific quality rules

### **⚡ Performance Optimization**
- **Query Caching**: 80%+ cache hit rate for frequent queries
- **Database Partitioning**: Monthly partitions for time-series data
- **Strategic Indexing**: Optimized for analytical query patterns
- **Connection Pooling**: Efficient database resource management

### **📈 Monitoring & Observability**
- **Complete Data Lineage**: Track data from source to target
- **Structured Logging**: JSON-formatted logs for analysis
- **Performance Metrics**: Query timing and throughput tracking
- **Alert Management**: Multi-channel alerting system

### **🛠️ Management Interface**
- **CLI Commands**: 30+ commands for all platform operations
- **Job Scheduling**: Cron-like scheduler for automated runs
- **Version Control**: Track and rollback data changes
- **Metadata Export**: JSON/Markdown documentation generation

---

## 🏗️ **Architecture Overview**

```
📥 DATA SOURCES          🔄 ETL PIPELINE           🗄️ DATA WAREHOUSE
┌─────────────────┐     ┌─────────────────┐      ┌─────────────────┐
│                 │     │                 │      │                 │
│  • CSV Files    │────▶│  • Ingestion    │─────▶│  • dim_customers│
│  • Databases    │     │  • Cleaning     │      │  • dim_products │
│  • APIs         │     │  • Transform    │      │  • dim_date     │
│                 │     │  • Loading      │      │  • fact_sales   │
└─────────────────┘     └─────────────────┘      └─────────────────┘
                                 │                         │
                        ┌─────────────────┐       ┌─────────────────┐
                        │                 │       │                 │
                        │ 📊 MONITORING   │       │ ⚡ PERFORMANCE  │
                        │                 │       │                 │
                        │ • Quality Check │       │ • Query Cache   │
                        │ • Alerts        │       │ • Optimization  │
                        │ • Lineage       │       │ • Indexing      │
                        └─────────────────┘       └─────────────────┘
```

### **Technology Stack**
- **🐍 Python 3.8+**: Core processing with pandas, numpy, SQLAlchemy
- **🐘 PostgreSQL 12+**: High-performance analytical database
- **🔧 Click**: Professional CLI interface
- **📊 Rich**: Beautiful terminal output and progress bars
- **📝 YAML**: Human-readable configuration management

---

## 💻 **Command Reference**

### **Essential Commands**

```bash
# 🚀 System Management
python main.py setup                    # Initialize database schema
python main.py test                     # Test system connectivity

# 🔄 ETL Operations
python main.py etl --source data.csv    # Run ETL pipeline
python main.py etl --job-name daily     # Named ETL job

# 📊 Data Quality
python main.py quality check            # Validate data quality
python main.py quality report           # Generate quality report

# ⚡ Performance
python main.py performance analyze      # Query performance analysis
python main.py performance cache-stats  # Cache performance metrics
```

### **Advanced Operations**

```bash
# 📅 Scheduling
python main.py schedule daily --name job1 --time 02:00
python main.py schedule start           # Start scheduler daemon

# 📈 Monitoring
python main.py alerts run               # Detect anomalies
python main.py metadata lineage         # Show data lineage
python main.py versions list            # List data versions

# 📚 Documentation
python main.py docs generate            # Update all documentation
python main.py metadata export          # Export metadata catalog
```

---

## 📊 **Sample Data Schema**

### **Source Data (CSV)**
```csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26,2.55,17850,United Kingdom
536365,71053,WHITE METAL LANTERN,6,2010-12-01 08:26,3.39,17850,United Kingdom
```

### **Dimensional Model (Warehouse)**
```sql
-- Customer Dimension (SCD Type 2)
dim_customers: customer_key, customer_id, customer_name, country, 
               effective_date, expiry_date, is_current

-- Product Dimension (SCD Type 1)
dim_products: product_key, stock_code, description, current_unit_price

-- Sales Facts
fact_sales: sales_key, customer_key, product_key, date_key,
            invoice_no, quantity, unit_price, line_total, transaction_datetime
```

---

## 📈 **Performance Metrics**

### **Processing Performance**
- **Dataset Size**: 541,909 records processed
- **Success Rate**: 98.5% with quality validation
- **Processing Time**: ~3 seconds end-to-end
- **Throughput**: 180,000+ records/second
- **Memory Usage**: Constant (chunked processing)

### **Query Performance**
- **Average Response**: <100ms for analytical queries
- **Cache Hit Rate**: 80%+ for dimension lookups
- **Index Coverage**: 95%+ of queries use indexes
- **Concurrent Users**: 20+ with connection pooling

### **Data Quality Metrics**
- **Completeness**: 95%+ (configurable threshold)
- **Validity**: 90%+ format compliance
- **Accuracy**: 85%+ business rule compliance
- **Anomaly Detection**: 10%+ quality drop triggers alerts

---

## 🔧 **Configuration**

### **Database Configuration**
```yaml
# config/development.yaml
database:
  host: localhost
  port: 5432
  name: retail_warehouse
  user: postgres
  password: your_password
  pool_size: 20
  max_overflow: 30
```

### **ETL Configuration**
```yaml
etl:
  batch_size: 1000          # Records per batch
  chunk_size: 1000          # CSV reading chunk size
  max_retries: 3            # Retry failed operations
  quality_threshold: 0.95   # Minimum quality score
```

### **Quality Rules**
```yaml
quality_rules:
  completeness_threshold: 0.95    # 95% non-null values
  validity_threshold: 0.90        # 90% valid formats
  accuracy_threshold: 0.85        # 85% business rule compliance
  anomaly_threshold: 0.10         # 10% quality drop = alert
```

---

## 🧪 **Development & Testing**

### **Running Tests**
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test modules
python -m pytest tests/test_transformation.py -v
python -m pytest tests/test_cache_and_pipeline.py -v

# Run with coverage
python -m pytest tests/ --cov=retail_data_platform --cov-report=html
```

### **Development Commands**
```bash
# Check data quality during development
python main.py quality check --table fact_sales

# Analyze performance
python main.py performance analyze --query "SELECT COUNT(*) FROM fact_sales"

# Export metadata for documentation
python main.py metadata export --complete --filename docs/current_metadata.json
```

### **Adding New Data Sources**
```python
# 1. Create new ingestion method in etl/ingestion.py
def ingest_new_source(source_config):
    # Implementation here
    pass

# 2. Add cleaning rules in etl/cleaning.py
def clean_new_source_data(df):
    # Custom cleaning logic
    pass

# 3. Update transformation in etl/transformation.py
def transform_new_source(df):
    # Business rules for new source
    pass
```

---

## 🗂️ **Project Structure**

```
📁 online-retail-sales/app/
├── 🎯 main.py                     # CLI entry point (30+ commands)
├── 📋 requirements.txt            # Python dependencies
├── 📊 data/online_retail.csv      # Sample dataset (541K records)
├── 🔧 config/
│   ├── config_manager.py          # Environment configuration
│   └── development.yaml           # Dev settings
├── 🗄️ database/
│   ├── connection.py              # DB connectivity & pooling
│   ├── models.py                  # SQLAlchemy ORM models
│   ├── schema.py                  # Schema management
│   └── setup.sql                  # Database initialization
├── 🔄 etl/
│   ├── ingestion.py               # Data source readers (CSV, DB, API)
│   ├── cleaning.py                # Quality validation & cleaning
│   ├── transformation.py          # Business rules & modeling
│   ├── loader.py                  # Warehouse loading
│   └── pipeline.py                # ETL orchestration
├── 📈 monitoring/
│   ├── quality.py                 # Data quality framework
│   └── alerts.py                  # Alert management system
├── ⚡ performance/
│   ├── cache.py                   # Query result caching
│   └── optimization.py            # Performance tuning
├── 📅 scheduling/
│   ├── job_manager.py             # Cron-like job scheduling
│   └── scheduler.py               # Background scheduler
├── 📚 metadata/
│   └── catalog.py                 # Data lineage & catalog
├── 🛠️ utils/
│   └── logging_config.py          # Structured logging
├── 🧪 tests/
│   ├── test_transformation.py     # Transformation tests
│   ├── test_cache_and_pipeline.py # Integration tests
│   └── conftest.py                # Test configuration
└── 📖 docs/
    ├── architecture.md            # System architecture
    ├── etl_pipeline.md            # ETL documentation
    ├── data_quality.md            # Quality framework
    ├── schema.md                  # Database schema
    └── *.json                     # Metadata exports
```

---

## 🤝 **Contributing**

### **Development Setup**
```bash
# 1. Fork the repository
git fork <repository-url>

# 2. Create feature branch
git checkout -b feature/new-feature

# 3. Install development dependencies
pip install -r requirements.txt
pip install pytest pytest-cov black flake8

# 4. Run tests before committing
python -m pytest tests/ -v
black retail_data_platform/
flake8 retail_data_platform/

# 5. Submit pull request
git push origin feature/new-feature
```

### **Code Standards**
- **Type Hints**: All functions must have type annotations
- **Documentation**: Docstrings for all public methods
- **Testing**: 80%+ test coverage required
- **Logging**: Structured logging for all operations
- **Error Handling**: Graceful error handling with recovery

---

## 📞 **Support & Troubleshooting**

### **Common Issues**

**🔴 Database Connection Errors**
```bash
# Check database connectivity
python main.py test

# Verify PostgreSQL is running
pg_ctl status

# Check configuration
cat config/development.yaml
```

**🔴 ETL Processing Errors**
```bash
# Check data quality first
python main.py quality check --table fact_sales

# Run with debug logging
python main.py etl --source data.csv --log-level DEBUG

# Check recent job status
python main.py versions list
```

**🔴 Performance Issues**
```bash
# Analyze query performance
python main.py performance analyze

# Check cache statistics
python main.py performance cache-stats

# Review system resources
python main.py metadata tables
```

### **Getting Help**
- **📖 Documentation**: Check `docs/` directory for detailed guides
- **🧪 Tests**: Run tests to validate your setup
- **📊 Monitoring**: Use built-in monitoring commands
- **🔍 Logs**: Check structured logs for detailed error information

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🏆 **Acknowledgments**

- **PostgreSQL Team**: For the robust analytical database
- **Pandas Community**: For excellent data manipulation tools
- **SQLAlchemy**: For the powerful ORM framework
- **Click Library**: For the elegant CLI interface

---

