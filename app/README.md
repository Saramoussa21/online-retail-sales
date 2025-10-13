# 🏪 **Retail Data Platform - Complete ETL Solution**

A production-ready data engineering platform for processing online retail sales data with comprehensive ETL pipelines, data quality monitoring, and performance optimization.

## 🎯 **What This Platform Does**

- **Processes retail sales data** from CSV files into a dimensional data warehouse
- **Monitors data quality** with automated alerts and comprehensive metrics
- **Optimizes performance** with intelligent indexing and caching
- **Tracks data lineage** for complete audit trails
- **Provides scheduling** for automated ETL jobs
- **Offers monitoring** with real-time dashboards and alerts

---

## 🚀 **Quick Start (5 Minutes)**

### **Prerequisites**
```bash
- Python 3.9+
- PostgreSQL 12+
- 4GB RAM minimum
```

### **1. Setup Environment**
```bash
# Clone and enter directory
cd retail-data-platform/app

# Create virtual environment
python -m venv retail_env
retail_env\Scripts\activate  # Windows
# source retail_env/bin/activate  # Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

### **2. Configure Database**
```bash
# Create PostgreSQL database
createdb -U postgres ors

# Copy environment template
copy .env.example .env  # Windows
# cp .env.example .env  # Linux/macOS

# Edit .env with your database credentials:
# DB_HOST=localhost
# DB_USER=postgres
# DB_PASSWORD=your_password
```

### **3. Initialize Platform**
```bash
# Create database schema
python main.py setup

# Verify installation
python main.py test

# Expected output: ✅ All systems operational
```

### **4. Run First ETL Job**
```bash
# Process sample retail data
python main.py etl --source data/online_retail.csv

# Check results
python main.py query --table fact_sales --limit 5
python main.py quality dashboard
```

**🎉 You're ready! Your retail data platform is operational.**

---

## 📊 **What Gets Created**

### **Database Schema**
- **`fact_sales`**: Central sales transactions (541K+ records)
- **`dim_customer`**: Customer dimension with SCD Type 2
- **`dim_product`**: Product catalog with categories
- **`dim_date`**: Date dimension for time analysis
- **`data_versions`**: ETL job tracking and versioning

### **Monitoring Tables**
- **`data_quality_metrics`**: Quality scores and trends
- **`data_lineage`**: Complete data transformation tracking
- **`etl_job_runs`**: Job execution history and performance

---

## 🎛️ **Command Reference**

### **Setup & Testing**
```bash
# Initialize everything
python main.py setup                    # Create database schema
python main.py setup --drop-existing    # Fresh start (⚠️ destroys data)
python main.py test                     # Verify all systems
python main.py test --verbose           # Detailed diagnostics
```

### **ETL Operations**
```bash
# Run ETL pipeline
python main.py etl --source data/online_retail.csv
python main.py etl --source data/file.csv --job-name my_job
python main.py etl --source data/file.csv --batch-size 500

# ETL with quality checks
python main.py etl --source data/file.csv --validate
```

### **Data Quality**
```bash
# Quality monitoring
python main.py quality check --table fact_sales
python main.py quality dashboard
python main.py quality report --days 7

# Quality thresholds
python main.py quality rules --show
python main.py quality rules --set completeness=0.95
```

### **Performance & Monitoring**
```bash
# Performance analysis
python main.py performance audit
python main.py performance indexes --recommend
python main.py performance cache --stats

# Job monitoring
python main.py monitor --jobs 5
python main.py monitor --job-id etl_20241008_123456
```

### **Data Querying**
```bash
# Query data warehouse
python main.py query --table fact_sales --limit 10
python main.py query --table dim_customer --where "country='United Kingdom'"
python main.py query --sql "SELECT COUNT(*) FROM retail_dw.fact_sales"
```

### **Scheduling & Automation**
```bash
# Schedule management
python main.py schedule list
python main.py schedule add --name daily_etl --cron "0 2 * * *"
python main.py schedule run --name daily_etl
```

### **Metadata & Documentation**
```bash
# Data catalog
python main.py metadata dictionary
python main.py metadata lineage --table fact_sales
python main.py metadata export --format json

# Versioning
python main.py versions list
python main.py versions current
python main.py versions partitions
```

---

## 📁 **Project Structure**

```
retail-data-platform/app/
├── 📄 main.py                          # CLI entry point - all commands
├── 📄 requirements.txt                 # Python dependencies
├── 📄 .env.example                     # Environment template
├── 📁 retail_data_platform/            # Core platform code
│   ├── 📁 config/                      # Configuration management
│   │   ├── config_manager.py          # YAML config loader
│   │   └── development.yaml           # Environment settings
│   ├── 📁 database/                    # Database layer
│   │   ├── connection.py              # Connection pooling
│   │   ├── models.py                  # SQLAlchemy ORM models
│   │   └── schema.py                  # Schema creation/management
│   ├── 📁 etl/                        # ETL pipeline components
│   │   ├── ingestion.py               # CSV/data source readers
│   │   ├── cleaning.py                # Data validation/cleaning
│   │   ├── transformation.py          # Business rules/SCD logic
│   │   └── pipeline.py                # ETL orchestration
│   ├── 📁 monitoring/                 # Data quality monitoring
│   │   ├── quality.py                 # Quality metrics/rules
│   │   └── alerts.py                  # Alert system
│   ├── 📁 performance/                # Performance optimization
│   │   ├── optimization.py            # Query optimization
│   │   └── cache.py                   # Caching layer
│   ├── 📁 scheduling/                 # Job scheduling
│   │   ├── scheduler.py               # Cron-like scheduler
│   │   └── job_manager.py             # Job lifecycle management
│   ├── 📁 metadata/                   # Data catalog/lineage
│   │   └── catalog.py                 # Metadata management
│   └── 📁 utils/                      # Utilities
│       └── logging_config.py          # Structured logging
├── 📁 data/                           # Sample datasets
│   ├── online_retail.csv             # Sample retail data (541K records)
│   └── Online Retail Sales Data Engineering.md  # Data description
├── 📁 docs/                          # Documentation
│   ├── schema.md                     # Database schema guide
│   ├── etl_pipeline.md               # ETL process documentation
│   ├── deployment.md                 # Production deployment
│   └── scheduling.md                 # Job scheduling guide
└── 📁 scripts/                       # Utility scripts
    └── setup_database.py             # Standalone database setup
```

---

## 🔧 **Configuration**

### **Environment Variables (.env)**
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ors
DB_USER=postgres
DB_PASSWORD=your_password

# ETL Configuration
ETL_BATCH_SIZE=1000
ETL_MAX_RETRIES=3

# Quality Monitoring
MONITORING_QUALITY_THRESHOLD=0.95
MONITORING_RETENTION_DAYS=30
```

### **YAML Configuration (retail_data_platform/config/development.yaml)**
```yaml
database:
  host: "${DB_HOST:localhost}"
  port: ${DB_PORT:5432}
  name: "${DB_NAME:ors}"
  user: "${DB_USER:postgres}"
  password: "${DB_PASSWORD}"
  pool_size: 10

etl:
  batch_size: ${ETL_BATCH_SIZE:1000}
  max_retries: ${ETL_MAX_RETRIES:3}
  enable_parallel: false

monitoring:
  quality_threshold: ${MONITORING_QUALITY_THRESHOLD:0.95}
  retention_days: ${MONITORING_RETENTION_DAYS:30}
```

**💡 Use .env for sensitive values, YAML for application settings.**

---

## 📊 **ETL Pipeline Flow**

```
📥 CSV File → 🔍 Ingestion → 🧹 Cleaning → 🔄 Transformation → 📊 Loading
                    ↓              ↓             ↓            ↓
              📋 Validation    🚨 Quality    📈 Business    💾 Warehouse
                                 Rules         Logic        + Metadata
```

### **Processing Steps:**
1. **Ingestion**: Read CSV, validate format, detect encoding
2. **Cleaning**: Remove duplicates, validate data types, handle nulls
3. **Transformation**: Apply business rules, lookup dimensions, calculate metrics
4. **Loading**: Insert to warehouse with SCD processing and versioning
5. **Quality**: Validate completeness, accuracy, and business rules
6. **Monitoring**: Track lineage, performance, and data quality metrics

---

## 📈 **Sample Analytics Queries**

```sql
-- Top selling products
SELECT p.product_description, SUM(f.line_total) as revenue
FROM retail_dw.fact_sales f
JOIN retail_dw.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_description
ORDER BY revenue DESC LIMIT 10;

-- Monthly sales trends
SELECT d.year, d.month, COUNT(*) as transactions, SUM(f.line_total) as revenue
FROM retail_dw.fact_sales f
JOIN retail_dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Customer segmentation
SELECT c.country, COUNT(DISTINCT c.customer_key) as customers, SUM(f.line_total) as revenue
FROM retail_dw.fact_sales f
JOIN retail_dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY revenue DESC;
```

---

## 🛠️ **Troubleshooting**

### **Common Issues**

| Issue | Solution |
|-------|----------|
| Database connection failed | Check PostgreSQL service, verify credentials in .env |
| Import errors | Activate virtual environment: `retail_env\Scripts\activate` |
| ETL job fails | Check `python main.py test --verbose` for diagnostics |
| Performance slow | Run `python main.py performance audit` for recommendations |
| Quality checks fail | Review rules: `python main.py quality rules --show` |

### **Diagnostic Commands**
```bash
# Full system check
python main.py test --verbose

# Check database connectivity
python main.py query --sql "SELECT version();"

# View recent logs
python main.py monitor --jobs 5

# Performance analysis
python main.py performance audit
```

---

## 📚 **Documentation**

- **[Database Schema](docs/schema.md)**: Complete table structure and relationships
- **[ETL Pipeline](docs/etl_pipeline.md)**: Detailed ETL process documentation
- **[Deployment Guide](docs/deployment.md)**: Production deployment instructions
- **[Scheduling Guide](docs/scheduling.md)**: Automated job scheduling setup

---

## 🎯 **Next Steps & Recommendations**

### **Performance Optimizations**
- Implement partitioning for large fact tables
- Add materialized views for common aggregations
- Configure query result caching
- Set up read replicas for analytics workloads

### **Monitoring Enhancements**
- Add real-time alerting (email/Slack integration)
- Create business KPI dashboards
- Implement anomaly detection for data quality
- Set up automated data validation reports

### **Scalability Improvements**
- Add support for multiple data sources (APIs, databases)
- Implement parallel processing for large datasets
- Add data archiving and retention policies
- Create automated backup and recovery procedures

### **Analytics Extensions**
- Build dimensional models for specific business domains
- Add machine learning pipeline integration
- Create automated report generation
- Implement real-time streaming data processing

---

## 📞 **Support**

### **Getting Help**
1. **Check documentation**: Review files in `docs/` directory
2. **Run diagnostics**: `python main.py test --verbose`
3. **View logs**: Check console output for detailed error information
4. **Test configuration**: Verify `.env` and YAML settings

### **Development**
- Built with Python 3.9+ and PostgreSQL 12+
- Uses SQLAlchemy ORM for database operations
- Implements Click for CLI interface
- Structured logging with JSON output

---

**🎉 Ready to process your retail data! This platform scales from development to production with enterprise-grade features.**

---
