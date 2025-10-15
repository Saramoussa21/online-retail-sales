# 📋 **CLI Commands Reference Guide**

## 🎯 **Quick Start Commands**

```bash
# Complete setup from scratch
python main.py setup                    # Initialize database schema
python main.py test                     # Verify system connectivity
python main.py etl --source data/online_retail.csv  # Run ETL pipeline

# Daily operations
python main.py quality check            # Check data quality
python main.py performance analyze      # Performance analysis
python main.py versions list            # View data versions
```

---

## 🏗️ **System Management**

### **setup** - Database Initialization
```bash
# Basic setup (creates schema, tables, indexes)
python main.py setup

# Destructive setup (drops existing data)
python main.py setup --drop-existing
# ⚠️  WARNING: This deletes ALL existing data!
```

**What it does:**
- Creates `retail_dw` schema
- Sets up dimensional tables (customers, products, dates)
- Creates fact table with partitioning
- Establishes indexes and constraints
- Initializes metadata tables

### **test** - System Health Check
```bash
# Test all system components
python main.py test
```

**Validates:**
- Database connectivity
- Configuration loading
- Schema existence
- Connection pool health

---

## 🔄 **ETL Operations**

### **etl** - Extract, Transform, Load
```bash
# Basic ETL execution
python main.py etl --source data/online_retail.csv

# Custom job name and batch size
python main.py etl \
  --source data/online_retail.csv \
  --job-name "monthly_import" \
  --batch-size 2000

# Large file processing (smaller batches)
python main.py etl \
  --source data/large_file.csv \
  --batch-size 500
```

**Process Flow:**
1. **Extract**: Read CSV in configurable chunks
2. **Validate**: Check data format and structure
3. **Clean**: Remove duplicates, handle missing values, ensure positive values
4. **Transform**: Apply business rules, create dimensional lookups
5. **Load**: Insert into warehouse with transaction integrity
6. **Monitor**: Record quality metrics and lineage

**Output Example:**
```
Starting ETL job: retail_etl_20241015_143022
Job ID: etl_20241015_143022
Status: SUCCESS
Records Loaded: 541909
Duration: 3.45s
Version: v1.2
Version ID: 12
```

---

## 📅 **Scheduling Commands**

### **schedule** - Job Automation

#### **Add Daily Job**
```bash
# Schedule daily ETL at 2:00 AM
python main.py schedule daily \
  --name "daily_sales_import" \
  --csv-path "data/daily_sales.csv" \
  --time "02:00"

# Schedule at different time
python main.py schedule daily \
  --name "evening_import" \
  --csv-path "data/evening_sales.csv" \
  --time "18:30"
```

#### **List Scheduled Jobs**
```bash
python main.py schedule list
```

**Output:**
```
Scheduled Jobs:
- daily_sales_import: 02:00 daily (data/daily_sales.csv)
- evening_import: 18:30 daily (data/evening_sales.csv)
```

#### **Start Scheduler Daemon**
```bash
# Start scheduler (runs continuously)
python main.py schedule start
# Press Ctrl+C to stop
```

---

## ⚡ **Performance Commands**

### **performance** - Query Optimization

#### **Analyze Query Performance**
```bash
# Analyze default query
python main.py performance analyze

# Analyze custom query
python main.py performance analyze \
  --query "SELECT COUNT(*) FROM retail_dw.fact_sales WHERE transaction_datetime >= '2023-01-01'"
```

**Output:**
```
Execution Time: 45.23 ms
Rows Returned: 1
Cache Hit: false
```

#### **Cache Statistics**
```bash
python main.py performance cache-stats
```

**Output:**
```
Total Entries: 15
Hit Ratio: 82.3%
```

---

## 📊 **Data Quality Commands**

### **quality** - Data Validation

#### **Quick Quality Check**
```bash
# Check default table (fact_sales)
python main.py quality check

# Check specific table
python main.py quality check --table dim_customers
```

**Output:**
```
Checks run: 8, Passed: 7
Quality Score: 87.5%
```

#### **Detailed Quality Report**
```bash
# Generate comprehensive report
python main.py quality report

# For specific table
python main.py quality report --table dim_products
```

**Sample Report:**
```
Data Quality Report for fact_sales
=====================================
Completeness: 98.2% (threshold: 95%) ✅
Validity: 94.1% (threshold: 90%) ✅  
Uniqueness: 99.8% (threshold: 98%) ✅
Accuracy: 96.5% (threshold: 85%) ✅

Issues Found:
- 892 records with missing CustomerID
- 1,245 records with invalid stock codes
```

---

## 📚 **Metadata Commands**

### **metadata** - Data Catalog & Lineage

#### **Table Information**
```bash
# List all tables with sizes
python main.py metadata tables

# Show specific table schema
python main.py metadata tables --table fact_sales
```

**Output:**
```
Tables summary:
  fact_sales: size=125MB
  dim_customers: size=2.1MB
  dim_products: size=890KB
  dim_date: size=45KB
```

#### **Data Lineage**
```bash
# Show recent data lineage (last 5 jobs)
python main.py metadata lineage
```

**Output:**
```
retail_etl_20241015_143022: 541909 records -> fact_sales
daily_import_20241015_020003: 15430 records -> fact_sales
evening_import_20241014_183015: 8920 records -> fact_sales
```

#### **Export Metadata**
```bash
# Export data dictionary only
python main.py metadata export

# Export complete metadata repository
python main.py metadata export --complete

# Export to specific file
python main.py metadata export \
  --complete \
  --filename "metadata_backup_20241015.json"
```

**Output:**
```
✅ Metadata exported to: docs/metadata_repository_20241015_143045.json
```

---

## 🚨 **Alert Commands**

### **alerts** - Anomaly Detection

#### **Run Anomaly Detection**
```bash
# Detect anomalies across all tables
python main.py alerts run

# For specific table only
python main.py alerts run --table fact_sales

# Show anomalies in output
python main.py alerts run --show
```

**Output with --show:**
```
Processed 3 anomalies and triggered alerts (see logs)
Anomalies:
- table=fact_sales metric=completeness current=87.2 prev=95.1 drop=7.90 severity=HIGH
- table=dim_customers metric=validity current=92.3 prev=98.1 drop=5.80 severity=MEDIUM
```

#### **Test Alert System**
```bash
# Send test alert (default: CRITICAL level)
python main.py alerts test

# Send with custom level and message
python main.py alerts test \
  --level WARNING \
  --message "System maintenance scheduled"
```

---

## 📦 **Version Management**

### **versions** - Data Version Control

#### **List Data Versions**
```bash
# Show last 10 versions (default)
python main.py versions list

# Show more versions
python main.py versions list --limit 20
```

**Output:**
```
v1.5 | 541909 rows | SUCCESS | 2024-10-15 14:30:22
v1.4 | 523450 rows | SUCCESS | 2024-10-14 02:00:15
v1.3 | 501203 rows | SUCCESS | 2024-10-13 02:00:08
v1.2 | 487950 rows | FAILED  | 2024-10-12 02:00:12
```

#### **Show Version Details**
```bash
# Show specific version information
python main.py versions show v1.5
```

**Output:**
```
v1.5 | 541909 rows | SUCCESS | 2024-10-15 14:30:22
Job: retail_etl_20241015_143022
Duration: 3.45s
Quality Score: 96.8%
```

---

## 🔧 **Configuration Options**

### **Global Options**
These options can be used with any command:

```bash
# Custom configuration file
python main.py --config config/production.yaml [command]

# Set logging level
python main.py --log-level DEBUG [command]

# Set environment
python main.py --environment production [command]
```

### **Environment Variables**
```bash
# Database configuration
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=retail_db
export DB_USER=retail_user
export DB_PASSWORD=secret

# Application settings
export ENVIRONMENT=production
export LOG_LEVEL=INFO
export BATCH_SIZE=1000
```

---

## 🎯 **Common Workflows**

### **Daily Operations Workflow**
```bash
# 1. Check system health
python main.py test

# 2. Run ETL for new data
python main.py etl --source data/daily_sales_20241015.csv

# 3. Validate data quality
python main.py quality check

# 4. Check for anomalies
python main.py alerts run

# 5. View processing results
python main.py versions list --limit 1
```

### **Weekly Monitoring Workflow**
```bash
# 1. Generate quality report
python main.py quality report > reports/weekly_quality_$(date +%Y%m%d).txt

# 2. Check performance trends
python main.py performance cache-stats

# 3. Export metadata backup
python main.py metadata export --complete --filename backups/metadata_$(date +%Y%m%d).json

# 4. Review data lineage
python main.py metadata lineage
```

### **Troubleshooting Workflow**
```bash
# 1. Test system connectivity
python main.py test

# 2. Check recent job status
python main.py versions list --limit 5

# 3. Run anomaly detection
python main.py alerts run --show

# 4. Analyze performance issues
python main.py performance analyze

# 5. Generate detailed quality report
python main.py quality report --table fact_sales
```

### **New Environment Setup**
```bash
# 1. Initialize database
python main.py setup

# 2. Test connectivity
python main.py test

# 3. Run initial data load
python main.py etl --source data/historical_data.csv --job-name initial_load

# 4. Set up scheduling
python main.py schedule daily --name daily_etl --csv-path data/daily.csv --time 02:00

# 5. Start scheduler
python main.py schedule start
```

---

## 📖 **Command Categories Summary**

| Category | Commands | Purpose |
|----------|----------|---------|
| **System** | `setup`, `test` | Database initialization, health checks |
| **ETL** | `etl` | Data processing pipeline |
| **Scheduling** | `schedule daily`, `schedule list`, `schedule start` | Job automation |
| **Performance** | `performance analyze`, `performance cache-stats` | Query optimization |
| **Quality** | `quality check`, `quality report` | Data validation |
| **Metadata** | `metadata tables`, `metadata lineage`, `metadata export` | Data catalog |
| **Alerts** | `alerts run`, `alerts test` | Anomaly detection |
| **Versions** | `versions list`, `versions show` | Version control |

---

## 🚨 **Error Handling**

### **Common Error Messages**

#### **Database Connection Errors**
```
❌ Database connection failed. Please check your configuration.
```
**Solution**: Verify database is running and connection parameters are correct

#### **ETL Processing Errors**
```
❌ ETL failed: File not found: data/missing_file.csv
```
**Solution**: Check file path and permissions

#### **Quality Threshold Errors**
```
❌ Quality check failed: Completeness 78.2% below threshold 95%
```
**Solution**: Review data source quality or adjust thresholds

### **Debug Mode**
```bash
# Enable verbose logging for troubleshooting
python main.py --log-level DEBUG etl --source data.csv
```

---