# 🎛️ **Command Reference Guide**

Complete reference for all CLI commands in the Retail Data Platform.

## Setup & Testing Commands

### Database Setup
```bash
# Initialize complete database schema
python main.py setup
# Creates: schema, tables, indexes, views, versioning system

# Fresh start (⚠️ DESTROYS ALL DATA)
python main.py setup --drop-existing
# Drops all tables and recreates from scratch

# Setup with custom configuration
python main.py setup --config production.yaml
# Uses specific configuration file
```

### System Testing
```bash
# Basic system health check
python main.py test
# ✅ Database connection successful
# ✅ Schema validation passed
# ✅ All systems operational

# Detailed diagnostics
python main.py test --verbose
# Shows detailed connection info, table counts, index status

# Test specific component
python main.py test --component database
python main.py test --component etl
python main.py test --component quality
```

## ETL Commands

### Basic ETL Operations
```bash
# Process CSV file
python main.py etl --source data/online_retail.csv
# Standard ETL: Extract → Clean → Transform → Load

# Named ETL job
python main.py etl --source data/online_retail.csv --job-name "daily_import"
# Creates job with custom name for tracking

# Custom batch size
python main.py etl --source data/online_retail.csv --batch-size 2000
# Process in larger/smaller batches (default: 1000)

# ETL with validation
python main.py etl --source data/online_retail.csv --validate
# Includes comprehensive quality checks
```

### Advanced ETL Options
```bash
# Dry run (validation only)
python main.py etl --source data/online_retail.csv --dry-run
# Validates data without loading to database

# ETL with custom quality threshold
python main.py etl --source data/online_retail.csv --quality-threshold 0.98
# Fails if quality below 98%

# Parallel processing (if enabled)
python main.py etl --source data/online_retail.csv --parallel
# Uses multiple threads for processing

# Resume from checkpoint
python main.py etl --source data/online_retail.csv --resume --checkpoint-id cp_12345
# Resume interrupted job from last checkpoint
```

## Data Quality Commands

### Quality Checks
```bash
# Check all tables
python main.py quality check
# Runs quality rules on all monitored tables

# Check specific table
python main.py quality check --table fact_sales
# Detailed quality analysis for single table

# Check with date filter
python main.py quality check --table fact_sales --since "2024-10-01"
# Only check data from specified date

# Verbose quality output
python main.py quality check --table fact_sales --verbose
# Shows detailed metrics and failing records
```

### Quality Dashboard & Reporting
```bash
# Interactive quality dashboard
python main.py quality dashboard
# Opens web-based quality monitoring dashboard

# Generate quality report
python main.py quality report --days 7
# HTML report for last 7 days

# Export quality metrics
python main.py quality export --format json --output metrics.json
# Export to JSON, CSV, or Excel

# Quality summary
python main.py quality summary
# Quick overview of current quality status
```

### Quality Rules Management
```bash
# Show current quality rules
python main.py quality rules --show
# Display all configured quality thresholds

# Set quality thresholds
python main.py quality rules --set completeness=0.95 validity=0.90
# Update quality thresholds

# Add custom quality rule
python main.py quality rules --add --table fact_sales --rule "quantity > 0" --threshold 0.95
# Add business-specific validation rule

# Remove quality rule
python main.py quality rules --remove --table fact_sales --rule-name "custom_quantity_check"
# Remove specified quality rule
```

### Quality Alerts
```bash
# Configure quality alerts
python main.py quality alerts --set completeness=0.95 validity=0.90
# Set alert thresholds

# Test alert system
python main.py quality alerts --test
# Send test alerts to verify configuration

# View alert history
python main.py quality alerts --history --days 7
# Show recent quality alerts

# Enable/disable alerts
python main.py quality alerts --enable
python main.py quality alerts --disable
```

## Query Commands

### Basic Querying
```bash
# Query table with limit
python main.py query --table fact_sales --limit 10
# Show first 10 records from fact_sales

# Query with WHERE clause
python main.py query --table dim_customer --where "country='United Kingdom'"
# Filter records by condition

# Custom SQL query
python main.py query --sql "SELECT COUNT(*) FROM retail_dw.fact_sales"
# Execute custom SQL statement

# Export query results
python main.py query --table fact_sales --limit 1000 --output sales_data.csv
# Export results to CSV file
```

### Advanced Querying
```bash
# Query with joins
python main.py query --sql "
SELECT c.country, COUNT(*) as customers
FROM retail_dw.dim_customer c
GROUP BY c.country
ORDER BY customers DESC
LIMIT 10"

# Query specific columns
python main.py query --table fact_sales --columns "invoice_no,quantity,unit_price,line_total" --limit 5

# Query with date range
python main.py query --table fact_sales --where "transaction_datetime >= '2010-12-01'" --limit 10
```

## Monitoring Commands

### Job Monitoring
```bash
# View recent ETL jobs
python main.py monitor --jobs 5
# Show last 5 ETL job executions

# Monitor specific job
python main.py monitor --job-id etl_20241008_123456
# Detailed info for specific job

# Monitor with verbose output
python main.py monitor --jobs 10 --verbose
# Detailed job information including metrics

# Real-time job monitoring
python main.py monitor --follow
# Live monitoring of running jobs
```

### System Monitoring
```bash
# Database connection monitoring
python main.py monitor --connections
# Show active database connections

# Performance monitoring
python main.py monitor --performance
# CPU, memory, and database performance

# Error monitoring
python main.py monitor --errors --days 7
# Show errors from last 7 days
```

## Performance Commands

### Performance Analysis
```bash
# Full performance audit
python main.py performance audit
# Comprehensive performance analysis

# Query performance analysis
python main.py performance queries
# Analyze slow queries and optimization opportunities

# Index recommendations
python main.py performance indexes --recommend
# Suggest new indexes for better performance

# Cache statistics
python main.py performance cache --stats
# Show cache hit rates and efficiency
```

### Performance Optimization
```bash
# Rebuild indexes
python main.py performance indexes --rebuild
# Rebuild all database indexes

# Clear query cache
python main.py performance cache --clear
# Clear application query cache

# Database maintenance
python main.py performance maintenance
# Run database maintenance tasks (VACUUM, ANALYZE)
```

## Scheduling Commands

### Schedule Management
```bash
# List all scheduled jobs
python main.py schedule list
# Show all configured schedules

# Add new scheduled job
python main.py schedule add --name daily_etl --cron "0 2 * * *" --command "etl --source data/online_retail.csv"
# Schedule daily ETL at 2 AM

# Remove scheduled job
python main.py schedule remove --name daily_etl
# Remove specified schedule

# Enable/disable schedule
python main.py schedule enable --name daily_etl
python main.py schedule disable --name daily_etl
```

### Schedule Operations
```bash
# Start scheduler daemon
python main.py schedule start
# Start background scheduler

# Stop scheduler daemon
python main.py schedule stop
# Stop background scheduler

# Run scheduled job immediately
python main.py schedule run --name daily_etl
# Execute scheduled job on-demand

# View schedule history
python main.py schedule history --name daily_etl --days 7
# Show execution history for scheduled job
```

## Version Management Commands

### Version Information
```bash
# List all data versions
python main.py versions list
# Show all ETL job versions and status

# Show current active version
python main.py versions current
# Display current data version info

# Show version details
python main.py versions show --version-id 123
# Detailed info for specific version
```

### Version Operations
```bash
# Create new version
python main.py versions create --name "Q4_2024_data" --description "Q4 sales data import"
# Create new data version

# Compare versions
python main.py versions compare --version1 v1.0 --version2 v1.1
# Compare data between versions

# Rollback to version
python main.py versions rollback --version-id 122
# Rollback to previous version (⚠️ destructive)
```

### Partitioning Commands
```bash
# Check partitioning status
python main.py versions partitions
# Show table partitioning information

# Enable partitioning (requires --drop-existing)
python main.py setup --drop-existing  # Then partitioning is enabled
```

## Metadata Commands

### Data Catalog
```bash
# Show data dictionary
python main.py metadata dictionary
# Complete table and column documentation

# Export metadata
python main.py metadata export --format json --output metadata.json
# Export complete metadata catalog

# Show data lineage
python main.py metadata lineage --table fact_sales
# Data lineage for specific table

# Search metadata
python main.py metadata search --term "customer"
# Search tables/columns containing term
```

### Metadata Management
```bash
# Update table documentation
python main.py metadata update --table fact_sales --description "Central sales transactions"

# Add column documentation
python main.py metadata update --table fact_sales --column quantity --description "Number of items sold"

# Generate documentation
python main.py metadata generate-docs --output docs/data_dictionary.md
```

## Utility Commands

### Data Import/Export
```bash
# Export table data
python main.py export --table fact_sales --format csv --output sales_export.csv
# Export table to CSV

# Import data from backup
python main.py import --table fact_sales --source backup_sales.csv
# Import data from CSV file

# Backup database
python main.py backup --output backup_20241008.sql
# Create full database backup
```

### Configuration Commands
```bash
# Show current configuration
python main.py config show
# Display current configuration values

# Validate configuration
python main.py config validate
# Check configuration file syntax

# Set configuration value
python main.py config set --key etl.batch_size --value 2000
# Update configuration value
```

## Command Categories Summary

| Category | Commands | Purpose |
|----------|----------|---------|
| **Setup** | `setup`, `test` | Database initialization and testing |
| **ETL** | `etl` | Data extraction, transformation, loading |
| **Quality** | `quality check`, `quality dashboard`, `quality rules` | Data quality monitoring |
| **Query** | `query` | Data querying and exploration |
| **Monitor** | `monitor` | Job and system monitoring |
| **Performance** | `performance audit`, `performance indexes` | Performance optimization |
| **Schedule** | `schedule list`, `schedule add`, `schedule start` | Job scheduling |
| **Versions** | `versions list`, `versions current`, `versions partitions` | Version management |
| **Metadata** | `metadata dictionary`, `metadata lineage` | Data catalog |

## Getting Help

### Command Help
```bash
# General help
python main.py --help
# Show all available commands

# Command-specific help
python main.py etl --help
python main.py quality --help
python main.py query --help

# Subcommand help
python main.py quality check --help
python main.py schedule add --help
```

### Common Usage Patterns
```bash
# Daily workflow
python main.py etl --source data/daily_sales.csv
python main.py quality check
python main.py monitor --jobs 1

# Weekly analysis
python main.py quality report --days 7
python main.py performance audit
python main.py versions list

# Troubleshooting
python main.py test --verbose
python main.py monitor --errors --days 1
python main.py performance queries
```

---

**All commands support `--help` flag for detailed usage information. Use `python main.py --help` to see all available commands.**