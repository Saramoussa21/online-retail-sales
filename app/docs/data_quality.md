# 📊 **Data Quality Monitoring Guide**

## Overview

The Retail Data Platform includes comprehensive data quality monitoring to ensure data integrity, accuracy, and reliability throughout the ETL pipeline. The monitoring system provides real-time quality metrics, trend analysis, and automated alerting.

## Quality Framework

### Quality Dimensions
1. **Completeness**: Percentage of non-null, non-empty values
2. **Validity**: Adherence to data type, format, and range constraints
3. **Uniqueness**: Absence of inappropriate duplicate records
4. **Accuracy**: Conformance to business rules and logical constraints
5. **Consistency**: Alignment across related tables and fields
6. **Timeliness**: Data freshness and processing latency

### Quality Metrics Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Sources   │────│  Quality Rules  │────│  Quality Metrics│
│                 │    │                 │    │                 │
│ • CSV Files     │    │ • Completeness  │    │ • Scores        │
│ • Database      │    │ • Validity      │    │ • Trends        │
│ • APIs          │    │ • Business Rules│    │ • Alerts        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                        │
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Monitoring    │    │   Reporting     │
                       │                 │    │                 │
                       │ • Real-time     │    │ • Dashboards    │
                       │ • Thresholds    │    │ • Alerts        │
                       │ • History       │    │ • Trends        │
                       └─────────────────┘    └─────────────────┘
```

## Quality Rules Configuration

### Default Quality Rules
```python
# retail_data_platform/monitoring/quality.py

DEFAULT_QUALITY_RULES = {
    'fact_sales': {
        'completeness': {
            'invoice_no': 1.0,        # 100% required
            'stock_code': 1.0,        # 100% required
            'quantity': 1.0,          # 100% required
            'unit_price': 1.0,        # 100% required
            'customer_key': 0.95,     # 95% required (guests allowed)
            'transaction_datetime': 1.0  # 100% required
        },
        'validity': {
            'quantity': {'min': -1000, 'max': 10000},
            'unit_price': {'min': 0, 'max': 1000},
            'line_total': {'min': -10000, 'max': 50000}
        },
        'uniqueness': {
            'composite_key': ['invoice_no', 'stock_code'],
            'threshold': 0.98  # Allow 2% duplicates
        },
        'accuracy': {
            'line_total_calculation': {
                'rule': 'quantity * unit_price = line_total',
                'tolerance': 0.01
            }
        }
    },
    'dim_customer': {
        'completeness': {
            'customer_id': 1.0,
            'country': 0.98
        },
        'validity': {
            'customer_id': {'pattern': r'^\d+$'}
        }
    },
    'dim_product': {
        'completeness': {
            'stock_code': 1.0,
            'description': 0.95
        },
        'validity': {
            'stock_code': {'pattern': r'^[A-Z0-9]+$'}
        }
    }
}
```

### Custom Quality Rules
```python
# Add custom rule
from retail_data_platform.monitoring.quality import QualityMonitor

monitor = QualityMonitor()

# Add business-specific rule
monitor.add_rule(
    table='fact_sales',
    rule_type='accuracy',
    rule_name='uk_sales_hours',
    condition="country = 'United Kingdom'",
    expression="EXTRACT(HOUR FROM transaction_datetime) BETWEEN 8 AND 22",
    threshold=0.90,
    description="UK sales should occur during business hours"
)
```

## Quality Monitoring Commands

### Basic Quality Checks
```bash
# Check all tables
python main.py quality check

# Check specific table
python main.py quality check --table fact_sales

# Check with detailed output
python main.py quality check --table fact_sales --verbose

# Check recent data only
python main.py quality check --table fact_sales --since "2024-10-01"
```

### Quality Dashboard
```bash
# Launch interactive dashboard
python main.py quality dashboard

# Generate static report
python main.py quality report --output html

# Get summary metrics
python main.py quality summary
```

### Quality Trends
```bash
# View quality trends over time
python main.py quality trends --days 30

# Compare quality across versions
python main.py quality compare --version1 v1.0 --version2 v1.1

# Export quality metrics
python main.py quality export --format json --output quality_metrics.json
```

## Quality Metrics Tables

### data_quality_metrics Table Structure
```sql
CREATE TABLE retail_dw.data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    metric_type VARCHAR(50) NOT NULL,  -- completeness, validity, uniqueness, accuracy
    metric_value DECIMAL(5,4) NOT NULL, -- 0.0000 to 1.0000
    threshold_value DECIMAL(5,4),
    passed BOOLEAN NOT NULL,
    record_count BIGINT,
    failed_count BIGINT,
    measurement_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    job_id VARCHAR(100),
    version_id INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
```

### Sample Quality Metrics Data
```sql
-- View recent quality metrics
SELECT 
    table_name,
    metric_type,
    AVG(metric_value) as avg_score,
    MIN(metric_value) as min_score,
    COUNT(*) as measurements
FROM retail_dw.data_quality_metrics 
WHERE measurement_time >= CURRENT_DATE - 7
GROUP BY table_name, metric_type
ORDER BY table_name, metric_type;

-- Example output:
-- table_name  | metric_type  | avg_score | min_score | measurements
-- fact_sales  | completeness | 0.9850    | 0.9820    | 7
-- fact_sales  | validity     | 0.9780    | 0.9750    | 7
-- fact_sales  | uniqueness   | 0.9920    | 0.9900    | 7
-- fact_sales  | accuracy     | 0.9650    | 0.9600    | 7
```

## Quality Alerting

### Alert Configuration
```python
# retail_data_platform/monitoring/alerts.py

QUALITY_ALERT_CONFIG = {
    'thresholds': {
        'critical': 0.90,  # Below 90% triggers critical alert
        'warning': 0.95,   # Below 95% triggers warning
        'info': 0.98       # Below 98% triggers info alert
    },
    'notification_channels': {
        'email': 'data-team@company.com',
        'slack': '#data-quality',
        'console': True
    },
    'alert_frequency': {
        'critical': 'immediate',
        'warning': 'hourly',
        'info': 'daily'
    }
}
```

### Alert Types
1. **Quality Threshold Breach**: Metric falls below configured threshold
2. **Data Drift**: Significant change in data patterns
3. **Processing Failures**: ETL job quality failures
4. **Trend Alerts**: Declining quality trends over time

### Setting Up Alerts
```bash
# Configure alert thresholds
python main.py quality alerts --set completeness=0.95 validity=0.90

# Test alert system
python main.py quality alerts --test

# View alert history
python main.py quality alerts --history --days 7

# Enable/disable alerts
python main.py quality alerts --enable
python main.py quality alerts --disable
```

## Quality Reports

### Automated Quality Reports
```python
# Generate daily quality report
from retail_data_platform.monitoring.quality import QualityReporter

reporter = QualityReporter()

# Generate comprehensive report
report = reporter.generate_daily_report(
    date='2024-10-08',
    include_trends=True,
    include_recommendations=True
)

# Export to different formats
reporter.export_report(report, format='html', file='quality_report.html')
reporter.export_report(report, format='pdf', file='quality_report.pdf')
reporter.export_report(report, format='json', file='quality_report.json')
```

### Report Contents
1. **Executive Summary**: Overall quality scores and status
2. **Detailed Metrics**: Table-by-table quality breakdown
3. **Trend Analysis**: Quality changes over time
4. **Issue Detection**: Identified quality problems
5. **Recommendations**: Suggested improvements
6. **Data Lineage**: Processing history and impact

### Sample Quality Report Output
```
📊 DAILY QUALITY REPORT - 2024-10-08
==========================================

🎯 EXECUTIVE SUMMARY
Overall Quality Score: 96.8% ✅
Tables Monitored: 5
Quality Checks Passed: 47/50
Critical Issues: 0
Warnings: 3

📈 QUALITY SCORES BY TABLE
fact_sales: 96.5% ✅
- Completeness: 98.5% ✅
- Validity: 97.8% ✅  
- Uniqueness: 99.2% ✅
- Accuracy: 96.5% ⚠️ (Below 97% threshold)

dim_customer: 97.2% ✅
- Completeness: 97.8% ✅
- Validity: 98.1% ✅
- Uniqueness: 99.9% ✅

dim_product: 96.8% ✅
- Completeness: 95.2% ⚠️ (Below 96% threshold)
- Validity: 98.7% ✅
- Uniqueness: 99.5% ✅

🔍 QUALITY ISSUES DETECTED
1. fact_sales.accuracy: Line total calculations have 3.5% error rate
   Recommendation: Review price calculation logic

2. dim_product.completeness: 4.8% missing descriptions
   Recommendation: Implement fallback description rules

📊 QUALITY TRENDS (7 days)
fact_sales completeness: 98.2% → 98.5% (↗️ improving)
fact_sales accuracy: 97.1% → 96.5% (↘️ declining)
dim_product completeness: 94.8% → 95.2% (↗️ improving)

💡 RECOMMENDATIONS
1. Investigate declining accuracy in fact_sales
2. Implement automated product description lookup
3. Add validation for UK business hours rule
4. Consider stricter uniqueness thresholds
```

## Quality Monitoring API

### Programmatic Quality Checks
```python
from retail_data_platform.monitoring.quality import QualityMonitor

# Initialize monitor
monitor = QualityMonitor()

# Check table quality
result = monitor.check_table_quality('fact_sales')

print(f"Overall score: {result.overall_score}")
print(f"Completeness: {result.completeness_score}")
print(f"Validity: {result.validity_score}")
print(f"Issues found: {len(result.issues)}")

# Check specific column
column_result = monitor.check_column_quality('fact_sales', 'quantity')

# Get quality history
history = monitor.get_quality_history('fact_sales', days=30)
```

### Quality Monitoring Integration
```python
# In ETL pipeline
from retail_data_platform.etl.pipeline import ETLPipeline
from retail_data_platform.monitoring.quality import QualityMonitor

pipeline = ETLPipeline()
monitor = QualityMonitor()

# Run ETL with quality monitoring
result = pipeline.run_etl_job(
    source_file='data/online_retail.csv',
    job_name='daily_etl'
)

# Check quality after ETL
quality_result = monitor.check_table_quality('fact_sales')

if quality_result.overall_score < 0.95:
    # Trigger alert or rollback
    monitor.send_alert(f"Quality below threshold: {quality_result.overall_score}")
```

## Best Practices

### Quality Rule Design
1. **Start Conservative**: Begin with achievable thresholds
2. **Business Alignment**: Rules should reflect business requirements
3. **Iterative Improvement**: Adjust thresholds based on data patterns
4. **Documentation**: Clearly document business rationale for rules

### Monitoring Strategy
1. **Real-time Checks**: Monitor during ETL processing
2. **Scheduled Reviews**: Daily/weekly quality assessments
3. **Trend Analysis**: Identify quality degradation patterns
4. **Root Cause Analysis**: Investigate quality issues systematically

### Alert Management
1. **Threshold Tuning**: Avoid alert fatigue with appropriate thresholds
2. **Escalation Paths**: Define clear escalation procedures
3. **Response Procedures**: Document quality issue response workflows
4. **Continuous Improvement**: Regularly review and update alert rules

## Troubleshooting Quality Issues

### Common Quality Problems
1. **Low Completeness**: Missing required data
   - Check source data quality
   - Review ETL data handling logic
   - Verify business rules for optional fields

2. **Poor Validity**: Data format/type issues
   - Validate source data formats
   - Review transformation logic
   - Check for encoding issues

3. **Uniqueness Violations**: Unexpected duplicates
   - Review duplicate detection logic
   - Check for timing issues in source data
   - Verify business keys

4. **Accuracy Problems**: Business rule violations
   - Review calculation logic
   - Check for rounding errors
   - Validate business rule implementation

### Diagnostic Commands
```bash
# Deep dive into quality issues
python main.py quality diagnose --table fact_sales --metric accuracy

# Compare quality across time periods
python main.py quality compare --period1 "last week" --period2 "this week"

# Export failing records for analysis
python main.py quality export-failures --table fact_sales --metric completeness

# Analyze quality patterns
python main.py quality analyze --table fact_sales --column quantity
```

---

**The quality monitoring system ensures 96.8% average quality score across all tables with automated alerting and comprehensive reporting.**