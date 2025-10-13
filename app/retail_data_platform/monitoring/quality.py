"""
Data Quality Monitoring System

Comprehensive data quality monitoring with metrics calculation,
anomaly detection, threshold monitoring, and alerting capabilities.
"""

import statistics
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from ..database.connection import get_db_session
from ..database.models import DataQualityMetrics
from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config


class MetricType(Enum):
    """Types of data quality metrics"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"


@dataclass
class QualityThreshold:
    """Data quality threshold definition"""
    metric_name: str
    metric_type: MetricType
    threshold_value: float
    operator: str = ">=",  # >=, <=, ==, !=
    severity: str = "ERROR"  # ERROR, WARNING, INFO
    enabled: bool = True


@dataclass
class QualityResult:
    """Result of data quality check"""
    metric_name: str
    metric_type: MetricType
    metric_value: float
    threshold_value: Optional[float]
    is_threshold_met: Optional[bool]
    table_name: str
    column_name: Optional[str]
    measured_at: datetime
    details: Dict[str, Any] = field(default_factory=dict)


class DataQualityRule(ABC):
    """Abstract base class for data quality rules"""
    
    def __init__(self, name: str, metric_type: MetricType, description: str):
        self.name = name
        self.metric_type = metric_type
        self.description = description
        self.logger = ETLLogger(f"quality.{name}")
    
    @abstractmethod
    def calculate_metric(self, data: List[Dict[str, Any]], 
                        table_name: str, column_name: str = None) -> QualityResult:
        """Calculate the quality metric"""
        pass


class CompletenessRule(DataQualityRule):
    """Rule to check data completeness (non-null values)"""
    
    def __init__(self, name: str = "completeness"):
        super().__init__(name, MetricType.COMPLETENESS, 
                        "Percentage of non-null values")
    
    def calculate_metric(self, data: List[Dict[str, Any]], 
                        table_name: str, column_name: str = None) -> QualityResult:
        """Calculate completeness percentage"""
        if not data or not column_name:
            return QualityResult(
                metric_name=self.name,
                metric_type=self.metric_type,
                metric_value=0.0,
                threshold_value=None,
                is_threshold_met=None,
                table_name=table_name,
                column_name=column_name,
                measured_at=datetime.utcnow()
            )
        
        total_records = len(data)
        non_null_records = sum(1 for record in data 
                              if record.get(column_name) is not None 
                              and str(record.get(column_name)).strip() != "")
        
        completeness_percentage = (non_null_records / total_records) * 100
        
        return QualityResult(
            metric_name=self.name,
            metric_type=self.metric_type,
            metric_value=completeness_percentage,
            threshold_value=None,
            is_threshold_met=None,
            table_name=table_name,
            column_name=column_name,
            measured_at=datetime.utcnow(),
            details={
                'total_records': total_records,
                'non_null_records': non_null_records,
                'null_records': total_records - non_null_records
            }
        )


class UniquenessRule(DataQualityRule):
    """Rule to check data uniqueness"""
    
    def __init__(self, name: str = "uniqueness"):
        super().__init__(name, MetricType.UNIQUENESS, 
                        "Percentage of unique values")
    
    def calculate_metric(self, data: List[Dict[str, Any]], 
                        table_name: str, column_name: str = None) -> QualityResult:
        """Calculate uniqueness percentage"""
        if not data or not column_name:
            return QualityResult(
                metric_name=self.name,
                metric_type=self.metric_type,
                metric_value=0.0,
                threshold_value=None,
                is_threshold_met=None,
                table_name=table_name,
                column_name=column_name,
                measured_at=datetime.utcnow()
            )
        
        values = [record.get(column_name) for record in data 
                 if record.get(column_name) is not None]
        
        total_values = len(values)
        unique_values = len(set(str(v) for v in values))
        
        uniqueness_percentage = (unique_values / total_values) * 100 if total_values > 0 else 0
        
        return QualityResult(
            metric_name=self.name,
            metric_type=self.metric_type,
            metric_value=uniqueness_percentage,
            threshold_value=None,
            is_threshold_met=None,
            table_name=table_name,
            column_name=column_name,
            measured_at=datetime.utcnow(),
            details={
                'total_values': total_values,
                'unique_values': unique_values,
                'duplicate_values': total_values - unique_values
            }
        )


class ValidityRule(DataQualityRule):
    """Rule to check data validity against constraints"""
    
    def __init__(self, name: str = "validity", 
                 validation_function: callable = None):
        super().__init__(name, MetricType.VALIDITY, 
                        "Percentage of values meeting validation criteria")
        self.validation_function = validation_function or self._default_validation
    
    def _default_validation(self, value: Any) -> bool:
        """Default validation - just check if value exists and is not empty"""
        return value is not None and str(value).strip() != ""
    
    def calculate_metric(self, data: List[Dict[str, Any]], 
                        table_name: str, column_name: str = None) -> QualityResult:
        """Calculate validity percentage"""
        if not data or not column_name:
            return QualityResult(
                metric_name=self.name,
                metric_type=self.metric_type,
                metric_value=0.0,
                threshold_value=None,
                is_threshold_met=None,
                table_name=table_name,
                column_name=column_name,
                measured_at=datetime.utcnow()
            )
        
        total_records = len(data)
        valid_records = sum(1 for record in data 
                           if self.validation_function(record.get(column_name)))
        
        validity_percentage = (valid_records / total_records) * 100
        
        return QualityResult(
            metric_name=self.name,
            metric_type=self.metric_type,
            metric_value=validity_percentage,
            threshold_value=None,
            is_threshold_met=None,
            table_name=table_name,
            column_name=column_name,
            measured_at=datetime.utcnow(),
            details={
                'total_records': total_records,
                'valid_records': valid_records,
                'invalid_records': total_records - valid_records
            }
        )


class NumericRangeRule(DataQualityRule):
    """Rule to check if numeric values are within expected range"""
    
    def __init__(self, name: str = "numeric_range", 
                 min_value: float = None, max_value: float = None):
        super().__init__(name, MetricType.VALIDITY, 
                        "Percentage of numeric values within expected range")
        self.min_value = min_value
        self.max_value = max_value
    
    def calculate_metric(self, data: List[Dict[str, Any]], 
                        table_name: str, column_name: str = None) -> QualityResult:
        """Calculate percentage of values within range"""
        if not data or not column_name:
            return QualityResult(
                metric_name=self.name,
                metric_type=self.metric_type,
                metric_value=0.0,
                threshold_value=None,
                is_threshold_met=None,
                table_name=table_name,
                column_name=column_name,
                measured_at=datetime.utcnow()
            )
        
        numeric_values = []
        for record in data:
            value = record.get(column_name)
            if value is not None:
                try:
                    numeric_values.append(float(value))
                except (ValueError, TypeError):
                    pass
        
        total_numeric = len(numeric_values)
        if total_numeric == 0:
            return QualityResult(
                metric_name=self.name,
                metric_type=self.metric_type,
                metric_value=0.0,
                threshold_value=None,
                is_threshold_met=None,
                table_name=table_name,
                column_name=column_name,
                measured_at=datetime.utcnow(),
                details={'error': 'No numeric values found'}
            )
        
        in_range_count = 0
        for value in numeric_values:
            in_range = True
            if self.min_value is not None and value < self.min_value:
                in_range = False
            if self.max_value is not None and value > self.max_value:
                in_range = False
            if in_range:
                in_range_count += 1
        
        range_percentage = (in_range_count / total_numeric) * 100
        
        return QualityResult(
            metric_name=self.name,
            metric_type=self.metric_type,
            metric_value=range_percentage,
            threshold_value=None,
            is_threshold_met=None,
            table_name=table_name,
            column_name=column_name,
            measured_at=datetime.utcnow(),
            details={
                'total_numeric_values': total_numeric,
                'values_in_range': in_range_count,
                'values_out_of_range': total_numeric - in_range_count,
                'min_value': self.min_value,
                'max_value': self.max_value
            }
        )


class DataQualityMonitor:
    """
    Main data quality monitoring system
    """
    
    def __init__(self, batch_id: str = None):
        self.config = get_config()
        self.logger = ETLLogger("quality.monitor")
        self.batch_id = batch_id or "manual"
        
        # Initialize quality rules
        self.quality_rules: Dict[str, List[DataQualityRule]] = {
            'fact_sales': [
                CompletenessRule("invoice_completeness"),
                CompletenessRule("product_completeness"),
                CompletenessRule("customer_completeness"),
                UniquenessRule("transaction_uniqueness"),
                NumericRangeRule("quantity_range", min_value=-1000, max_value=10000),
                NumericRangeRule("price_range", min_value=0, max_value=1000),
                ValidityRule("date_validity", self._validate_date)
            ],
            'dim_customer': [
                CompletenessRule("customer_id_completeness"),
                CompletenessRule("country_completeness"),
                UniquenessRule("customer_id_uniqueness")
            ],
            'dim_product': [
                CompletenessRule("stock_code_completeness"),
                UniquenessRule("stock_code_uniqueness"),
                ValidityRule("description_validity", self._validate_description)
            ]
        }
        
        # Quality thresholds
        self.quality_thresholds = self._initialize_thresholds()
        
        # Results storage
        self.quality_results: List[QualityResult] = []
    # ADD these methods to your existing DataQualityMonitor class

    def track_quality_trends(self, days: int = 30) -> Dict[str, Any]:
        """Track quality trends over time"""
        try:
            with get_db_session() as session:
                from sqlalchemy import func, text
                from datetime import datetime, timedelta
                
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=days)
                
                # Get daily quality averages
                query = text("""
                    SELECT 
                        DATE(measured_at) as date,
                        AVG(metric_value::FLOAT) as avg_score,
                        COUNT(*) as checks_count,
                        SUM(CASE WHEN metric_value::FLOAT < 90 THEN 1 ELSE 0 END) as poor_quality_count
                    FROM retail_dw.data_quality_metrics 
                    WHERE measured_at >= :start_date 
                    GROUP BY DATE(measured_at)
                    ORDER BY date
                """)
                
                results = session.execute(query, {'start_date': start_date}).fetchall()
                
                trend_data = []
                for row in results:
                    trend_data.append({
                        'date': row.date.isoformat(),
                        'avg_score': float(row.avg_score or 0),
                        'checks_count': row.checks_count,
                        'poor_quality_count': row.poor_quality_count
                    })
                
                return {
                    'period_days': days,
                    'data_points': len(trend_data),
                    'trends': trend_data,
                    'summary': self._calculate_trend_summary(trend_data)
                }
                
        except Exception as e:
            self.logger.error(f"Trend tracking failed: {e}")
            return {'error': str(e)}

    def detect_quality_anomalies(self, threshold: float = 10.0) -> List[Dict[str, Any]]:
        """Detect quality score anomalies"""
        try:
            with get_db_session() as session:
                from sqlalchemy import text
                
                # 🔧 FIX: Use ANY metric, not just 'overall_completeness'
                query = text("""
                    WITH quality_with_lag AS (
                        SELECT 
                            metric_id,
                            table_name,
                            metric_name,
                            metric_value::FLOAT as overall_score,
                            measured_at,
                            LAG(metric_value::FLOAT, 1) OVER (
                                PARTITION BY table_name, metric_name 
                                ORDER BY measured_at
                            ) as prev_score
                        FROM retail_dw.data_quality_metrics 
                        WHERE measured_at >= NOW() - INTERVAL '7 days'
                    )
                    SELECT 
                        metric_id,
                        table_name,
                        metric_name,
                        overall_score,
                        prev_score,
                        (prev_score - overall_score) as score_drop,
                        measured_at
                    FROM quality_with_lag 
                    WHERE prev_score IS NOT NULL 
                    AND (prev_score - overall_score) > :threshold
                    ORDER BY score_drop DESC
                """)
                
                results = session.execute(query, {'threshold': threshold}).fetchall()
                
                anomalies = []
                for row in results:
                    anomalies.append({
                        'metric_id': row.metric_id,
                        'table_name': row.table_name,
                        'metric_name': row.metric_name,  # Added metric name
                        'current_score': float(row.overall_score),
                        'previous_score': float(row.prev_score or 0),
                        'score_drop': float(row.score_drop),
                        'measured_at': row.measured_at.isoformat(),
                        'severity': 'HIGH' if row.score_drop > 20 else 'MEDIUM'
                    })
                
                return anomalies
                
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}")
            return []
    
    def _calculate_trend_summary(self, trend_data: List[Dict]) -> Dict[str, Any]:
        """Calculate trend summary statistics"""
        if not trend_data:
            return {}
        
        scores = [d['avg_score'] for d in trend_data if d['avg_score'] > 0]
        if not scores:
            return {}
        
        return {
            'avg_quality_score': sum(scores) / len(scores),
            'min_quality_score': min(scores),
            'max_quality_score': max(scores),
            'quality_trend': 'IMPROVING' if len(scores) > 1 and scores[-1] > scores[0] else 'DECLINING' if len(scores) > 1 and scores[-1] < scores[0] else 'STABLE',
            'total_checks': sum(d['checks_count'] for d in trend_data),
            'poor_quality_days': sum(1 for d in trend_data if d['poor_quality_count'] > 0)
        }


    def _initialize_thresholds(self) -> List[QualityThreshold]:
        """Initialize quality thresholds"""
        return [
            QualityThreshold("invoice_completeness", MetricType.COMPLETENESS, 95.0),
            QualityThreshold("product_completeness", MetricType.COMPLETENESS, 95.0),
            QualityThreshold("customer_completeness", MetricType.COMPLETENESS, 80.0),
            QualityThreshold("transaction_uniqueness", MetricType.UNIQUENESS, 99.0),
            QualityThreshold("quantity_range", MetricType.VALIDITY, 95.0),
            QualityThreshold("price_range", MetricType.VALIDITY, 98.0),
            QualityThreshold("date_validity", MetricType.VALIDITY, 100.0),
            QualityThreshold("customer_id_completeness", MetricType.COMPLETENESS, 90.0),
            QualityThreshold("country_completeness", MetricType.COMPLETENESS, 95.0),
            QualityThreshold("customer_id_uniqueness", MetricType.UNIQUENESS, 100.0),
            QualityThreshold("stock_code_completeness", MetricType.COMPLETENESS, 100.0),
            QualityThreshold("stock_code_uniqueness", MetricType.UNIQUENESS, 100.0),
            QualityThreshold("description_validity", MetricType.VALIDITY, 90.0)
        ]
    
    def _validate_date(self, value: Any) -> bool:
        """Validate date values"""
        if value is None:
            return False
        
        try:
            if isinstance(value, datetime):
                return True
            elif isinstance(value, str):
                datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                return True
        except (ValueError, TypeError):
            pass
        
        return False
    
    def _validate_description(self, value: Any) -> bool:
        """Validate product descriptions"""
        if value is None or str(value).strip() == "":
            return False
        
        return len(str(value).strip()) >= 3
    
    def check_data_quality(self, data: List[Dict[str, Any]], 
                          table_name: str) -> List[QualityResult]:
        """Run data quality checks on a dataset"""
        results = []
        
        if table_name not in self.quality_rules:
            self.logger.warning(f"No quality rules defined for table: {table_name}")
            return results
        
        self.logger.info(f"Running data quality checks for {table_name}", 
                        record_count=len(data))
        
        rules = self.quality_rules[table_name]
        
        for rule in rules:
            try:
                column_name = self._get_column_for_rule(rule.name, table_name)
                
                result = rule.calculate_metric(data, table_name, column_name)
                
                # Apply thresholds
                threshold = self._get_threshold(rule.name)
                if threshold:
                    result.threshold_value = threshold.threshold_value
                    result.is_threshold_met = self._evaluate_threshold(
                        result.metric_value, threshold
                    )
                
                results.append(result)
                self.quality_results.append(result)
                
                # Log quality metrics
                self.logger.log_data_quality(table_name, {
                    'metric_name': result.metric_name,
                    'metric_value': result.metric_value,
                    'threshold_met': result.is_threshold_met,
                    'details': result.details
                })
                
            except Exception as e:
                self.logger.error(f"Quality check failed for rule {rule.name}: {e}")
        
        return results
    
    def _get_column_for_rule(self, rule_name: str, table_name: str) -> Optional[str]:
        """Map rule names to column names"""
        rule_column_mapping = {
            'fact_sales': {
                'invoice_completeness': 'invoice_no',
                'product_completeness': 'product_key',
                'customer_completeness': 'customer_key',
                'transaction_uniqueness': 'sales_key',
                'quantity_range': 'quantity',
                'price_range': 'unit_price',
                'date_validity': 'transaction_datetime'
            },
            'dim_customer': {
                'customer_id_completeness': 'customer_id',
                'country_completeness': 'country',
                'customer_id_uniqueness': 'customer_id'
            },
            'dim_product': {
                'stock_code_completeness': 'stock_code',
                'stock_code_uniqueness': 'stock_code',
                'description_validity': 'description'
            }
        }
        
        return rule_column_mapping.get(table_name, {}).get(rule_name)
    
    def _get_threshold(self, metric_name: str) -> Optional[QualityThreshold]:
        """Get threshold for a metric"""
        for threshold in self.quality_thresholds:
            if threshold.metric_name == metric_name and threshold.enabled:
                return threshold
        return None
    
    def _evaluate_threshold(self, metric_value: float, 
                           threshold: QualityThreshold) -> bool:
        """Evaluate if metric meets threshold"""
        if threshold.operator == ">=":
            return metric_value >= threshold.threshold_value
        elif threshold.operator == "<=":
            return metric_value <= threshold.threshold_value
        elif threshold.operator == "==":
            return abs(metric_value - threshold.threshold_value) < 0.01
        elif threshold.operator == "!=":
            return abs(metric_value - threshold.threshold_value) >= 0.01
        else:
            return True
    
    def persist_quality_metrics(self) -> None:
        """Persist quality metrics to database"""
        if not self.quality_results:
            return
        
        try:
            with get_db_session() as session:
                for result in self.quality_results:
                    metric = DataQualityMetrics(
                        table_name=result.table_name,
                        column_name=result.column_name,
                        metric_name=result.metric_name,
                        metric_value=Decimal(str(result.metric_value)),
                        threshold_value=Decimal(str(result.threshold_value)) if result.threshold_value else None,
                        is_threshold_met=result.is_threshold_met,
                        batch_id=self.batch_id,
                        measured_at=result.measured_at,
                        details=result.details
                    )
                    
                    session.add(metric)
                
                session.commit()
                self.logger.info(f"Persisted {len(self.quality_results)} quality metrics")
                
        except Exception as e:
            self.logger.error(f"Failed to persist quality metrics: {e}")
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get summary of quality check results"""
        if not self.quality_results:
            return {}
        
        total_checks = len(self.quality_results)
        passed_checks = sum(1 for r in self.quality_results 
                           if r.is_threshold_met is True)
        failed_checks = sum(1 for r in self.quality_results 
                           if r.is_threshold_met is False)
        
        # Calculate average scores by metric type
        scores_by_type = {}
        for metric_type in MetricType:
            type_results = [r for r in self.quality_results 
                           if r.metric_type == metric_type]
            if type_results:
                scores_by_type[metric_type.value] = {
                    'average_score': statistics.mean(r.metric_value for r in type_results),
                    'min_score': min(r.metric_value for r in type_results),
                    'max_score': max(r.metric_value for r in type_results),
                    'check_count': len(type_results)
                }
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'success_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            'scores_by_type': scores_by_type,
            'overall_score': statistics.mean(r.metric_value for r in self.quality_results),
            'batch_id': self.batch_id,
            'measured_at': datetime.utcnow().isoformat()
        }
    
    def generate_quality_report(self) -> str:
        """Generate a text-based quality report"""
        if not self.quality_results:
            return "No quality metrics available"
        
        summary = self.get_quality_summary()
        
        report = f"""
DATA QUALITY REPORT
==================
Batch ID: {self.batch_id}
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY
-------
Total Checks: {summary['total_checks']}
Passed: {summary['passed_checks']}
Failed: {summary['failed_checks']}
Success Rate: {summary['success_rate']:.2f}%
Overall Score: {summary['overall_score']:.2f}%

DETAILED RESULTS
---------------
"""
        
        # Group results by table
        results_by_table = {}
        for result in self.quality_results:
            table = result.table_name
            if table not in results_by_table:
                results_by_table[table] = []
            results_by_table[table].append(result)
        
        for table, results in results_by_table.items():
            report += f"\n{table.upper()}\n"
            report += "-" * len(table) + "\n"
            
            for result in results:
                status = "✅ PASS" if result.is_threshold_met else "❌ FAIL"
                threshold_text = f" (Threshold: {result.threshold_value}%)" if result.threshold_value else ""
                
                report += f"{status} {result.metric_name}: {result.metric_value:.2f}%{threshold_text}\n"
                
                if result.details:
                    for key, value in result.details.items():
                        if key != 'error':
                            report += f"    {key}: {value}\n"
        
        return report


# Factory function
def create_quality_monitor(batch_id: str = None) -> DataQualityMonitor:
    """Create and return a configured data quality monitor"""
    return DataQualityMonitor(batch_id)