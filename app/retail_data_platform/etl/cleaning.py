"""
Data Cleaning and Validation Module

Comprehensive data cleaning pipeline with configurable rules,
missing value handling, duplicate detection, and outlier management.
"""

import re
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from statistics import median, mode, stdev

from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config


@dataclass
class CleaningRule:
    """Represents a data cleaning rule"""
    name: str
    description: str
    function: Callable[[Any], Any]
    columns: List[str] = field(default_factory=list)  
    severity: str = "ERROR"  
    enabled: bool = True


@dataclass
class ValidationRule:
    """Represents a data validation rule"""
    name: str
    description: str
    function: Callable[[Any], bool]
    columns: List[str] = field(default_factory=list)
    severity: str = "ERROR"
    enabled: bool = True


@dataclass
class CleaningMetrics:
    """Metrics for data cleaning operations"""
    total_records: int = 0
    records_cleaned: int = 0
    records_rejected: int = 0
    missing_values_handled: int = 0
    duplicates_removed: int = 0
    outliers_detected: int = 0
    validation_errors: int = 0
    cleaning_rules_applied: Dict[str, int] = field(default_factory=dict)
    
    @property
    def cleaning_rate(self) -> float:
        """Calculate cleaning success rate"""
        if self.total_records > 0:
            return (self.records_cleaned / self.total_records) * 100
        return 0.0


class DataCleaner(ABC):
    """Abstract base class for data cleaners"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = ETLLogger(f"cleaning.{name}")
        self.metrics = CleaningMetrics()
    
    @abstractmethod
    def clean(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a single record"""
        pass
    
    def get_metrics(self) -> CleaningMetrics:
        """Get cleaning metrics"""
        return self.metrics


class RetailDataCleaner(DataCleaner):
    """Specialized cleaner for retail sales data"""
    
    def __init__(self):
        super().__init__("retail_sales")
        self.cleaning_rules = self._initialize_cleaning_rules()
        self.validation_rules = self._initialize_validation_rules()
    
    def _initialize_cleaning_rules(self) -> List[CleaningRule]:
        """Initialize cleaning rules for retail data"""
        return [
            CleaningRule(
                name="clean_invoice_no",
                description="Clean and standardize invoice numbers",
                function=self._clean_invoice_number,
                columns=["InvoiceNo"]
            ),
            CleaningRule(
                name="clean_stock_code",
                description="Clean and standardize stock codes",
                function=self._clean_stock_code,
                columns=["StockCode"]
            ),
            CleaningRule(
                name="clean_description",
                description="Clean product descriptions",
                function=self._clean_description,
                columns=["Description"]
            ),
            CleaningRule(
                name="clean_quantity",
                description="Clean and validate quantities",
                function=self._clean_quantity,
                columns=["Quantity"]
            ),
            CleaningRule(
                name="clean_unit_price",
                description="Clean and validate unit prices",
                function=self._clean_unit_price,
                columns=["UnitPrice"]
            ),
            CleaningRule(
                name="clean_customer_id",
                description="Clean customer IDs",
                function=self._clean_customer_id,
                columns=["CustomerID"]
            ),
            CleaningRule(
                name="clean_country",
                description="Standardize country names",
                function=self._clean_country,
                columns=["Country"]
            ),
            CleaningRule(
                name="clean_date",
                description="Parse and validate dates",
                function=self._clean_date,
                columns=["InvoiceDate"]
            )
        ]
    
    def _initialize_validation_rules(self) -> List[ValidationRule]:
        """Initialize validation rules for retail data"""
        return [
            ValidationRule(
                name="validate_invoice_format",
                description="Validate invoice number format",
                function=lambda x: bool(re.match(r'^[C]?\d{5,7}[A-Z]?$', str(x))),
                columns=["InvoiceNo"]
            ),
            ValidationRule(
                name="validate_quantity_exists",
                description="Validate quantity is a number and not zero",
                function=lambda x: isinstance(x, (int, float)) and x != 0,
                columns=["Quantity"]
            ),
            ValidationRule(
                name="validate_non_negative_price",
                description="Validate unit price is non-negative",
                function=lambda x: isinstance(x, (int, float, Decimal)) and x >= 0,
                columns=["UnitPrice"]
            ),
            ValidationRule(
                name="validate_date_range",
                description="Validate invoice date is within reasonable range",
                function=lambda x: isinstance(x, (date, datetime)) and 
                         date(2009, 1, 1) <= x.date() <= date.today(),
                columns=["InvoiceDate"]
            )
        ]
    
    def clean(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a single retail sales record"""
        cleaned_data = data.copy()
        self.metrics.total_records += 1
        
        try:
            # Apply cleaning rules (including invoice cleaning)
            for rule in self.cleaning_rules:
                if rule.enabled:
                    if not rule.columns or any(col in cleaned_data for col in rule.columns):
                        try:
                            if rule.columns:
                                for col in rule.columns:
                                    if col in cleaned_data:
                                        cleaned_data[col] = rule.function(cleaned_data[col])
                            else:
                                cleaned_data = rule.function(cleaned_data)
                            
                            self.metrics.cleaning_rules_applied[rule.name] = \
                                self.metrics.cleaning_rules_applied.get(rule.name, 0) + 1
                                
                        except Exception as e:
                            self.logger.warning(f"Cleaning rule {rule.name} failed", 
                                            error=str(e), record_data=data)
            
            
            # Apply validation rules
            validation_passed = True
            for rule in self.validation_rules:
                if rule.enabled:
                    for col in rule.columns:
                        if col in cleaned_data:
                            if not rule.function(cleaned_data[col]):
                                validation_passed = False
                                self.metrics.validation_errors += 1
                                
                                if rule.severity == "ERROR":
                                    self.logger.error(f"Validation failed: {rule.description}",
                                                    column=col, value=cleaned_data[col])
                                    break
                                else:
                                    self.logger.warning(f"Validation warning: {rule.description}",
                                                    column=col, value=cleaned_data[col])
            
            if validation_passed:
                self.metrics.records_cleaned += 1
                return cleaned_data
            else:
                self.metrics.records_rejected += 1
                return None
                
        except Exception as e:
            self.logger.error(f"Record cleaning failed: {e}", record_data=data)
            self.metrics.records_rejected += 1
            return None
        
    def _clean_invoice_number(self, invoice_no: str) -> str:
        """Clean invoice number"""
        if not invoice_no:
            return ""
        
        # Remove whitespace and convert to uppercase
        cleaned = str(invoice_no).strip().upper()
        
        return cleaned
    
    def _clean_stock_code(self, stock_code: str) -> str:
        """Clean stock code"""
        if not stock_code:
            return ""
        
        # Remove whitespace and standardize case
        cleaned = str(stock_code).strip().upper()
        
        # Remove special characters except alphanumeric and common separators
        cleaned = re.sub(r'[^\w\-\.]', '', cleaned)
        
        return cleaned
    
    def _clean_description(self, description: str) -> str:
        """Clean product description"""
        if not description:
            return ""
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', str(description)).strip()
        
        # Standardize case (title case)
        cleaned = cleaned.title()
        
        # Remove special characters at the end
        cleaned = re.sub(r'[\.\,\-\s]+$', '', cleaned)
        
        return cleaned
    
    def _clean_quantity(self, quantity: str) -> int:
        """Clean and convert quantity to integer, handling returns"""
        if not quantity:
            return 0
        
        try:
            # Handle string representations
            if isinstance(quantity, str):
                quantity = quantity.strip()
                # Remove any non-numeric characters except minus sign
                quantity = re.sub(r'[^\d\-\.]', '', quantity)
            
            qty_value = int(float(quantity))
            
            return qty_value
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid quantity value: {quantity}")
            return 0
    
    def _clean_unit_price(self, unit_price: str) -> Decimal:
        """Clean and convert unit price to decimal"""
        if not unit_price:
            return Decimal('0.00')
        
        try:
            # Handle string representations
            if isinstance(unit_price, str):
                unit_price = unit_price.strip()
                # Remove currency symbols and spaces
                unit_price = re.sub(r'[£$€\s,]', '', unit_price)
            
            return Decimal(str(unit_price)).quantize(Decimal('0.01'))
        except (ValueError, TypeError, InvalidOperation):
            self.logger.warning(f"Invalid unit price value: {unit_price}")
            return Decimal('0.00')
    
    def _clean_customer_id(self, customer_id: str) -> str:
        """Clean customer ID"""
        if not customer_id:
            return ""
        
        # Convert to string and remove decimal points if it's a float string
        cleaned = str(customer_id).strip()
        if cleaned.endswith('.0'):
            cleaned = cleaned[:-2]
        
        return cleaned
    
    def _clean_country(self, country: str) -> str:
        """Standardize country names"""
        if not country:
            return ""
        
        country = str(country).strip().title()
        
        # Country name standardization mapping
        country_mapping = {
            'Uk': 'United Kingdom',
            'Usa': 'United States',
            'Uae': 'United Arab Emirates',
            'Rsa': 'South Africa',
        }
        
        return country_mapping.get(country, country)
    
    def _clean_date(self, date_str: str) -> datetime:
        """Parse and clean invoice date"""
        if not date_str:
            raise ValueError("Empty date string")
        
        # Common date formats to try
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M',
            '%d-%m-%Y %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y'
        ]
        
        date_str = str(date_str).strip()
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # If no format worked, try pandas parsing
        try:
            return pd.to_datetime(date_str)
        except Exception:
            raise ValueError(f"Could not parse date: {date_str}")


class DuplicateHandler:
    """Handles duplicate record detection and removal"""
    
    def __init__(self, key_columns: List[str], strategy: str = "keep_latest"):
        self.key_columns = key_columns
        self.strategy = strategy  # keep_latest, keep_first, remove_all
        self.logger = ETLLogger("cleaning.duplicates")
        self.seen_keys: Set[str] = set()
        self.duplicate_count = 0
    
    def is_duplicate(self, record: Dict[str, Any]) -> bool:
        """Check if record is a duplicate"""
        # Create composite key from specified columns
        key_values = []
        for col in self.key_columns:
            key_values.append(str(record.get(col, '')))
        
        composite_key = '|'.join(key_values)
        
        if composite_key in self.seen_keys:
            self.duplicate_count += 1
            return True
        
        self.seen_keys.add(composite_key)
        return False
    
    def get_duplicate_count(self) -> int:
        """Get number of duplicates detected"""
        return self.duplicate_count


class OutlierDetector:
    """Detects outliers in numerical data"""
    
    def __init__(self, method: str = "iqr", threshold: float = 1.5):
        self.method = method  # iqr, zscore, modified_zscore
        self.threshold = threshold
        self.logger = ETLLogger("cleaning.outliers")
        self.outlier_count = 0
    
    def is_outlier(self, value: float, column_data: List[float]) -> bool:
        """Check if value is an outlier"""
        if self.method == "iqr":
            return self._iqr_outlier(value, column_data)
        elif self.method == "zscore":
            return self._zscore_outlier(value, column_data)
        else:
            return False
    
    def _iqr_outlier(self, value: float, data: List[float]) -> bool:
        """Detect outlier using IQR method"""
        try:
            q1 = np.percentile(data, 25)
            q3 = np.percentile(data, 75)
            iqr = q3 - q1
            
            lower_bound = q1 - (self.threshold * iqr)
            upper_bound = q3 + (self.threshold * iqr)
            
            is_outlier = value < lower_bound or value > upper_bound
            if is_outlier:
                self.outlier_count += 1
                
            return is_outlier
        except Exception:
            return False
    
    def _zscore_outlier(self, value: float, data: List[float]) -> bool:
        """Detect outlier using Z-score method"""
        try:
            mean_val = np.mean(data)
            std_val = np.std(data)
            
            if std_val == 0:
                return False
            
            z_score = abs((value - mean_val) / std_val)
            is_outlier = z_score > self.threshold
            
            if is_outlier:
                self.outlier_count += 1
                
            return is_outlier
        except Exception:
            return False


class MissingValueHandler:
    """Handles missing values with various strategies"""
    
    def __init__(self, strategy_map: Dict[str, str]):
        """
        Initialize with column-specific strategies
        
        Args:
            strategy_map: Dict mapping column names to strategies
                         (drop, fill_mean, fill_median, fill_mode, fill_zero, fill_unknown)
        """
        self.strategy_map = strategy_map
        self.logger = ETLLogger("cleaning.missing_values")
        self.missing_count = 0
    
    def handle_missing(self, record: Dict[str, Any], 
                      reference_data: Dict[str, List[Any]] = None) -> Optional[Dict[str, Any]]:
        """Handle missing values in a record"""
        cleaned_record = record.copy()
        
        for column, value in record.items():
            if self._is_missing(value):
                self.missing_count += 1
                strategy = self.strategy_map.get(column, "fill_unknown")
                
                if strategy == "drop":
                    return None  # Drop entire record
                
                cleaned_record[column] = self._apply_strategy(
                    strategy, column, reference_data
                )
        
        return cleaned_record
    
    def _is_missing(self, value: Any) -> bool:
        """Check if value is considered missing"""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        if isinstance(value, float) and np.isnan(value):
            return True
        return False
    
    def _apply_strategy(self, strategy: str, column: str, 
                       reference_data: Dict[str, List[Any]] = None) -> Any:
        """Apply missing value strategy"""
        if strategy == "fill_zero":
            return 0
        elif strategy == "fill_unknown":
            return "Unknown"
        elif strategy == "fill_mean" and reference_data and column in reference_data:
            try:
                return np.mean([x for x in reference_data[column] if not self._is_missing(x)])
            except:
                return 0
        elif strategy == "fill_median" and reference_data and column in reference_data:
            try:
                return median([x for x in reference_data[column] if not self._is_missing(x)])
            except:
                return 0
        elif strategy == "fill_mode" and reference_data and column in reference_data:
            try:
                valid_values = [x for x in reference_data[column] if not self._is_missing(x)]
                return mode(valid_values) if valid_values else "Unknown"
            except:
                return "Unknown"
        else:
            return "Unknown"


class DataCleaningPipeline:
    """
    Orchestrates the complete data cleaning process
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._get_default_config()
        self.logger = ETLLogger("cleaning.pipeline")
        
        # Initialize components
        self.cleaner = RetailDataCleaner()
        self.duplicate_handler = DuplicateHandler(
            key_columns=self.config.get('duplicate_key_columns', ['InvoiceNo', 'StockCode']),
            strategy=self.config.get('duplicate_strategy', 'keep_latest')
        )
        self.outlier_detector = OutlierDetector(
            method=self.config.get('outlier_method', 'iqr'),
            threshold=self.config.get('outlier_threshold', 1.5)
        )
        self.missing_value_handler = MissingValueHandler(
            strategy_map=self.config.get('missing_value_strategies', {
                'CustomerID': 'fill_unknown',
                'Description': 'fill_unknown',
                'Quantity': 'drop',
                'UnitPrice': 'drop',
                'InvoiceDate': 'drop'
            })
        )
        
        self.total_processed = 0
        self.total_cleaned = 0
        self.total_rejected = 0
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default cleaning configuration"""
        return {
            'enable_duplicate_detection': True,
            'enable_outlier_detection': True,
            'enable_missing_value_handling': True,
            'quality_threshold': 0.95,
            'duplicate_key_columns': ['InvoiceNo', 'StockCode'],
            'duplicate_strategy': 'keep_latest',
            'outlier_method': 'iqr',
            'outlier_threshold': 1.5,
            'missing_value_strategies': {
                'CustomerID': 'fill_unknown',
                'Description': 'fill_unknown',
                'Quantity': 'drop',
                'UnitPrice': 'drop',
                'InvoiceDate': 'drop'
            }
        }
    
    def clean_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean a single record through the complete pipeline"""
        self.total_processed += 1
        
        try:
            # Step 1: Handle missing values
            if self.config.get('enable_missing_value_handling', True):
                record = self.missing_value_handler.handle_missing(record)
                if record is None:
                    self.total_rejected += 1
                    return None
            
            # Step 2: Apply data cleaning rules
            cleaned_record = self.cleaner.clean(record)
            if cleaned_record is None:
                self.total_rejected += 1
                return None
            
            # Step 3: Check for duplicates
            if self.config.get('enable_duplicate_detection', True):
                if self.duplicate_handler.is_duplicate(cleaned_record):
                    self.logger.debug("Duplicate record detected", record=cleaned_record)
                    self.total_rejected += 1
                    return None
            
            # Step 4: Detect outliers (for numerical columns)
            if self.config.get('enable_outlier_detection', True):
                self._check_extreme_values(cleaned_record)
            
            self.total_cleaned += 1
            return cleaned_record
            
        except Exception as e:
            self.logger.error(f"Record cleaning failed: {e}", record=record)
            self.total_rejected += 1
            return None
    
    def _check_extreme_values(self, record: Dict[str, Any]) -> None:
        """Check for extreme values that might be outliers"""
        try:
            quantity = float(record.get('Quantity', 0))
            unit_price = float(record.get('UnitPrice', 0))
            
            # Basic extreme value checks
            if abs(quantity) > 10000:
                self.logger.warning("Extreme quantity detected", 
                                  quantity=quantity, record=record)
            
            if unit_price > 1000:
                self.logger.warning("Extreme unit price detected", 
                                  unit_price=unit_price, record=record)
                                  
        except (ValueError, TypeError):
            pass


# Factory function
def create_cleaning_pipeline(config: Optional[Dict[str, Any]] = None) -> DataCleaningPipeline:
    """Create and return a configured data cleaning pipeline"""
    return DataCleaningPipeline(config)