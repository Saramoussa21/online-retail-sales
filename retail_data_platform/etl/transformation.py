"""
Data Transformation Engine

Handles data transformation including type conversion, normalization,
business rule application, and dimensional modeling transformations.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
import uuid
import hashlib
import os
import time
from pathlib import Path

from sqlalchemy import text
from ..database.connection import get_db_session
from ..database.models import DimCustomer, DimProduct, DimDate, FactSales
from ..utils.logging_config import ETLLogger
from ..config.config_manager import get_config


@dataclass
class TransformationRule:
    """Represents a data transformation rule"""
    name: str
    description: str
    source_columns: List[str]
    target_column: str
    transformation_function: Callable
    data_type: str = "string"
    required: bool = True
    default_value: Any = None


@dataclass
class TransformationMetrics:
    """Metrics for transformation operations"""
    total_records: int = 0
    successful_transformations: int = 0
    failed_transformations: int = 0
    transformation_rules_applied: Dict[str, int] = field(default_factory=dict)
    dimension_lookups: Dict[str, int] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        """Calculate transformation success rate"""
        if self.total_records > 0:
            return (self.successful_transformations / self.total_records) * 100
        return 0.0


class DataTransformer(ABC):
    """Abstract base class for data transformers"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = ETLLogger(f"transformation.{name}")
        self.metrics = TransformationMetrics()
    
    @abstractmethod
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record"""
        pass
    
    def get_metrics(self) -> TransformationMetrics:
        """Get transformation metrics"""
        return self.metrics


class DimensionLookupCache:
    """Cache for dimension lookups to improve performance"""
    
    def __init__(self, cache_size: int = 10000):
        self.cache_size = cache_size
        self._caches = {
            'customer': {},
            'product': {},
            'date': {}
        }
        self.logger = ETLLogger("transformation.cache")
    
    def get_customer_key(self, customer_id: str, country: str) -> Optional[int]:
        """Get customer key from cache"""
        cache_key = f"{customer_id}|{country}"
        return self._caches['customer'].get(cache_key)
    
    def set_customer_key(self, customer_id: str, country: str, customer_key: int) -> None:
        """Set customer key in cache"""
        cache_key = f"{customer_id}|{country}"
        self._caches['customer'][cache_key] = customer_key
        self._manage_cache_size('customer')
    
    def get_product_key(self, stock_code: str) -> Optional[int]:
        """Get product key from cache"""
        return self._caches['product'].get(stock_code)
    
    def set_product_key(self, stock_code: str, product_key: int) -> None:
        """Set product key in cache"""
        self._caches['product'][stock_code] = product_key
        self._manage_cache_size('product')
    
    def get_date_key(self, date_value: date) -> Optional[int]:
        """Get date key from cache"""
        date_str = date_value.strftime('%Y-%m-%d')
        return self._caches['date'].get(date_str)
    
    def set_date_key(self, date_value: date, date_key: int) -> None:
        """Set date key in cache"""
        date_str = date_value.strftime('%Y-%m-%d')
        self._caches['date'][date_str] = date_key
        self._manage_cache_size('date')
    
    def _manage_cache_size(self, cache_name: str) -> None:
        """Manage cache size by removing oldest entries"""
        cache = self._caches[cache_name]
        if len(cache) > self.cache_size:
            items_to_remove = len(cache) - int(self.cache_size * 0.8)
            keys_to_remove = list(cache.keys())[:items_to_remove]
            for key in keys_to_remove:
                del cache[key]
    
    def clear_all(self) -> None:
        """Clear all caches"""
        for cache in self._caches.values():
            cache.clear()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {name: len(cache) for name, cache in self._caches.items()}


class RetailDataTransformer(DataTransformer):
    """Transformer specialized for retail sales data to dimensional model"""
    
    def __init__(self):
        super().__init__("retail_dimensional")
        self.config = get_config()
        self.cache = DimensionLookupCache()
        self.transformation_rules = self._initialize_transformation_rules()
        
    def _initialize_transformation_rules(self) -> List[TransformationRule]:
        """Initialize transformation rules"""
        return [
            TransformationRule(
                name="generate_batch_id",
                description="Generate batch ID for lineage tracking",
                source_columns=[],
                target_column="batch_id",
                transformation_function=lambda x: str(uuid.uuid4()),
                data_type="string"
            ),
            TransformationRule(
                name="calculate_line_total",
                description="Calculate line total from quantity and unit price",
                source_columns=["Quantity", "UnitPrice"],
                target_column="line_total",
                transformation_function=self._calculate_line_total,
                data_type="decimal"
            ),
            TransformationRule(
                name="extract_transaction_date",
                description="Extract date from invoice datetime",
                source_columns=["InvoiceDate"],
                target_column="transaction_date",
                transformation_function=self._extract_date,
                data_type="date"
            ),
            TransformationRule(
                name="determine_transaction_type",
                description="Determine if transaction is sale or return",
                source_columns=["InvoiceNo", "Quantity"],
                target_column="transaction_type",
                transformation_function=self._determine_transaction_type,
                data_type="string"
            ),
            TransformationRule(
                name="standardize_customer_id",
                description="Standardize customer ID format",
                source_columns=["CustomerID"],
                target_column="customer_id_std",
                transformation_function=self._standardize_customer_id,
                data_type="string"
            )
        ]
    
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform retail record to dimensional model format"""
        self.metrics.total_records += 1
        
        try:
            transformed_record = {}
            

            original_invoice = str(record.get('InvoiceNo', '')).strip()
            print("original_invoice",original_invoice)
            is_return = original_invoice.startswith('C')
            
            print(f"🔍 DEBUG: Original invoice: {original_invoice}, Is return: {is_return}")  # Debug line
            
            # Apply transformation rules
            for rule in self.transformation_rules:
                try:
                    if rule.source_columns:
                        source_values = {col: record.get(col) for col in rule.source_columns}
                        transformed_value = rule.transformation_function(source_values)
                    else:
                        transformed_value = rule.transformation_function(record)
                    
                    transformed_record[rule.target_column] = transformed_value
                    
                    self.metrics.transformation_rules_applied[rule.name] = \
                        self.metrics.transformation_rules_applied.get(rule.name, 0) + 1
                        
                except Exception as e:
                    if rule.required:
                        self.logger.error(f"Required transformation failed: {rule.name}", 
                                        error=str(e), record=record)
                        raise
                    else:
                        self.logger.warning(f"Optional transformation failed: {rule.name}",
                                        error=str(e))
                        transformed_record[rule.target_column] = rule.default_value
            
            if is_return:
                clean_invoice = original_invoice[1:] 
                transformed_record['invoice_no'] = int(clean_invoice) if clean_invoice.isdigit() else 0
                print(f"🔍 DEBUG: Cleaned invoice number: {transformed_record['invoice_no']}")
                transformed_record['transaction_type'] = 'RETURN'
                print(f"🔍 DEBUG: Transaction type set to: {transformed_record['transaction_type']}")
                # Handle negative quantities for returns
                quantity = float(record.get('Quantity', 0))
                unit_price = float(record.get('UnitPrice', 0))
                
                transformed_record['quantity'] = abs(int(quantity))  # Always positive
                transformed_record['line_total'] = quantity * unit_price  # Keep negative
                
                print(f"🔍 DEBUG: Return processed - quantity: {transformed_record['quantity']}, line_total: {transformed_record['line_total']}")
            else:
                transformed_record['invoice_no'] = int(original_invoice) if original_invoice.isdigit() else 0
                transformed_record['transaction_type'] = 'SALE'
                
                quantity = float(record.get('Quantity', 0))
                unit_price = float(record.get('UnitPrice', 0))
                
                transformed_record['quantity'] = abs(int(quantity))
                transformed_record['line_total'] = abs(quantity * unit_price)
                
                print(f"🔍 DEBUG: Sale processed - quantity: {transformed_record['quantity']}, line_total: {transformed_record['line_total']}")
            
            # Perform dimension lookups
            dimension_keys = self._lookup_dimension_keys(record, transformed_record)
            transformed_record.update(dimension_keys)
            
            # Copy other required fields
            transformed_record['transaction_datetime'] = record.get('InvoiceDate')
            transformed_record['unit_price'] = float(record.get('UnitPrice', 0))
            transformed_record['created_at'] = datetime.utcnow()
            transformed_record['batch_id'] = record.get('_ingestion_batch_id')
            transformed_record['data_source'] = 'CSV'
            
            self.metrics.successful_transformations += 1
            return transformed_record
            
        except Exception as e:
            self.logger.error(f"Record transformation failed: {e}", record=record)
            self.metrics.failed_transformations += 1
            return None
    
    def _calculate_line_total(self, values: Dict[str, Any]) -> Decimal:
        """Calculate line total from quantity and unit price"""
        try:
            quantity = Decimal(str(values['Quantity']))
            unit_price = Decimal(str(values['UnitPrice']))
            return quantity * unit_price
        except (ValueError, TypeError, KeyError):
            return Decimal('0.00')
    
    def _extract_date(self, values: Dict[str, Any]) -> date:
        """Extract date from datetime"""
        try:
            invoice_date = values['InvoiceDate']
            if isinstance(invoice_date, datetime):
                return invoice_date.date()
            elif isinstance(invoice_date, date):
                return invoice_date
            else:
                # Try to parse string
                return datetime.strptime(str(invoice_date), '%Y-%m-%d %H:%M:%S').date()
        except (ValueError, TypeError, KeyError):
            return date.today()
    

    def _determine_transaction_type(self, values: Dict[str, Any]) -> str:
        """🔥 UPDATED: Determine transaction type based on C prefix"""
        try:
            print(values)
            invoice_no = str(values.get('InvoiceNo', ''))
            
            if invoice_no.startswith('C'):
                return 'RETURN'
            else:
                return 'SALE'
        except (ValueError, TypeError):
            return 'SALE' 
    
    def _standardize_customer_id(self, values: Dict[str, Any]) -> str:
        """Standardize customer ID format"""
        customer_id = values.get('CustomerID', '')
        if not customer_id or customer_id == 'Unknown':
            return 'GUEST'
        
        customer_id = str(customer_id)
        if customer_id.endswith('.0'):
            customer_id = customer_id[:-2]
        
        return customer_id.strip()
    
    def _lookup_dimension_keys(self, original_record: Dict[str, Any], 
                              transformed_record: Dict[str, Any]) -> Dict[str, int]:
        """Lookup dimension keys from dimension tables"""
        dimension_keys = {}
        
        try:
            # Customer dimension lookup
            customer_id = transformed_record.get('customer_id_std', 'GUEST')
            country = original_record.get('Country', 'Unknown')
            
            customer_key = self._lookup_customer_key(customer_id, country)
            if customer_key:
                dimension_keys['customer_key'] = customer_key
                self.metrics.dimension_lookups['customer'] = \
                    self.metrics.dimension_lookups.get('customer', 0) + 1
            
            # Product dimension lookup
            stock_code = original_record.get('StockCode', '')
            description = original_record.get('Description', '')
            
            product_key = self._lookup_product_key(stock_code, description)
            if product_key:
                dimension_keys['product_key'] = product_key
                self.metrics.dimension_lookups['product'] = \
                    self.metrics.dimension_lookups.get('product', 0) + 1
            
            # Date dimension lookup
            transaction_date = transformed_record.get('transaction_date')
            if transaction_date:
                date_key = self._lookup_date_key(transaction_date)
                if date_key:
                    dimension_keys['date_key'] = date_key
                    self.metrics.dimension_lookups['date'] = \
                        self.metrics.dimension_lookups.get('date', 0) + 1
            
        except Exception as e:
            self.logger.error(f"Dimension lookup failed: {e}", 
                            original_record=original_record)
        
        return dimension_keys
    
    def _lookup_customer_key(self, customer_id: str, country: str) -> Optional[int]:
        """Lookup or create customer dimension key using ORM"""
        # Check cache first
        cached_key = self.cache.get_customer_key(customer_id, country)
        if cached_key:
            return cached_key
        
        try:
            with get_db_session() as session:
                # Check if current record exists
                existing_customer = session.query(DimCustomer).filter(
                    DimCustomer.customer_id == customer_id,
                    DimCustomer.is_current == True
                ).first()
                
                if existing_customer:
                    # Check if country changed (SCD Type 2)
                    if existing_customer.country != country:
                        # Close existing record
                        existing_customer.is_current = False
                        existing_customer.expiry_date = datetime.utcnow()
                        existing_customer.updated_at = datetime.utcnow()
                        
                        # Create new record
                        new_customer = DimCustomer(
                            customer_id=customer_id,
                            country=country,
                            is_current=True,
                            effective_date=datetime.utcnow()
                        )
                        session.add(new_customer)
                        session.commit()
                        
                        customer_key = new_customer.customer_key
                    else:
                        customer_key = existing_customer.customer_key
                else:
                    # Create new customer
                    new_customer = DimCustomer(
                        customer_id=customer_id,
                        country=country,
                        is_current=True,
                        effective_date=datetime.utcnow()
                    )
                    session.add(new_customer)
                    session.commit()
                    
                    customer_key = new_customer.customer_key
                
                # Cache the result
                if customer_key:
                    self.cache.set_customer_key(customer_id, country, customer_key)
                    return customer_key
                    
        except Exception as e:
            self.logger.error(f"Customer lookup failed: {e}", 
                            customer_id=customer_id, country=country)
        
        return None
    
    def _lookup_product_key(self, stock_code: str, description: str) -> Optional[int]:
        """Lookup or create product dimension key using ORM"""
        # Check cache first
        cached_key = self.cache.get_product_key(stock_code)
        if cached_key:
            return cached_key
        
        try:
            with get_db_session() as session:
                # Check if product exists
                existing_product = session.query(DimProduct).filter(
                    DimProduct.stock_code == stock_code
                ).first()
                
                if existing_product:
                    # Update description if new one is more detailed
                    if description and len(description) > len(existing_product.description or ''):
                        existing_product.description = description
                        existing_product.updated_at = datetime.utcnow()
                        session.commit()
                    
                    product_key = existing_product.product_key
                else:
                    # Derive category and gift flag from description
                    category = self._derive_product_category(description)
                    is_gift = self._is_gift_product(description)
                    
                    # Create new product
                    new_product = DimProduct(
                        stock_code=stock_code,
                        description=description,
                        category=category,
                        is_gift=is_gift
                    )
                    session.add(new_product)
                    session.commit()
                    
                    product_key = new_product.product_key
                
                # Cache the result
                if product_key:
                    self.cache.set_product_key(stock_code, product_key)
                    return product_key
                    
        except Exception as e:
            self.logger.error(f"Product lookup failed: {e}", 
                            stock_code=stock_code, description=description)
        
        return None
    
    def _derive_product_category(self, description: str) -> str:
        """Derive product category from description"""
        if not description:
            return 'General Merchandise'
        
        desc_upper = description.upper()
        
        if any(word in desc_upper for word in ['HEART', 'LOVE']):
            return 'Home Decor'
        elif any(word in desc_upper for word in ['MUG', 'CUP']):
            return 'Drinkware'
        elif any(word in desc_upper for word in ['BAG', 'LUNCH']):
            return 'Bags & Storage'
        elif any(word in desc_upper for word in ['LIGHT', 'LANTERN']):
            return 'Lighting'
        elif 'BOTTLE' in desc_upper:
            return 'Home & Garden'
        elif any(word in desc_upper for word in ['CAKE', 'BAKING']):
            return 'Kitchen & Dining'
        else:
            return 'General Merchandise'
    
    def _is_gift_product(self, description: str) -> bool:
        """Determine if product is a gift item"""
        if not description:
            return False
        
        desc_upper = description.upper()
        return any(word in desc_upper for word in ['GIFT', 'PRESENT', 'VALENTINE', 'CHRISTMAS'])
    
    def _lookup_date_key(self, date_value: date) -> Optional[int]:
        """Lookup or create date dimension key using ORM"""
        # Check cache first
        cached_key = self.cache.get_date_key(date_value)
        if cached_key:
            return cached_key
        
        try:
            with get_db_session() as session:
                # Convert date to date key format (YYYYMMDD)
                date_key = int(date_value.strftime('%Y%m%d'))
                
                # Check if date exists in dimension table
                existing_date = session.query(DimDate).filter(
                    DimDate.date_key == date_key
                ).first()
                
                if existing_date:
                    self.cache.set_date_key(date_value, date_key)
                    return date_key
                else:
                    # Create the date dimension record if it doesn't exist
                    new_date = self._create_date_dimension_record(date_value, date_key)
                    session.add(new_date)
                    session.commit()
                    
                    self.cache.set_date_key(date_value, date_key)
                    return date_key
                    
        except Exception as e:
            self.logger.error(f"Date lookup failed: {e}", 
                            date_value=str(date_value))
        
        return None
    
    def _create_date_dimension_record(self, date_value: date, date_key: int) -> DimDate:
        """Create a date dimension record"""
        return DimDate(
            date_key=date_key,
            date_value=date_value,
            year=date_value.year,
            quarter=(date_value.month - 1) // 3 + 1,
            month=date_value.month,
            week=date_value.isocalendar()[1],
            day_of_year=date_value.timetuple().tm_yday,
            day_of_month=date_value.day,
            day_of_week=date_value.weekday(),
            month_name=date_value.strftime('%B'),
            day_name=date_value.strftime('%A'),
            quarter_name=f'Q{(date_value.month - 1) // 3 + 1}',
            is_weekend=date_value.weekday() >= 5,
            is_holiday=False  
        )
    

    def _copy_required_fields(self, original: Dict[str, Any], 
                            transformed: Dict[str, Any]) -> None:
        """Copy required fields from original record with transaction type logic"""
        
        original_invoice = str(original.get('InvoiceNo', '')).strip()
        is_return = original_invoice.startswith('C')
        
        if is_return:
            clean_invoice = original_invoice[1:]  # C536548 → 536548
            transformed['invoice_no'] = int(clean_invoice) if clean_invoice.isdigit() else 0
            transformed['transaction_type'] = 'RETURN'
        else:
            transformed['invoice_no'] = int(original_invoice) if original_invoice.isdigit() else 0
            transformed['transaction_type'] = 'SALE'
        
        # 🔥 CHANGE 2: Handle quantity (always positive for analysis)
        quantity = float(original.get('Quantity', 0))
        transformed['quantity'] = abs(int(quantity))  # Always positive
        
        # 🔥 CHANGE 3: Handle unit price
        transformed['unit_price'] = float(original.get('UnitPrice', 0))
        
        # 🔥 CHANGE 4: Calculate line_total based on transaction type
        if is_return:
            # For returns: Keep line_total negative to show it's a cost/refund
            transformed['line_total'] = quantity * float(original.get('UnitPrice', 0))  # Will be negative
        else:
            # For sales: Keep line_total positive to show it's revenue
            transformed['line_total'] = abs(quantity) * float(original.get('UnitPrice', 0))  # Always positive
        
        # Handle datetime
        transformed['transaction_datetime'] = original.get('InvoiceDate')


class BusinessRuleEngine:
    """Applies business rules and calculations"""
    
    def __init__(self):
        self.logger = ETLLogger("transformation.business_rules")
        self.rules_applied = 0
    
    def apply_retail_business_rules(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Apply business rules specific to retail data"""
        enhanced_record = record.copy()
        
        try:
            # Calculate derived metrics
            enhanced_record.update(self._calculate_derived_metrics(record))
            
            # Apply business categorizations
            enhanced_record.update(self._apply_business_categorizations(record))
            
            # Add audit fields
            enhanced_record.update(self._add_audit_fields(record))
            
            self.rules_applied += 1
            
        except Exception as e:
            self.logger.error(f"Business rule application failed: {e}", record=record)
        
        return enhanced_record
    
    def _calculate_derived_metrics(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived business metrics"""
        try:
            quantity = float(record.get('quantity', 0))
            unit_price = float(record.get('unit_price', 0))
            line_total = float(record.get('line_total', 0))
            
            return {
                'revenue_impact': abs(line_total),  # Absolute value for returns
                'volume_impact': abs(quantity),     # Absolute value for returns
                'avg_unit_value': line_total / quantity if quantity != 0 else 0,
                'is_high_value': line_total > 100,  # Business rule: high value threshold
                'is_bulk_purchase': quantity > 10   # Business rule: bulk purchase threshold
            }
        except (ValueError, TypeError, ZeroDivisionError):
            return {
                'revenue_impact': 0,
                'volume_impact': 0,
                'avg_unit_value': 0,
                'is_high_value': False,
                'is_bulk_purchase': False
            }
    

    def _apply_business_categorizations(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """🔥 UPDATED: Apply business categorizations with transaction type"""
        try:
            transaction_type = record.get('transaction_type', 'SALE')
            original_invoice = str(record.get('InvoiceNo', ''))
            country = record.get('Country', 'Unknown')
            
            return {
                'is_return': transaction_type == 'RETURN',
                'is_cancellation': original_invoice.startswith('C'),  # Same as is_return
                'is_domestic': country == 'United Kingdom',
                'market_segment': self._determine_market_segment(country),
                'customer_type': self._determine_customer_type(record)
            }
        except Exception:
            return {
                'is_return': False,
                'is_cancellation': False,
                'is_domestic': True,
                'market_segment': 'Unknown',
                'customer_type': 'Unknown'
            }
    
    def _determine_market_segment(self, country: str) -> str:
        """Determine market segment based on country"""
        domestic_markets = ['United Kingdom', 'UK']
        european_markets = ['France', 'Germany', 'Spain', 'Italy', 'Netherlands', 'Belgium']
        
        if country in domestic_markets:
            return 'Domestic'
        elif country in european_markets:
            return 'European'
        else:
            return 'International'
    
    def _determine_customer_type(self, record: Dict[str, Any]) -> str:
        """Determine customer type based on customer ID"""
        customer_id = record.get('customer_id_std', 'GUEST')
        
        if customer_id == 'GUEST' or customer_id == 'Unknown':
            return 'Guest'
        else:
            return 'Registered'
    
    def _add_audit_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add audit and metadata fields"""
        return {
            'created_at': datetime.utcnow(),
            'etl_batch_id': record.get('batch_id', ''),
            'data_source': 'CSV_IMPORT',
            'record_hash': self._calculate_record_hash(record)
        }
    
    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """Calculate hash for record integrity checking"""
        # Create a consistent string representation of key fields
        key_fields = ['invoice_no', 'stock_code', 'quantity', 'unit_price']
        hash_string = '|'.join(str(record.get(field, '')) for field in key_fields)
        
        return hashlib.md5(hash_string.encode()).hexdigest()

class DataTransformationPipeline:
    """
    Orchestrates the complete data transformation process
    """
    
    def __init__(self):
        self.logger = ETLLogger("transformation.pipeline")
        self.transformer = RetailDataTransformer()
        self.business_rules = BusinessRuleEngine()
        
        self.total_processed = 0
        self.total_transformed = 0
        self.total_failed = 0
    
    def _create_version_for_job(self, job_name: str, source_file: str = None) -> int:
        """Create a new data version for this ETL job"""
        import hashlib
        from datetime import datetime
        
        # Generate version number based on timestamp
        version_number = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Calculate file hash if source file exists
        file_hash = None
        if source_file and os.path.exists(source_file):
            try:
                with open(source_file, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()[:16]
            except:
                file_hash = None
        
        # Create version record
        with get_db_session() as session:
            query = text("""
                INSERT INTO retail_dw.data_versions (
                    version_number, version_type, description, 
                    source_file, file_hash, etl_job_id, status
                ) VALUES (
                    :version_number, 'INCREMENTAL', :description,
                    :source_file, :file_hash, :job_name, 'ACTIVE'
                ) RETURNING version_id
            """)
            
            result = session.execute(query, {
                'version_number': version_number,
                'description': f'ETL load from {os.path.basename(source_file) if source_file else "manual"}',
                'source_file': source_file,
                'file_hash': file_hash,
                'job_name': job_name
            })
            
            version_id = result.scalar()
            session.commit()
            
            self.logger.info(f"✅ Created data version {version_number} (ID: {version_id}) for job {job_name}")
            return version_id

    def _tag_data_with_version(self, version_id: int):
        """Tag newly loaded data with version ID"""
        with get_db_session() as session:
            try:
                queries = [
                    "UPDATE retail_dw.fact_sales SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL",
                    "UPDATE retail_dw.dim_customer SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL AND is_current = true",
                    "UPDATE retail_dw.dim_product SET version_id = :version_id, version_created_at = NOW() WHERE version_id IS NULL"
                ]
                
                total_updated = 0
                for query in queries:
                    result = session.execute(text(query), {'version_id': version_id})
                    total_updated += result.rowcount
                
                session.commit()
                self.logger.info(f"✅ Tagged {total_updated} records with version {version_id}")
                
            except Exception as e:
                session.rollback()
                self.logger.error(f"Failed to tag data with version: {e}")

    def _update_version_record_count(self, version_id: int):
        """Update the record count in the version table"""
        with get_db_session() as session:
            # Count records for this version
            count_query = text("SELECT COUNT(*) FROM retail_dw.fact_sales WHERE version_id = :version_id")
            result = session.execute(count_query, {'version_id': version_id})
            record_count = result.scalar()
            
            # Update version record
            update_query = text("UPDATE retail_dw.data_versions SET records_count = :count WHERE version_id = :version_id")
            session.execute(update_query, {'count': record_count, 'version_id': version_id})
            session.commit()
            
            self.logger.info(f"✅ Updated version {version_id} with {record_count} records")

    def transform_and_load(self, df, job_name: str = "manual_load", source_file: str = None) -> Dict[str, Any]:
        """Transform data and load to warehouse with automatic versioning"""
        start_time = time.time()
        
        try:

            version_id = self._create_version_for_job(job_name, source_file)
            
            self.logger.info(f"Starting data transformation for {len(df)} records with version {version_id}")
            
            successful_records = []
            failed_records = []
            
            for index, record in df.iterrows():
                try:
                    transformed_record = self.transform_record(record.to_dict())
                    
                    if transformed_record:
                        successful_records.append(transformed_record)
                    else:
                        failed_records.append(record.to_dict())
                        
                except Exception as e:
                    self.logger.error(f"Record transformation failed at index {index}: {e}")
                    failed_records.append(record.to_dict())
            
            # Load transformed records to database
            loaded_count = self._load_transformed_records(successful_records)
            
            # 🆕 Step 2: Tag new data with version
            self._tag_data_with_version(version_id)
            
            # 🆕 Step 3: Update version record count
            self._update_version_record_count(version_id)
            
            # Calculate metrics
            total_time = time.time() - start_time
            
            results = {
                'job_name': job_name,
                'version_id': version_id,  
                'version_number': f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}",  
                'records_processed': len(df),
                'records_transformed': len(successful_records),
                'records_loaded': loaded_count,
                'records_failed': len(failed_records),
                'processing_time': total_time,
                'transformation_rate': (len(successful_records) / len(df) * 100) if len(df) > 0 else 0
            }
            
            self.logger.info(f"✅ Transformation completed with versioning: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise

    def _load_transformed_records(self, records: List[Dict[str, Any]]) -> int:
        """Load transformed records to the database"""
        if not records:
            return 0
        
        loaded_count = 0
        
        try:
            with get_db_session() as session:
                for record in records:
                    try:
                        # Create FactSales record
                        fact_record = FactSales(
                            customer_key=record.get('customer_key'),
                            product_key=record.get('product_key'),
                            date_key=record.get('date_key'),
                            invoice_no=record.get('invoice_no'),
                            stock_code=record.get('stock_code', ''),
                            quantity=record.get('quantity', 0),
                            unit_price=record.get('unit_price', 0),
                            line_total=record.get('line_total', 0),
                            transaction_datetime=record.get('transaction_datetime'),
                            batch_id=record.get('batch_id'),
                            data_source=record.get('data_source', 'CSV')
                        )
                        
                        session.add(fact_record)
                        loaded_count += 1
                        
                        # Commit in batches of 1000 for performance
                        if loaded_count % 1000 == 0:
                            session.commit()
                            self.logger.info(f"Loaded {loaded_count} records...")
                    
                    except Exception as e:
                        self.logger.error(f"Failed to load record: {e}", record=record)
                        session.rollback()
                        continue
                
                # Commit remaining records
                session.commit()
                self.logger.info(f"✅ Successfully loaded {loaded_count} records to fact_sales")
                
        except Exception as e:
            self.logger.error(f"Batch loading failed: {e}")
            raise
        
        return loaded_count


    def transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a single record through the complete pipeline"""
        self.total_processed += 1
        
        try:
            # Step 1: Apply data transformations
            transformed_record = self.transformer.transform(record)
            if transformed_record is None:
                self.total_failed += 1
                return None
            
            # Step 2: Apply business rules
            enhanced_record = self.business_rules.apply_retail_business_rules(transformed_record)
            
            self.total_transformed += 1
            return enhanced_record
            
        except Exception as e:
            self.logger.error(f"Record transformation failed: {e}", record=record)
            self.total_failed += 1
            return None

    def get_summary_metrics(self) -> Dict[str, Any]:
        """Get summary of transformation metrics"""
        transformer_metrics = self.transformer.get_metrics()
        cache_stats = self.transformer.cache.get_cache_stats()
        
        return {
            'total_processed': self.total_processed,
            'total_transformed': self.total_transformed,
            'total_failed': self.total_failed,
            'transformation_rate': (self.total_transformed / self.total_processed * 100) if self.total_processed > 0 else 0,
            'transformation_rules_applied': transformer_metrics.transformation_rules_applied,
            'dimension_lookups': transformer_metrics.dimension_lookups,
            'business_rules_applied': self.business_rules.rules_applied,
            'cache_statistics': cache_stats
        }
    
    def log_summary(self) -> None:
        """Log transformation summary"""
        metrics = self.get_summary_metrics()
        self.logger.info("Data transformation pipeline completed", **metrics)


def create_transformation_pipeline() -> DataTransformationPipeline:
    """Create and return a configured data transformation pipeline with versioning support"""
    return DataTransformationPipeline()