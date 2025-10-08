"""
Caching System for Frequently Accessed Data
"""
import json
import time
from typing import Any, Dict, Optional,List
from datetime import datetime, timedelta
import hashlib
from ..utils.logging_config import ETLLogger
from ..database.connection import get_db_session
from sqlalchemy import text

class SimpleCache:
    """Simple in-memory cache with TTL"""
    
    def __init__(self, default_ttl: int = 3600):  # 1 hour default
        self.cache = {}
        self.default_ttl = default_ttl
        self.logger = ETLLogger(self.__class__.__name__)
    
    def _generate_key(self, key: str) -> str:
        """Generate cache key"""
        return hashlib.md5(key.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        cache_key = self._generate_key(key)
        
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if time.time() < entry['expires_at']:
                self.logger.debug(f"Cache HIT for key: {key[:50]}...")
                return entry['value']
            else:
                # Expired, remove from cache
                del self.cache[cache_key]
                self.logger.debug(f"Cache EXPIRED for key: {key[:50]}...")
        
        self.logger.debug(f"Cache MISS for key: {key[:50]}...")
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache"""
        cache_key = self._generate_key(key)
        ttl = ttl or self.default_ttl
        
        self.cache[cache_key] = {
            'value': value,
            'expires_at': time.time() + ttl,
            'created_at': time.time()
        }
        
        self.logger.debug(f"Cache SET for key: {key[:50]}... (TTL: {ttl}s)")
    
    def clear(self) -> None:
        """Clear all cache"""
        cache_size = len(self.cache)
        self.cache.clear()
        self.logger.info(f"Cache cleared - {cache_size} entries removed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self.cache)
        expired_entries = 0
        current_time = time.time()
        
        for entry in self.cache.values():
            if current_time >= entry['expires_at']:
                expired_entries += 1
        
        return {
            'total_entries': total_entries,
            'active_entries': total_entries - expired_entries,
            'expired_entries': expired_entries,
            'cache_size_mb': len(str(self.cache)) / (1024 * 1024)
        }

class QueryCache:
    """Cache for database query results"""
    
    def __init__(self, cache_ttl: int = 1800):  # 30 minutes default
        self.cache = SimpleCache(cache_ttl)
        self.logger = ETLLogger(self.__class__.__name__)
    
    def get_cached_query(self, query: str, params: Dict = None) -> Optional[Any]:
        """Get cached query result"""
        cache_key = f"query:{query}:params:{json.dumps(params or {}, sort_keys=True)}"
        return self.cache.get(cache_key)
    
    def cache_query_result(self, query: str, result: Any, params: Dict = None, ttl: int = None) -> None:
        """Cache query result"""
        cache_key = f"query:{query}:params:{json.dumps(params or {}, sort_keys=True)}"
        self.cache.set(cache_key, result, ttl)
    
    def execute_cached_query(self, query: str, params: Dict = None, ttl: int = None) -> Any:
        """Execute query with caching"""
        # Check cache first
        cached_result = self.get_cached_query(query, params)
        if cached_result is not None:
            return cached_result
        
        # Execute query and cache result
        try:
            with get_db_session() as session:
                if params:
                    result = session.execute(text(query), params).fetchall()
                else:
                    result = session.execute(text(query)).fetchall()
                
                # Convert to serializable format
                serializable_result = []
                for row in result:
                    row_dict = {}
                    for key in row._fields:  # SQLAlchemy Row object
                        value = getattr(row, key)
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        row_dict[key] = value
                    serializable_result.append(row_dict)
                
                # Cache the result
                self.cache_query_result(query, serializable_result, params, ttl)
                
                return serializable_result
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return []

class FrequentDataCache:
    """Cache for frequently accessed business data"""
    
    def __init__(self):
        self.query_cache = QueryCache(cache_ttl=3600)  # 1 hour for business data
        self.logger = ETLLogger(self.__class__.__name__)
    
    def get_sales_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get cached sales summary"""
        query = """
            SELECT 
                COUNT(*) as total_transactions,
                SUM(line_total) as total_revenue,
                AVG(line_total) as avg_transaction_value,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(DISTINCT product_key) as unique_products
            FROM retail_dw.fact_sales 
            WHERE transaction_datetime >= NOW() - INTERVAL '%s days'
        """ % days
        
        results = self.query_cache.execute_cached_query(query, ttl=1800)  # 30 min cache
        
        if results and len(results) > 0:
            row = results[0]
            return {
                'total_transactions': row.get('total_transactions', 0),
                'total_revenue': float(row.get('total_revenue', 0) or 0),
                'avg_transaction_value': float(row.get('avg_transaction_value', 0) or 0),
                'unique_customers': row.get('unique_customers', 0),
                'unique_products': row.get('unique_products', 0),
                'period_days': days,
                'cached_at': datetime.now().isoformat()
            }
        
        return {}
    def get_top_products(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get cached top products"""
        query = """
            SELECT 
                p.description as product_name,
                p.stock_code,
                SUM(f.quantity) as total_quantity_sold,
                SUM(f.line_total) as total_revenue,
                COUNT(*) as transaction_count
            FROM retail_dw.fact_sales f
            JOIN retail_dw.dim_product p ON f.product_key = p.product_key
            WHERE f.transaction_datetime >= NOW() - INTERVAL '365 days'
            GROUP BY p.product_key, p.description, p.stock_code
            ORDER BY total_revenue DESC
            LIMIT %s
        """ % limit
        
        return self.query_cache.execute_cached_query(query, ttl=3600)

    def get_customer_stats(self) -> Dict[str, Any]:
        """Get cached customer statistics"""
        query = """
            SELECT 
                COUNT(DISTINCT f.customer_key) as total_customers,
                COUNT(DISTINCT CASE WHEN f.transaction_datetime >= NOW() - INTERVAL '365 days' 
                    THEN f.customer_key END) as active_customers_365d,
                COUNT(DISTINCT c.country) as countries_served
            FROM retail_dw.fact_sales f
            JOIN retail_dw.dim_customer c ON f.customer_key = c.customer_key
        """
        
        results = self.query_cache.execute_cached_query(query, ttl=3600)
        
        if results and len(results) > 0:
            row = results[0]
            return {
                'total_customers': row.get('total_customers', 0),
                'active_customers_365d': row.get('active_customers_365d', 0),
                'countries_served': row.get('countries_served', 0),
                'cached_at': datetime.now().isoformat()
            }
        
        return {}

    def get_sales_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get cached sales summary"""
        query = """
            SELECT 
                COUNT(*) as total_transactions,
                SUM(line_total) as total_revenue,
                AVG(line_total) as avg_transaction_value,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(DISTINCT product_key) as unique_products
            FROM retail_dw.fact_sales 
            WHERE transaction_datetime >= NOW() - INTERVAL '%s days'
        """ % max(days, 365) 
        
        results = self.query_cache.execute_cached_query(query, ttl=1800)
        
        if results and len(results) > 0:
            row = results[0]
            return {
                'total_transactions': row.get('total_transactions', 0),
                'total_revenue': float(row.get('total_revenue', 0) or 0),
                'avg_transaction_value': float(row.get('avg_transaction_value', 0) or 0),
                'unique_customers': row.get('unique_customers', 0),
                'unique_products': row.get('unique_products', 0),
                'period_days': days,
                'cached_at': datetime.now().isoformat()
            }
        
        return {}

# Global instances
query_cache = QueryCache()
frequent_data_cache = FrequentDataCache()