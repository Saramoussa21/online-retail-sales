"""
Caching System for Frequently Accessed Data

- QueryCache: persistent shelve-backed cache with TTL support and execute_cached_query()
- FrequentDataCache: uses QueryCache.execute_cached_query(...) for SQL result caching
"""
from typing import Any, Dict, Optional, List
from datetime import datetime
import time
import hashlib
import os
from threading import Lock
import contextlib
import shelve
import pickle

from ..utils.logging_config import ETLLogger
from ..database.connection import get_db_session
from sqlalchemy import text

logger = ETLLogger("performance.cache")

_CACHE_FILENAME = os.path.join(os.path.dirname(__file__), "query_cache.db")
_LOCK = Lock()


class QueryCache:
    """
    Shelve-backed persistent cache with TTL.
    - get(key), set(key, value, ttl=None), clear_all(), stats()
    - execute_cached_query(sql, ttl=None) executes SQL via DB and caches the result (list[dict])
    """
    def __init__(self, filename: str = _CACHE_FILENAME, default_ttl: int = 3600):
        self._filename = filename
        self.default_ttl = default_ttl
        os.makedirs(os.path.dirname(self._filename), exist_ok=True)

    @contextlib.contextmanager
    def _open(self, flag="c"):
        with _LOCK:
            db = shelve.open(self._filename, flag)
            try:
                yield db
            finally:
                db.close()

    def _now(self) -> float:
        return time.time()

    def get(self, key: str) -> Optional[Any]:
        try:
            with self._open("r") as db:
                if key in db:
                    try:
                        wrapper = pickle.loads(db[key])
                        expires_at = wrapper.get("expires_at")
                        if expires_at is None or self._now() < expires_at:
                            return wrapper.get("value")
                        # expired
                        try:
                            with self._open("w") as dbw:
                                if key in dbw:
                                    del dbw[key]
                        except Exception:
                            pass
                    except Exception:
                        # corrupted entry: remove it
                        try:
                            with self._open("w") as dbw:
                                if key in dbw:
                                    del dbw[key]
                        except Exception:
                            pass
        except Exception:
            # fallback to open/create
            try:
                with self._open("c") as db:
                    if key in db:
                        wrapper = pickle.loads(db[key])
                        expires_at = wrapper.get("expires_at")
                        if expires_at is None or self._now() < expires_at:
                            return wrapper.get("value")
            except Exception:
                return None
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl if ttl is not None else self.default_ttl
        wrapper = {"value": value, "expires_at": (self._now() + ttl) if ttl > 0 else None}
        try:
            with self._open("c") as db:
                db[key] = pickle.dumps(wrapper)
        except Exception:
            # best-effort: ignore cache failures
            logger.debug("Failed to write cache entry (ignored)")

    def clear_all(self) -> None:
        try:
            # opening with 'n' recreates the db (empties)
            with self._open("n"):
                pass
        except Exception:
            # fallback: remove files
            try:
                for p in [self._filename, self._filename + ".db", self._filename + ".dat", self._filename + ".bak"]:
                    if os.path.exists(p):
                        try:
                            os.remove(p)
                        except Exception:
                            pass
            except Exception:
                pass

    def stats(self) -> dict:
        try:
            with self._open("r") as db:
                return {"size": len(db)}
        except Exception:
            return {"size": 0}

    def _sql_key(self, sql: str) -> str:
        return hashlib.md5(sql.encode("utf-8")).hexdigest()

    def execute_cached_query(self, sql: str, ttl: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL and cache results (list of dict rows).
        - sql: full query string (including any params inlined or formatted)
        - ttl: seconds to cache (None -> default_ttl)
        """
        key = f"sql:{self._sql_key(sql)}"
        cached = self.get(key)
        if cached is not None:
            return cached  # expected as list[dict]

        # execute query
        try:
            with get_db_session() as session:
                result = session.execute(text(sql))
                rows = [dict(r) for r in result.mappings().all()]
        except Exception as e:
            logger.debug(f"Query execution failed: {e}")
            return []

        # cache the result
        try:
            self.set(key, rows, ttl=ttl)
        except Exception:
            pass

        return rows


# lightweight in-memory helper (kept for small, ephemeral caches)
class SimpleCache:
    def __init__(self, default_ttl: int = 3600):
        self._store = {}
        self.default_ttl = default_ttl
        self.logger = ETLLogger(self.__class__.__name__)

    def _now(self):
        return time.time()

    def get(self, key: str):
        ent = self._store.get(key)
        if not ent:
            return None
        if ent["expires_at"] is None or self._now() < ent["expires_at"]:
            return ent["value"]
        del self._store[key]
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        ttl = ttl if ttl is not None else self.default_ttl
        self._store[key] = {"value": value, "expires_at": (self._now() + ttl) if ttl > 0 else None}

    def clear_all(self):
        self._store.clear()


class FrequentDataCache:
    """Cache for frequently accessed business data using QueryCache.execute_cached_query"""
    def __init__(self, query_cache: Optional[QueryCache] = None):
        # use the shared persistent QueryCache by default
        self.query_cache = query_cache or QueryCache()
        self.logger = ETLLogger(self.__class__.__name__)

    def get_sales_summary(self, days: int = 30, ttl: Optional[int] = 1800) -> Dict[str, Any]:
        query = f"""
            SELECT 
                COUNT(*) as total_transactions,
                SUM(line_total) as total_revenue,
                AVG(line_total) as avg_transaction_value,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(DISTINCT product_key) as unique_products
            FROM retail_dw.fact_sales 
            WHERE transaction_datetime >= NOW() - INTERVAL '{int(days)} days'
        """
        results = self.query_cache.execute_cached_query(query, ttl=ttl)
        if results and len(results) > 0:
            row = results[0]
            return {
                'total_transactions': int(row.get('total_transactions') or 0),
                'total_revenue': float(row.get('total_revenue') or 0.0),
                'avg_transaction_value': float(row.get('avg_transaction_value') or 0.0),
                'unique_customers': int(row.get('unique_customers') or 0),
                'unique_products': int(row.get('unique_products') or 0),
                'period_days': days,
                'cached_at': datetime.utcnow().isoformat()
            }
        return {}

    def get_top_products(self, limit: int = 10, ttl: Optional[int] = 3600) -> List[Dict[str, Any]]:
        query = f"""
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
            LIMIT {int(limit)}
        """
        return self.query_cache.execute_cached_query(query, ttl=ttl)

    def get_customer_stats(self, ttl: Optional[int] = 3600) -> Dict[str, Any]:
        query = """
            SELECT 
                COUNT(DISTINCT f.customer_key) as total_customers,
                COUNT(DISTINCT CASE WHEN f.transaction_datetime >= NOW() - INTERVAL '365 days' 
                    THEN f.customer_key END) as active_customers_365d,
                COUNT(DISTINCT c.country) as countries_served
            FROM retail_dw.fact_sales f
            JOIN retail_dw.dim_customer c ON f.customer_key = c.customer_key
        """
        results = self.query_cache.execute_cached_query(query, ttl=ttl)
        if results and len(results) > 0:
            row = results[0]
            return {
                'total_customers': int(row.get('total_customers') or 0),
                'active_customers_365d': int(row.get('active_customers_365d') or 0),
                'countries_served': int(row.get('countries_served') or 0),
                'cached_at': datetime.utcnow().isoformat()
            }
        return {}


# Global instances used by application
query_cache = QueryCache()
frequent_data_cache = FrequentDataCache(query_cache=query_cache)