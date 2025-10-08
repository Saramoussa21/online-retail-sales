"""
Performance Optimization System
"""
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy import text
import time

from ..database.connection import get_db_session
from ..utils.logging_config import ETLLogger

class QueryAnalyzer:
    """Simple query performance analyzer"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query performance"""
        with get_db_session() as session:
            try:
                # Get execution plan
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS) {query}"
                start_time = time.time()
                
                result = session.execute(text(explain_query))
                plan_lines = [row[0] for row in result]
                
                duration = time.time() - start_time
                
                # Extract key metrics
                execution_time = 0
                planning_time = 0
                
                for line in plan_lines:
                    if 'Execution Time:' in line:
                        execution_time = float(line.split(':')[1].strip().replace(' ms', ''))
                    elif 'Planning Time:' in line:
                        planning_time = float(line.split(':')[1].strip().replace(' ms', ''))
                
                return {
                    'query': query,
                    'execution_time_ms': execution_time,
                    'planning_time_ms': planning_time,
                    'total_time_ms': execution_time + planning_time,
                    'execution_plan': plan_lines,
                    'analyzed_at': datetime.utcnow().isoformat()
                }
                
            except Exception as e:
                return {'error': str(e), 'query': query}

    def get_table_stats(self) -> List[Dict]:
        """Get table statistics"""
        with get_db_session() as session:
            query = """
                SELECT 
                    schemaname,
                    relname as tablename,  
                    n_live_tup as estimated_rows,
                    n_dead_tup as dead_rows,
                    seq_scan as sequential_scans,
                    seq_tup_read as sequential_reads,
                    idx_scan as index_scans,
                    idx_tup_fetch as index_reads,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    last_vacuum,
                    last_analyze
                FROM pg_stat_user_tables 
                WHERE schemaname = 'retail_dw'
                ORDER BY n_live_tup DESC
            """
            result = session.execute(text(query))
            
            # Convert rows to dictionaries
            table_stats = []
            for row in result:
                table_stats.append({
                    'schemaname': row.schemaname,
                    'tablename': row.tablename,
                    'estimated_rows': row.estimated_rows or 0,
                    'dead_rows': row.dead_rows or 0,
                    'sequential_scans': row.sequential_scans or 0,
                    'sequential_reads': row.sequential_reads or 0,
                    'index_scans': row.index_scans or 0,
                    'index_reads': row.index_reads or 0,
                    'inserts': row.inserts or 0,
                    'updates': row.updates or 0,
                    'deletes': row.deletes or 0,
                    'last_vacuum': row.last_vacuum,
                    'last_analyze': row.last_analyze
                })
            return table_stats

        
class IndexAnalyzer:
    """Simple index analyzer"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)


    def get_index_usage(self) -> List[Dict]:
        """Get index usage statistics"""
        with get_db_session() as session:
            query = """
                SELECT
                    schemaname,
                    relname as tablename,  
                    indexrelname as indexname,
                    idx_scan as scans,
                    idx_tup_read as tuples_read,
                    idx_tup_fetch as tuples_fetched,
                    pg_size_pretty(pg_relation_size(indexrelname::regclass)) as size
                FROM pg_stat_user_indexes
                WHERE schemaname = 'retail_dw'
                ORDER BY idx_scan DESC
            """
            result = session.execute(text(query))
            
            # Convert rows to dictionaries
            index_usage = []
            for row in result:
                index_usage.append({
                    'schemaname': row.schemaname,
                    'tablename': row.tablename,
                    'indexname': row.indexname,
                    'scans': row.scans or 0,
                    'tuples_read': row.tuples_read or 0,
                    'tuples_fetched': row.tuples_fetched or 0,
                    'size': row.size
                })
            return index_usage


    
    def find_unused_indexes(self) -> List[Dict]:
        """Find potentially unused indexes"""
        with get_db_session() as session:
            query = """
                SELECT
                    schemaname,
                    relname as tablename,  
                    indexrelname as indexname,
                    idx_scan as scans,
                    pg_size_pretty(pg_relation_size(indexrelname::regclass)) as wasted_size
                FROM pg_stat_user_indexes
                WHERE schemaname = 'retail_dw'
                AND idx_scan < 10
                AND indexrelname NOT LIKE '%_pkey'
                ORDER BY pg_relation_size(indexrelname::regclass) DESC
            """
            result = session.execute(text(query))
            
            # Convert rows to dictionaries
            unused_indexes = []
            for row in result:
                unused_indexes.append({
                    'schemaname': row.schemaname,
                    'tablename': row.tablename,
                    'indexname': row.indexname,
                    'scans': row.scans or 0,
                    'wasted_size': row.wasted_size
                })
            return unused_indexes

class PerformanceMonitor:
    """Simple performance monitoring"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get basic database statistics"""
        with get_db_session() as session:
            # Database size
            db_size = session.execute(text("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """)).scalar()
            
            # Active connections
            connections = session.execute(text("""
                SELECT count(*) as active_connections
                FROM pg_stat_activity 
                WHERE state = 'active'
            """)).scalar()
            
            # Schema size
            schema_size = session.execute(text("""
                SELECT pg_size_pretty(
                    sum(pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(tablename)))::bigint
                ) as schema_size
                FROM pg_tables 
                WHERE schemaname = 'retail_dw'
            """)).scalar()
            
            return {
                'database_size': db_size,
                'schema_size': schema_size,
                'active_connections': connections,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def benchmark_common_queries(self) -> Dict[str, Any]:
        """Benchmark common queries"""
        queries = {
            'count_sales': "SELECT COUNT(*) FROM retail_dw.fact_sales",
            'recent_sales': "SELECT * FROM retail_dw.fact_sales ORDER BY created_at DESC LIMIT 100",
            'customer_count': "SELECT COUNT(DISTINCT customer_key) FROM retail_dw.fact_sales",
            'product_count': "SELECT COUNT(DISTINCT product_key) FROM retail_dw.fact_sales"
        }
        
        results = {}
        analyzer = QueryAnalyzer()
        
        for name, query in queries.items():
            try:
                start_time = time.time()
                
                with get_db_session() as session:
                    result = session.execute(text(query))
                    rows = result.fetchall()
                
                duration = time.time() - start_time
                
                results[name] = {
                    'duration_ms': duration * 1000,
                    'rows_returned': len(rows)
                }
                
            except Exception as e:
                results[name] = {'error': str(e)}
        
        return results

class PerformanceOptimizer:
    """Main performance optimization system"""
    
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
        self.query_analyzer = QueryAnalyzer()
        self.index_analyzer = IndexAnalyzer()
        self.monitor = PerformanceMonitor()
        # Import cache here to avoid circular imports
        from .cache import query_cache, frequent_data_cache
        self.query_cache = query_cache
        self.frequent_data_cache = frequent_data_cache

    def optimize_query_with_cache(self, query: str, params: Dict = None) -> Dict[str, Any]:
        """Execute query with performance analysis and caching"""
        self.logger.info("Analyzing query with caching")
        
        start_time = time.time()
        
        # Try to get from cache first
        cached_result = self.query_cache.get_cached_query(query, params)
        
        if cached_result is not None:
            cache_time = time.time() - start_time
            return {
                'source': 'cache',
                'execution_time_ms': cache_time * 1000,
                'rows_returned': len(cached_result),
                'data': cached_result,
                'cache_hit': True
            }
        
        # Execute and analyze query
        analysis = self.query_analyzer.analyze_query(query)
        
        # Execute actual query for results
        try:
            with get_db_session() as session:
                if params:
                    result = session.execute(text(query), params)
                else:
                    result = session.execute(text(query))
                
                rows = result.fetchall()
                
                # Convert to dict format
                data = []
                for row in rows:
                    row_dict = {}
                    for key in row._fields:
                        value = getattr(row, key)
                        if isinstance(value, datetime):
                            value = value.isoformat()
                        row_dict[key] = value
                    data.append(row_dict)
                
                # Cache the result
                self.query_cache.cache_query_result(query, data, params)
                
                return {
                    'source': 'database',
                    'execution_time_ms': analysis.get('execution_time_ms', 0),
                    'planning_time_ms': analysis.get('planning_time_ms', 0),
                    'total_time_ms': analysis.get('total_time_ms', 0),
                    'rows_returned': len(data),
                    'data': data,
                    'cache_hit': False,
                    'execution_plan': analysis.get('execution_plan', [])
                }
                
        except Exception as e:
            return {'error': str(e), 'cache_hit': False}

    def get_cache_performance(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        cache_stats = self.query_cache.cache.get_stats()
        
        return {
            'cache_statistics': cache_stats,
            'cache_efficiency': {
                'hit_ratio': cache_stats['active_entries'] / max(cache_stats['total_entries'], 1) * 100,
                'memory_usage_mb': cache_stats['cache_size_mb'],
                'recommendations': self._generate_cache_recommendations(cache_stats)
            },
            'timestamp': datetime.now().isoformat()
        }

    def _generate_cache_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """Generate cache optimization recommendations"""
        recommendations = []
        
        hit_ratio = stats['active_entries'] / max(stats['total_entries'], 1) * 100
        
        if hit_ratio < 70:
            recommendations.append("Low cache hit ratio - consider increasing cache TTL")
        
        if stats['cache_size_mb'] > 100:
            recommendations.append("High memory usage - consider reducing cache TTL or clearing cache")
        
        if stats['expired_entries'] > stats['active_entries']:
            recommendations.append("Many expired entries - consider implementing cache cleanup")
        
        return recommendations

    def clear_performance_cache(self) -> Dict[str, Any]:
        """Clear performance cache"""
        stats_before = self.query_cache.cache.get_stats()
        self.query_cache.cache.clear()
        
        return {
            'cleared_entries': stats_before['total_entries'],
            'memory_freed_mb': stats_before['cache_size_mb'],
            'timestamp': datetime.now().isoformat()
        }

    
    def run_performance_audit(self) -> Dict[str, Any]:
        """Run simple performance audit"""
        self.logger.info("Running performance audit")
        
        audit = {
            'audit_timestamp': datetime.utcnow().isoformat(),
            'database_stats': self.monitor.get_database_stats(),
            'table_stats': self.query_analyzer.get_table_stats(),
            'index_usage': self.index_analyzer.get_index_usage(),
            'unused_indexes': self.index_analyzer.find_unused_indexes(),
            'query_benchmarks': self.monitor.benchmark_common_queries(),
            'recommendations': []
        }
        
        # Generate simple recommendations
        recommendations = []
        
        # Check for unused indexes
        unused = audit['unused_indexes']
        if len(unused) > 0:
            recommendations.append(f"Found {len(unused)} potentially unused indexes - consider removing to improve write performance")
        
        # Check for tables with high sequential scans
        for table in audit['table_stats']:
            if table['sequential_scans'] and table['sequential_scans'] > 1000:
                recommendations.append(f"Table {table['tablename']} has {table['sequential_scans']} sequential scans - consider adding indexes")
        
        audit['recommendations'] = recommendations
        
        return audit
    
    def suggest_optimizations(self) -> List[str]:
        """Suggest performance optimizations"""
        suggestions = []
        
        # Basic optimization suggestions
        suggestions.extend([
            "Regularly VACUUM and ANALYZE tables to maintain statistics",
            "Monitor index usage and remove unused indexes",
            "Consider partitioning large tables by date",
            "Use appropriate data types to reduce storage",
            "Add indexes on frequently filtered columns"
        ])
        
        return suggestions

# Global instances
performance_optimizer = PerformanceOptimizer()
query_analyzer = QueryAnalyzer()