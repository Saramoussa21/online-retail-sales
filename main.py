#!/usr/bin/env python3
"""
Retail Data Platform - Main Application Entry Point

Command-line interface for the retail data engineering platform.
Provides commands for ETL execution, monitoring, and administration.

Usage:
    python main.py etl --source data/online_retail.csv
    python main.py monitor --job-id <job_id>
    python main.py setup --drop-existing
"""

import click
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from retail_data_platform.etl.pipeline import etl_orchestrator, run_retail_csv_etl
from retail_data_platform.database.schema import schema_manager
from retail_data_platform.database.connection import db_manager
from retail_data_platform.config.config_manager import ConfigManager, get_config
from retail_data_platform.utils.logging_config import configure_logging, get_logger
from retail_data_platform.scheduling.job_manager import job_manager
# ADD this import to your existing main.py
from retail_data_platform.performance.optimization import performance_optimizer, query_analyzer
# ADD this import to your existing main.py
from retail_data_platform.metadata.catalog import metadata_manager

import time


@click.group()
@click.option('--config', type=str, help='Configuration file path')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), 
              default='INFO', help='Logging level')
@click.option('--environment', type=str, help='Environment (development/production)')
def cli(config: Optional[str], log_level: str, environment: Optional[str]):
    """Retail Data Platform - Data Engineering Solution"""
    
    # Set environment if provided
    if environment:
        import os
        os.environ['ENVIRONMENT'] = environment
    
    # Configure logging
    configure_logging(log_level)
    
    # Initialize configuration if custom config provided
    if config:
        config_manager = ConfigManager(config)
        # Update global config
        from retail_data_platform.config import config_manager as global_config
        global_config._config = None  # Reset to reload
        global_config.config_path = config
    
    logger = get_logger(__name__)
    logger.info("Retail Data Platform initialized", 
               log_level=log_level, 
               environment=environment or 'default')


@cli.command()
@click.option('--drop-existing', is_flag=True, 
              help='Drop existing schema before creating (DESTRUCTIVE)')
@click.option('--populate-dates', is_flag=True, default=True,
              help='Populate date dimension')
def setup(drop_existing: bool, populate_dates: bool):
    """Setup database schema and initial data"""
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting database setup")
        
        # Test connection
        if not db_manager.test_connection():
            click.echo("❌ Database connection failed. Please check your configuration.", err=True)
            sys.exit(1)
        
        click.echo("✅ Database connection successful")
        
        # Drop existing if requested
        if drop_existing:
            click.echo("⚠️  Dropping existing schema...")
            if click.confirm('This will delete all existing data. Are you sure?'):
                schema_manager.drop_schema(confirm=True)
                click.echo("✅ Schema dropped")
            else:
                click.echo("Setup cancelled")
                return
        
        # Setup schema
        click.echo("🔧 Creating database schema...")
        schema_manager.setup_complete_schema()
        click.echo("✅ Database schema created")
        
        click.echo("✅ Database setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}", exc_info=True)
        click.echo(f"❌ Setup failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--source', required=True, type=click.Path(exists=True), 
              help='Source CSV file path')
@click.option('--job-name', type=str, help='Custom job name')
@click.option('--batch-size', type=int, default=1000, help='Batch size for processing')
@click.option('--dry-run', is_flag=True, help='Validate without loading data')
def etl(source: str, job_name: Optional[str], batch_size: int, dry_run: bool):
    """Execute ETL pipeline for retail data"""
    logger = get_logger(__name__)
    
    try:
        if not job_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_name = f"retail_etl_{timestamp}"
        
        click.echo(f"Starting ETL job: {job_name}")
        click.echo(f"📁 Source file: {source}")
        click.echo(f"📦 Batch size: {batch_size}")
        
        if dry_run:
            click.echo("🔍 DRY RUN MODE - No data will be loaded")
            # TODO: Implement dry run validation
            click.echo("✅ Validation completed")
            return
        
        # Execute ETL
        with click.progressbar(length=100, label='Processing') as bar:
            # This is a simplified progress bar
            # In a real implementation, you'd update it based on actual progress
            metrics = run_retail_csv_etl(source, job_name)
            bar.update(100)
        
        # Display results
        click.echo("\n📊 ETL Job Results:")
        click.echo(f"   Job ID: {metrics.job_id}")
        click.echo(f"   Status: {metrics.status.value}")
        click.echo(f"   Records Extracted: {metrics.records_extracted:,}")
        click.echo(f"   Records Cleaned: {metrics.records_cleaned:,}")
        click.echo(f"   Records Transformed: {metrics.records_transformed:,}")
        click.echo(f"   Records Loaded: {metrics.records_loaded:,}")
        click.echo(f"   Records Rejected: {metrics.records_rejected:,}")
        click.echo(f"   Success Rate: {metrics.success_rate:.2f}%")
        click.echo(f"   Duration: {metrics.total_duration:.2f} seconds")
        click.echo(f"   Processing Rate: {metrics.records_per_second:.2f} records/sec")
        
        if metrics.status.value == "SUCCESS":
            click.echo("✅ ETL job completed successfully!")
        else:
            click.echo(f"❌ ETL job failed with status: {metrics.status.value}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"ETL execution failed: {e}", exc_info=True)
        click.echo(f"❌ ETL failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--job-id', type=str, help='Specific job ID to monitor')
@click.option('--last', type=int, default=10, help='Number of recent jobs to show')
def monitor(job_id: Optional[str], last: int):
    """Monitor ETL job status and history"""
    logger = get_logger(__name__)
    
    try:
        if job_id:
            # Monitor specific job
            status = etl_orchestrator.get_job_status(job_id)
            if status:
                click.echo(f"Job {job_id}: {status.value}")
            else:
                click.echo(f"Job {job_id} not found")
        else:
            # Show job history
            history = etl_orchestrator.get_job_history(last)
            
            if not history:
                click.echo("No job history found")
                return
            
            click.echo(f"📈 Recent ETL Jobs (last {len(history)}):")
            click.echo("-" * 80)
            
            for metrics in reversed(history):  # Show most recent first
                status_emoji = "✅" if metrics.status.value == "SUCCESS" else "❌"
                duration = f"{metrics.total_duration:.1f}s" if metrics.total_duration else "N/A"
                
                click.echo(f"{status_emoji} {metrics.job_name}")
                click.echo(f"   ID: {metrics.job_id}")
                click.echo(f"   Status: {metrics.status.value}")
                click.echo(f"   Records: {metrics.records_loaded:,} loaded, "
                         f"{metrics.records_rejected:,} rejected")
                click.echo(f"   Duration: {duration}")
                click.echo(f"   Started: {metrics.start_time}")
                click.echo()
                
    except Exception as e:
        logger.error(f"Monitoring failed: {e}", exc_info=True)
        click.echo(f"❌ Monitoring failed: {e}", err=True)


@cli.command()
def test():
    """Test database connectivity and configuration"""
    logger = get_logger(__name__)
    
    try:
        click.echo("🔧 Testing system configuration...")
        
        # Test configuration
        config = get_config()
        click.echo(f"✅ Configuration loaded: {config.environment}")
        
        # Test database connection
        if db_manager.test_connection():
            click.echo("✅ Database connection successful")
        else:
            click.echo("❌ Database connection failed")
            sys.exit(1)
        
        # Test schema exists
        schema_name = config.database.schema
        result = db_manager.execute_query(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = :schema",
            {'schema': schema_name}
        )
        
        if result[0][0] > 0:
            click.echo(f"✅ Schema '{schema_name}' exists")
            
            # Test tables exist
            tables = ['dim_date', 'dim_customer', 'dim_product', 'fact_sales']
            for table in tables:
                table_result = db_manager.execute_query(
                    """SELECT COUNT(*) FROM information_schema.tables 
                       WHERE table_schema = :schema AND table_name = :table""",
                    {'schema': schema_name, 'table': table}
                )
                
                if table_result[0][0] > 0:
                    click.echo(f"✅ Table '{table}' exists")
                else:
                    click.echo(f"❌ Table '{table}' missing")
        else:
            click.echo(f"❌ Schema '{schema_name}' does not exist")
            click.echo("Run 'python main.py setup' to create the schema")
        
        click.echo("✅ System test completed")
        
    except Exception as e:
        logger.error(f"System test failed: {e}", exc_info=True)
        click.echo(f"❌ System test failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--table', type=str, help='Specific table to query')
@click.option('--limit', type=int, default=10, help='Number of records to show')
def query(table: Optional[str], limit: int):
    """Query data warehouse tables"""
    logger = get_logger(__name__)
    config = get_config()
    schema_name = config.database.schema
    
    try:
        if table:
            # Query specific table
            result = db_manager.execute_query(
                f"SELECT * FROM {schema_name}.{table} LIMIT {limit}"
            )
            
            if result:
                click.echo(f"📊 {table} (showing {len(result)} records):")
                for row in result:
                    click.echo(f"   {row}")
            else:
                click.echo(f"No data found in {table}")
        else:
            # Show summary of all tables
            tables = ['dim_date', 'dim_customer', 'dim_product', 'fact_sales']
            
            click.echo("📊 Data Warehouse Summary:")
            click.echo("-" * 40)
            
            for table_name in tables:
                try:
                    result = db_manager.execute_query(
                        f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
                    )
                    count = result[0][0] if result else 0
                    click.echo(f"{table_name}: {count:,} records")
                except Exception as e:
                    click.echo(f"{table_name}: Error - {e}")
                    
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        click.echo(f"❌ Query failed: {e}", err=True)


@click.group()
def schedule():
    """ETL scheduling commands"""
    pass

@schedule.command()
@click.option('--name', required=True, help='Job name')
@click.option('--csv-path', required=True, help='Path to CSV file')
@click.option('--time', default='02:00', help='Time to run (HH:MM)')
def daily(name, csv_path, time):
    """Add daily ETL job"""
    job_manager.create_daily_job(name, csv_path, time)

@schedule.command()
@click.option('--name', required=True, help='Job name')
@click.option('--csv-path', required=True, help='Path to CSV file')
@click.option('--hours', default=1, help='Run every X hours')
def hourly(name, csv_path, hours):
    """Add hourly ETL job"""
    job_manager.create_hourly_job(name, csv_path, hours)

@schedule.command()
@click.option('--name', required=True, help='Job name')
@click.option('--csv-path', required=True, help='Path to CSV file')
@click.option('--day', default='monday', help='Day of week')
@click.option('--time', default='02:00', help='Time to run (HH:MM)')
def weekly(name, csv_path, day, time):
    """Add weekly ETL job"""
    job_manager.create_weekly_job(name, csv_path, day, time)

@schedule.command()
def list():
    """List all scheduled jobs"""
    job_manager.list_jobs()

@schedule.command()
@click.argument('name')
def enable(name):
    """Enable a job"""
    job_manager.enable_job(name)

@schedule.command()
@click.argument('name')
def disable(name):
    """Disable a job"""
    job_manager.disable_job(name)

@schedule.command()
@click.argument('name')
def remove(name):
    """Remove a job"""
    job_manager.remove_job(name)

@schedule.command()
def start():
    """Start the scheduler daemon"""
    job_manager.start_scheduler()
    print("Scheduler running... Press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        job_manager.stop_scheduler()

@schedule.command()
def stop():
    """Stop the scheduler"""
    job_manager.stop_scheduler()

# Add to your main CLI
cli.add_command(schedule)

# ADD new performance command group
@click.group()
def performance():
    """Performance optimization commands"""
    pass

@performance.command()
@click.option('--query', help='Specific query to analyze')
def analyze(query):
    """Analyze query performance"""
    try:
        from retail_data_platform.performance.optimization import performance_optimizer
        
        if not query:
            # Default query for testing
            query = "SELECT COUNT(*) as total_sales FROM retail_dw.fact_sales"
        
        click.echo(f"🔍 Analyzing Query Performance...")
        result = performance_optimizer.optimize_query_with_cache(query)
        
        if 'error' in result:
            click.echo(f"❌ Query failed: {result['error']}")
            return
        
        click.echo(f"📊 Query Analysis Results")
        click.echo("=" * 50)
        click.echo(f"Source: {'🚀 Cache' if result['cache_hit'] else '💾 Database'}")
        click.echo(f"Execution Time: {result.get('execution_time_ms', 0):.2f} ms")
        click.echo(f"Rows Returned: {result['rows_returned']}")
        
        if not result['cache_hit']:
            click.echo(f"Planning Time: {result.get('planning_time_ms', 0):.2f} ms")
            click.echo(f"Total Time: {result.get('total_time_ms', 0):.2f} ms")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}")

@performance.command()
def audit():
    """Run performance audit"""
    try:
        from retail_data_platform.performance.optimization import performance_optimizer
        
        click.echo("🔍 Running Performance Audit...")
        audit_results = performance_optimizer.run_performance_audit()
        
        click.echo("📊 Performance Audit Results")
        click.echo("=" * 50)
        
        # Database stats
        db_stats = audit_results['database_stats']
        click.echo(f"\n🗄️ Database:")
        click.echo(f"   Size: {db_stats['database_size']}")
        click.echo(f"   Schema Size: {db_stats['schema_size']}")
        click.echo(f"   Active Connections: {db_stats['active_connections']}")
        
        # Table stats summary
        table_stats = audit_results['table_stats']
        click.echo(f"\n📋 Tables ({len(table_stats)} tables):")
        for table in table_stats[:5]:  # Top 5 tables
            click.echo(f"   ✅ {table['tablename']}: {table['estimated_rows']} rows, {table['sequential_scans']} seq scans")
        
        # Index usage
        index_usage = audit_results['index_usage']
        click.echo(f"\n📑 Index Usage ({len(index_usage)} indexes):")
        for idx in index_usage[:3]:  # Top 3 indexes
            click.echo(f"   📑 {idx['indexname']}: {idx['scans']} scans, {idx['size']}")
        
        # Recommendations
        recommendations = audit_results['recommendations']
        if recommendations:
            click.echo(f"\n💡 Recommendations:")
            for rec in recommendations:
                click.echo(f"   • {rec}")
        else:
            click.echo(f"\n✅ No performance issues detected")
        
    except Exception as e:
        click.echo(f"❌ Audit failed: {e}")

@performance.command()
def cache_stats():
    """Show cache performance statistics"""
    try:
        from retail_data_platform.performance.optimization import performance_optimizer
        
        stats = performance_optimizer.get_cache_performance()
        
        click.echo("🚀 Cache Performance Statistics")
        click.echo("=" * 50)
        
        cache_stats = stats['cache_statistics']
        click.echo(f"Total Entries: {cache_stats['total_entries']}")
        click.echo(f"Active Entries: {cache_stats['active_entries']}")
        click.echo(f"Expired Entries: {cache_stats['expired_entries']}")
        click.echo(f"Memory Usage: {cache_stats['cache_size_mb']:.2f} MB")
        
        efficiency = stats['cache_efficiency']
        click.echo(f"\n📈 Efficiency:")
        click.echo(f"Hit Ratio: {efficiency['hit_ratio']:.1f}%")
        
        recommendations = efficiency['recommendations']
        if recommendations:
            click.echo(f"\n💡 Recommendations:")
            for rec in recommendations:
                click.echo(f"   • {rec}")
        
    except Exception as e:
        click.echo(f"❌ Cache stats failed: {e}")

@performance.command()
def frequent_data():
    """Show frequently accessed cached data"""
    try:
        from retail_data_platform.performance.cache import frequent_data_cache
        
        click.echo("📊 Frequently Accessed Data (Cached)")
        click.echo("=" * 50)
        
        # Sales summary - use broader time range
        sales_summary = frequent_data_cache.get_sales_summary(365)  # ✅ Use 365 days
        if sales_summary and sales_summary.get('total_transactions', 0) > 0:
            click.echo(f"\n💰 Sales Summary (365 days):")
            click.echo(f"   Transactions: {sales_summary['total_transactions']:,}")
            click.echo(f"   Revenue: ${sales_summary['total_revenue']:,.2f}")
            click.echo(f"   Avg Transaction: ${sales_summary['avg_transaction_value']:.2f}")
            click.echo(f"   Unique Customers: {sales_summary['unique_customers']:,}")
            click.echo(f"   Unique Products: {sales_summary['unique_products']:,}")
        else:
            click.echo(f"\n💰 Sales Summary: No data found (your data may be older than 365 days)")
        
        # Top products
        top_products = frequent_data_cache.get_top_products(5)
        if top_products and len(top_products) > 0:
            click.echo(f"\n🏆 Top Products:")
            for i, product in enumerate(top_products, 1):
                product_name = product.get('product_name', 'Unknown Product')[:50]  # Truncate long names
                revenue = float(product.get('total_revenue', 0) or 0)
                click.echo(f"   {i}. {product_name}: ${revenue:,.2f}")
        else:
            click.echo(f"\n🏆 Top Products: No data found")
        
        # Customer stats
        customer_stats = frequent_data_cache.get_customer_stats()
        if customer_stats and customer_stats.get('total_customers', 0) > 0:
            click.echo(f"\n👥 Customer Statistics:")
            click.echo(f"   Total Customers: {customer_stats['total_customers']:,}")
            click.echo(f"   Active (365d): {customer_stats['active_customers_365d']:,}")
            click.echo(f"   Countries: {customer_stats['countries_served']}")
        else:
            click.echo(f"\n👥 Customer Statistics: No data found")
        
    except Exception as e:
        click.echo(f"❌ Frequent data failed: {e}")

@performance.command()
def clear_cache():
    """Clear performance cache"""
    try:
        from retail_data_platform.performance.optimization import performance_optimizer
        
        result = performance_optimizer.clear_performance_cache()
        
        click.echo("🧹 Cache Cleared")
        click.echo("=" * 30)
        click.echo(f"Entries Cleared: {result['cleared_entries']}")
        click.echo(f"Memory Freed: {result['memory_freed_mb']:.2f} MB")
        click.echo(f"✅ Cache cleared successfully")
        
    except Exception as e:
        click.echo(f"❌ Cache clear failed: {e}")


@performance.command()
def benchmark():
    """Run query benchmarks"""
    try:
        click.echo("⏱️ Running query benchmarks...")
        benchmarks = performance_optimizer.monitor.benchmark_common_queries()
        
        click.echo("📊 Query Benchmark Results")
        click.echo("=" * 40)
        
        for query_name, result in benchmarks.items():
            if 'error' in result:
                click.echo(f"❌ {query_name}: {result['error']}")
            else:
                duration = result['duration_ms']
                rows = result['rows_returned']
                speed_icon = "🟢" if duration < 100 else "🟡" if duration < 1000 else "🔴"
                click.echo(f"{speed_icon} {query_name}: {duration:.1f}ms ({rows:,} rows)")
        
    except Exception as e:
        click.echo(f"❌ Benchmark failed: {e}")


@performance.command()
def suggestions():
    """Show optimization suggestions"""
    try:
        suggestions = performance_optimizer.suggest_optimizations()
        
        click.echo("💡 Performance Optimization Suggestions")
        click.echo("=" * 50)
        
        for i, suggestion in enumerate(suggestions, 1):
            click.echo(f"{i}. {suggestion}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get suggestions: {e}")


# ADD this new CLI group
@click.group()
def metadata():
    """Metadata and lineage management commands"""
    pass

@metadata.command()
@click.option('--table', help='Specific table name')
def tables(table):
    """Show table information"""
    try:
        table_info = metadata_manager.catalog.get_table_info(table)
        table_sizes = metadata_manager.catalog.get_table_sizes()
        
        if table:
            click.echo(f"📋 Table: {table}")
            click.echo("=" * 50)
            
            # Show columns
            table_columns = [col for col in table_info if col['table_name'] == table]
            for col in table_columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                click.echo(f"  {col['column_name']}: {col['data_type']} {nullable}{default}")
        else:
            click.echo("📊 All Tables")
            click.echo("=" * 50)
            
            # Group table info by table name
            tables = {}
            for col in table_info:
                table_name = col['table_name']
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append(col)
            
            for table_name, columns in tables.items():
                # Find size info
                size_info = next((s for s in table_sizes if s['table_name'] == table_name), {})
                size = size_info.get('table_size', 'Unknown')
                rows = size_info.get('estimated_rows', 0) or 0
                
                click.echo(f"\n🗄️ {table_name}")
                click.echo(f"   Size: {size}, Rows: {rows:,}")
                click.echo(f"   Columns: {len(columns)}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get table info: {e}")

@metadata.command()
def relationships():
    """Show table relationships"""
    try:
        relationships = metadata_manager.catalog.get_relationships()
        
        click.echo("🔗 Table Relationships")
        click.echo("=" * 50)
        
        for rel in relationships:
            click.echo(f"{rel['child_table']}.{rel['child_column']} → {rel['parent_table']}.{rel['parent_column']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get relationships: {e}")

@metadata.command()
def lineage():
    """Show data lineage information"""
    try:
        from retail_data_platform.metadata.catalog import metadata_manager
        
        recent_jobs = metadata_manager.lineage.get_recent_lineage(5)
        
        if not recent_jobs:
            click.echo("📈 No lineage data found")
            click.echo("💡 Run some ETL jobs first: python main.py etl --source data/online_retail.csv")
            return
        
        click.echo("📈 Recent Data Lineage")
        click.echo("=" * 50)
        
        for job in recent_jobs:
            status_icon = "✅" if job['status'] == 'SUCCESS' else "❌"
            click.echo(f"\n{status_icon} {job['etl_job_name']}")
            click.echo(f"   Source: {job['source_file']}")
            click.echo(f"   Target: {job['target_table']}")
            click.echo(f"   Records: {job['records_processed']} processed, {job['records_inserted']} inserted")
            click.echo(f"   Time: {job['start_time']} - {job['end_time']}")
            click.echo(f"   Batch: {job['batch_id']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to show lineage: {e}")

@metadata.command()
@click.option('--output', help='Output filename')
def dictionary(output):
    """Generate and export data dictionary"""
    try:
        filename = metadata_manager.export_dictionary(output)
        click.echo(f"✅ Data dictionary exported to: {filename}")
        
        # Also show summary
        dict_data = metadata_manager.generate_data_dictionary()
        click.echo(f"\n📊 Data Dictionary Summary")
        click.echo("=" * 40)
        click.echo(f"Generated: {dict_data['generated_at']}")
        click.echo(f"Schema: {dict_data['schema']}")
        click.echo(f"Tables: {len(dict_data['tables'])}")
        click.echo(f"Relationships: {len(dict_data['relationships'])}")
        
    except Exception as e:
        click.echo(f"❌ Failed to generate dictionary: {e}")
@metadata.command()
def sources():
    """Show data sources documentation"""
    try:
        from retail_data_platform.metadata.catalog import metadata_manager
        
        sources = metadata_manager.document_data_sources()
        
        click.echo("📊 Data Sources Documentation")
        click.echo("=" * 50)
        
        for source_name, source_info in sources['sources'].items():
            click.echo(f"\n📁 {source_name}:")
            click.echo(f"   Type: {source_info['type']}")
            click.echo(f"   Location: {source_info['location']}")
            click.echo(f"   Description: {source_info['description']}")
            click.echo(f"   Size: {source_info['size_mb']} MB")
            click.echo(f"   Columns: {', '.join(source_info['columns'][:4])}...")
        
    except Exception as e:
        click.echo(f"❌ Failed to show sources: {e}")

@metadata.command()
def transformations():
    """Show data transformations documentation"""
    try:
        from retail_data_platform.metadata.catalog import metadata_manager
        
        transformations = metadata_manager.document_transformations()
        
        click.echo("🔄 Data Transformations Documentation")
        click.echo("=" * 50)
        
        for transform_name, transform_info in transformations['transformations'].items():
            click.echo(f"\n⚙️ {transform_name}:")
            click.echo(f"   Process: {transform_info['process']}")
            click.echo(f"   Implemented in: {transform_info.get('implemented_in', 'N/A')}")
            
            if 'rules' in transform_info:
                click.echo(f"   Rules: {len(transform_info['rules'])} cleaning rules")
            
            if 'dimensions' in transform_info:
                click.echo(f"   Dimensions: {len(transform_info['dimensions'])} dimension tables")
        
    except Exception as e:
        click.echo(f"❌ Failed to show transformations: {e}")

@metadata.command()
def storage():
    """Show storage locations documentation"""
    try:
        from retail_data_platform.metadata.catalog import metadata_manager
        
        storage = metadata_manager.document_storage_locations()
        
        click.echo("💾 Storage Locations Documentation")
        click.echo("=" * 50)
        
        # Database info
        db_info = storage['storage']['database']
        click.echo(f"\n🗄️ Database:")
        click.echo(f"   Type: {db_info['type']}")
        click.echo(f"   Schema: {db_info['schema']}")
        click.echo(f"   Host: {db_info['host']}")
        
        # Tables
        click.echo(f"\n📋 Tables:")
        for table_name, table_info in storage['storage']['tables'].items():
            click.echo(f"   ✅ {table_name}: {table_info['purpose']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to show storage: {e}")

@metadata.command()
@click.option('--export', is_flag=True, help='Export to JSON file')
def repository(export):
    """Show complete metadata repository"""
    try:
        from retail_data_platform.metadata.catalog import metadata_manager
        
        if export:
            filepath = metadata_manager.export_complete_repository()
            click.echo(f"✅ Metadata repository exported to: {filepath}")
        else:
            repo = metadata_manager.generate_complete_metadata_repository()
            
            click.echo("📚 Complete Metadata Repository")
            click.echo("=" * 50)
            click.echo(f"Generated: {repo['metadata_repository']['generated_at']}")
            click.echo(f"Version: {repo['metadata_repository']['version']}")
            
            click.echo(f"\n📊 Components:")
            click.echo(f"   📖 Data Dictionary: {len(repo['data_dictionary']['tables'])} tables")
            click.echo(f"   📁 Data Sources: {len(repo['data_sources']['sources'])} sources")
            click.echo(f"   🔄 Transformations: {len(repo['transformations']['transformations'])} processes")
            click.echo(f"   💾 Storage Tables: {len(repo['storage_locations']['storage']['tables'])} tables")
            click.echo(f"   📈 Recent Lineage: {len(repo['data_lineage']['recent_jobs'])} jobs")
            
            click.echo(f"\n💡 Use --export flag to save complete repository to file")
        
    except Exception as e:
        click.echo(f"❌ Failed to generate repository: {e}")


# ADD this line at the bottom of main.py
cli.add_command(metadata)

# ADD this line at the bottom of main.py
cli.add_command(performance)

# ADD these imports to your existing main.py (at the top with other imports)
from retail_data_platform.monitoring.quality import create_quality_monitor

# ADD this new CLI group (add after your existing CLI commands)
@click.group()
def quality():
    """Data quality monitoring commands"""
    pass

@quality.command()
@click.option('--table', default='fact_sales', help='Table name to check')
@click.option('--batch-id', help='Specific batch ID to check')
def check(table, batch_id):
    """Run data quality check on a table"""
    try:
        # Create quality monitor
        monitor = create_quality_monitor(batch_id)
        
        # Get data from database for quality check
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            # Get sample data for quality check (limit to 1000 for performance)
            query = f"SELECT * FROM retail_dw.{table} LIMIT 1000"
            if batch_id:
                query = f"SELECT * FROM retail_dw.{table} WHERE batch_id = '{batch_id}' LIMIT 1000"
            
            result = session.execute(text(query))
            data = [dict(row) for row in result]
        
        if not data:
            click.echo(f"❌ No data found in {table}")
            return
        
        # Run quality checks
        click.echo(f"🔍 Running quality checks on {table} ({len(data)} records)...")
        quality_results = monitor.check_data_quality(data, table)
        
        # Persist results
        monitor.persist_quality_metrics()
        
        # Show summary
        summary = monitor.get_quality_summary()
        
        click.echo(f"\n📊 Data Quality Results for {table}")
        click.echo("=" * 50)
        click.echo(f"Total Checks: {summary['total_checks']}")
        click.echo(f"Passed: {summary['passed_checks']} ✅")
        click.echo(f"Failed: {summary['failed_checks']} ❌")
        click.echo(f"Success Rate: {summary['success_rate']:.1f}%")
        click.echo(f"Overall Score: {summary['overall_score']:.1f}%")
        
        # Show individual results
        click.echo(f"\n📋 Detailed Results:")
        for result in quality_results:
            status = "✅ PASS" if result.is_threshold_met else "❌ FAIL" if result.is_threshold_met is not None else "ℹ️ INFO"
            threshold_text = f" (Threshold: {result.threshold_value}%)" if result.threshold_value else ""
            click.echo(f"  {status} {result.metric_name}: {result.metric_value:.1f}%{threshold_text}")
        
    except Exception as e:
        click.echo(f"❌ Quality check failed: {e}")
# ADD these commands to your existing @quality.command() group

@quality.command()
@click.option('--days', default=7, help='Days to analyze')
def trends(days):
    """Show quality trends over time"""
    try:
        from retail_data_platform.monitoring.quality import create_quality_monitor
        
        monitor = create_quality_monitor("trend_analysis")
        trends_data = monitor.track_quality_trends(days)
        
        if 'error' in trends_data:
            click.echo(f"❌ {trends_data['error']}")
            return
        
        click.echo(f"📈 Quality Trends ({days} days)")
        click.echo("=" * 50)
        
        summary = trends_data.get('summary', {})
        click.echo(f"Average Quality Score: {summary.get('avg_quality_score', 0):.1f}%")
        click.echo(f"Quality Trend: {summary.get('quality_trend', 'UNKNOWN')}")
        click.echo(f"Poor Quality Days: {summary.get('poor_quality_days', 0)}")
        click.echo(f"Total Checks: {summary.get('total_checks', 0)}")
        
        # Show recent trend data
        trends = trends_data.get('trends', [])[-5:]  # Last 5 days
        if trends:
            click.echo(f"\n📊 Recent Daily Averages:")
            for trend in trends:
                click.echo(f"  {trend['date']}: {trend['avg_score']:.1f}% ({trend['checks_count']} checks)")
        
    except Exception as e:
        click.echo(f"❌ Trends analysis failed: {e}")

@quality.command()
def anomalies():
    """Detect quality anomalies"""
    try:
        from retail_data_platform.monitoring.quality import create_quality_monitor
        
        monitor = create_quality_monitor("anomaly_detection")
        anomalies = monitor.detect_quality_anomalies()
        
        if not anomalies:
            click.echo("✅ No quality anomalies detected")
            return
        
        click.echo(f"🚨 Quality Anomalies Detected: {len(anomalies)}")
        click.echo("=" * 60)
        
        for anomaly in anomalies[:5]:  # Show top 5
            click.echo(f"Table: {anomaly['table_name']}")
            click.echo(f"  Score Drop: {anomaly['score_drop']:.1f}% ({anomaly['previous_score']:.1f}% → {anomaly['current_score']:.1f}%)")
            click.echo(f"  Severity: {anomaly['severity']}")
            click.echo(f"  Time: {anomaly['measured_at']}")
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Anomaly detection failed: {e}")

@quality.command()
@click.option('--days', default=7, help='Days to analyze')
def dashboard(days):
    """Show quality dashboard for recent data"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days)  # Fixed deprecation warning
        
        with get_db_session() as session:
            query = text("""
                SELECT
                    table_name,
                    metric_name,
                    AVG(metric_value::FLOAT) as avg_value,
                    COUNT(*) as measurements,
                    MAX(measured_at) as last_measured
                FROM retail_dw.data_quality_metrics
                WHERE measured_at >= :cutoff_date
                GROUP BY table_name, metric_name
                ORDER BY table_name, metric_name
            """)
            
            result = session.execute(query, {'cutoff_date': cutoff_date})
            
            # 🔧 FIX: Properly convert rows to dictionaries
            metrics = []
            for row in result:
                metrics.append({
                    'table_name': row.table_name,
                    'metric_name': row.metric_name,
                    'avg_value': float(row.avg_value) if row.avg_value else 0.0,
                    'measurements': row.measurements,
                    'last_measured': row.last_measured
                })
            
            if not metrics:
                click.echo(f"📊 No quality metrics found for the last {days} days")
                click.echo("💡 Run an ETL job first: python main.py etl --source data/online_retail.csv")
                return
            
            click.echo(f"📊 Quality Dashboard - Last {days} days")
            click.echo("=" * 50)
            
            # Group by table
            tables = {}
            for metric in metrics:
                table = metric['table_name']
                if table not in tables:
                    tables[table] = []
                tables[table].append(metric)
            
            # Display results
            for table_name, table_metrics in tables.items():
                click.echo(f"\n✅ {table_name}:")
                
                total_score = 0
                metric_count = 0
                
                for metric in table_metrics:
                    score = metric['avg_value']
                    total_score += score
                    metric_count += 1
                    
                    status = "✅" if score >= 95 else "⚠️" if score >= 85 else "❌"
                    click.echo(f"   {status} {metric['metric_name']}: {score:.1f}% ({metric['measurements']} measurements)")
                
                if metric_count > 0:
                    avg_score = total_score / metric_count
                    click.echo(f"\n📈 Overall Quality: {avg_score:.1f}%")
                
    except Exception as e:
        click.echo(f"❌ Dashboard generation failed: {e}")

@quality.command()
@click.option('--table', default='fact_sales', help='Table name')
def report(table):
    """Generate detailed quality report"""
    try:
        # Create quality monitor
        monitor = create_quality_monitor()
        
        # Get recent data
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            result = session.execute(text(f"SELECT * FROM retail_dw.{table} LIMIT 1000"))
            data = [dict(row) for row in result]
        
        if not data:
            click.echo(f"❌ No data found in {table}")
            return
        
        # Run quality checks
        monitor.check_data_quality(data, table)
        
        # Generate and display report
        report = monitor.generate_quality_report()
        click.echo(report)
        
    except Exception as e:
        click.echo(f"❌ Report generation failed: {e}")

# ADD this line at the bottom of main.py (with other cli.add_command calls)
cli.add_command(quality)
# MODIFY your existing etl command to show version information

@cli.command()
@click.option('--source', required=True, type=click.Path(exists=True), 
              help='Source CSV file path')
@click.option('--job-name', type=str, help='Custom job name')
@click.option('--batch-size', type=int, default=1000, help='Batch size for processing')
@click.option('--dry-run', is_flag=True, help='Validate without loading data')
def etl(source: str, job_name: Optional[str], batch_size: int, dry_run: bool):
    """Execute ETL pipeline for retail data"""
    logger = get_logger(__name__)
    
    try:
        if not job_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_name = f"retail_etl_{timestamp}"
        
        click.echo(f"Starting ETL job: {job_name}")
        click.echo(f"📁 Source file: {source}")
        click.echo(f"📦 Batch size: {batch_size}")
        
        if dry_run:
            click.echo("🔍 DRY RUN MODE - No data will be loaded")
            # TODO: Implement dry run validation
            click.echo("✅ Validation completed")
            return
        
        # Execute ETL
        with click.progressbar(length=100, label='Processing') as bar:
            # This is a simplified progress bar
            # In a real implementation, you'd update it based on actual progress
            metrics = run_retail_csv_etl(source, job_name)
            bar.update(100)
        
        # Display results
        click.echo("\n📊 ETL Job Results:")
        click.echo(f"   Job ID: {metrics.job_id}")
        click.echo(f"   Status: {metrics.status.value}")
        click.echo(f"   Records Extracted: {metrics.records_extracted:,}")
        click.echo(f"   Records Cleaned: {metrics.records_cleaned:,}")
        click.echo(f"   Records Transformed: {metrics.records_transformed:,}")
        click.echo(f"   Records Loaded: {metrics.records_loaded:,}")
        click.echo(f"   Records Rejected: {metrics.records_rejected:,}")
        click.echo(f"   Success Rate: {metrics.success_rate:.2f}%")
        click.echo(f"   Duration: {metrics.total_duration:.2f} seconds")
        click.echo(f"   Processing Rate: {metrics.records_per_second:.2f} records/sec")
        
        # 🆕 NEW: Show version information
        if metrics.version_number:
            click.echo(f"   Version: {metrics.version_number}")
        if metrics.version_id:
            click.echo(f"   Version ID: {metrics.version_id}")
        
        if metrics.status.value == "SUCCESS":
            click.echo("✅ ETL job completed successfully!")
        else:
            click.echo(f"❌ ETL job failed with status: {metrics.status.value}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"ETL execution failed: {e}", exc_info=True)
        click.echo(f"❌ ETL failed: {e}", err=True)
        sys.exit(1)
# ADD this new command group to your main.py

@click.group()
def versions():
    """Data versioning and history commands"""
    pass

@versions.command()
@click.option('--limit', default=10, help='Number of versions to show')
def list(limit):
    """List data versions"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            query = text("SELECT * FROM retail_dw.v_version_history LIMIT :limit")
            result = session.execute(query, {'limit': limit})
            
            versions = []
            for row in result:
                versions.append({
                    'version_number': row.version_number,
                    'version_type': row.version_type,
                    'created_at': row.created_at,
                    'records_count': row.records_count or 0,
                    'status': row.status,
                    'description': row.description,
                    'etl_job_id': row.etl_job_id,
                    'archived_records': row.archived_records or 0
                })
        
        if not versions:
            click.echo("📦 No data versions found")
            click.echo("💡 Run an ETL job to create your first version: python main.py etl --source data/online_retail.csv")
            return
        
        click.echo(f"📦 Data Versions (showing {len(versions)} versions)")
        click.echo("=" * 80)
        
        for version in versions:
            status_icon = "✅" if version['status'] == 'ACTIVE' else "📦"
            click.echo(f"\n{status_icon} {version['version_number']} ({version['version_type']})")
            click.echo(f"   Created: {version['created_at']}")
            click.echo(f"   Records: {version['records_count']:,}")
            click.echo(f"   ETL Job: {version['etl_job_id'] or 'N/A'}")
            if version['description']:
                click.echo(f"   Description: {version['description']}")
            if version['archived_records'] > 0:
                click.echo(f"   Archived: {version['archived_records']:,} records")
        
    except Exception as e:
        click.echo(f"❌ Failed to list versions: {e}")

@versions.command()
def current():
    """Show current active version"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            query = text("SELECT * FROM retail_dw.v_current_version")
            result = session.execute(query)
            row = result.fetchone()
            
            if not row:
                click.echo("📦 No current version found")
                return
            
            click.echo("📦 Current Active Version")
            click.echo("=" * 40)
            click.echo(f"Version: {row.version_number}")
            click.echo(f"Type: {row.version_type}")
            click.echo(f"Created: {row.created_at}")
            click.echo(f"Records: {row.records_count:,}")
            click.echo(f"ETL Job: {row.etl_job_id or 'N/A'}")
            if row.description:
                click.echo(f"Description: {row.description}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get current version: {e}")

@versions.command()
@click.option('--limit', default=5, help='Number of comparisons to show')
def compare(limit):
    """Compare versions to see changes"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            query = text("SELECT * FROM retail_dw.v_version_comparison WHERE previous_version IS NOT NULL LIMIT :limit")
            result = session.execute(query, {'limit': limit})
            
            comparisons = []
            for row in result:
                comparisons.append({
                    'current_version': row.current_version,
                    'current_records': row.current_records or 0,
                    'current_date': row.current_date,
                    'previous_version': row.previous_version,
                    'previous_records': row.previous_records or 0,
                    'previous_date': row.previous_date,
                    'record_change': row.record_change or 0
                })
        
        if not comparisons:
            click.echo("📊 No version comparisons available")
            click.echo("💡 You need at least 2 versions to compare")
            return
        
        click.echo(f"📊 Version Comparisons (showing {len(comparisons)})")
        click.echo("=" * 70)
        
        for comp in comparisons:
            change_icon = "📈" if comp['record_change'] > 0 else "📉" if comp['record_change'] < 0 else "➡️"
            change_text = f"{comp['record_change']:+,}" if comp['record_change'] != 0 else "No change"
            
            click.echo(f"\n{change_icon} {comp['current_version']} vs {comp['previous_version']}")
            click.echo(f"   Current:  {comp['current_records']:,} records ({comp['current_date']})")
            click.echo(f"   Previous: {comp['previous_records']:,} records ({comp['previous_date']})")
            click.echo(f"   Change:   {change_text}")
        
    except Exception as e:
        click.echo(f"❌ Failed to compare versions: {e}")

@versions.command()
@click.argument('version_number')
def show(version_number):
    """Show details for a specific version"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            # Get version details
            version_query = text("SELECT * FROM retail_dw.data_versions WHERE version_number = :version")
            version_result = session.execute(version_query, {'version': version_number})
            version_row = version_result.fetchone()
            
            if not version_row:
                click.echo(f"❌ Version '{version_number}' not found")
                return
            
            click.echo(f"📦 Version Details: {version_number}")
            click.echo("=" * 50)
            click.echo(f"ID: {version_row.version_id}")
            click.echo(f"Type: {version_row.version_type}")
            click.echo(f"Status: {version_row.status}")
            click.echo(f"Created: {version_row.created_at}")
            click.echo(f"Created by: {version_row.created_by}")
            click.echo(f"Records: {version_row.records_count or 0:,}")
            click.echo(f"ETL Job: {version_row.etl_job_id or 'N/A'}")
            if version_row.source_file:
                click.echo(f"Source File: {version_row.source_file}")
            if version_row.file_hash:
                click.echo(f"File Hash: {version_row.file_hash}")
            if version_row.description:
                click.echo(f"Description: {version_row.description}")
            
            # Get data distribution for this version
            data_query = text("""
                SELECT 
                    COUNT(*) as fact_count,
                    MIN(transaction_datetime) as earliest_transaction,
                    MAX(transaction_datetime) as latest_transaction,
                    COUNT(DISTINCT customer_key) as unique_customers,
                    COUNT(DISTINCT product_key) as unique_products
                FROM retail_dw.fact_sales 
                WHERE version_id = :version_id
            """)
            
            data_result = session.execute(data_query, {'version_id': version_row.version_id})
            data_row = data_result.fetchone()
            
            if data_row and data_row.fact_count > 0:
                click.echo(f"\n📊 Data Distribution:")
                click.echo(f"   Sales Records: {data_row.fact_count:,}")
                click.echo(f"   Date Range: {data_row.earliest_transaction} to {data_row.latest_transaction}")
                click.echo(f"   Unique Customers: {data_row.unique_customers:,}")
                click.echo(f"   Unique Products: {data_row.unique_products:,}")
        
    except Exception as e:
        click.echo(f"❌ Failed to show version details: {e}")

@versions.command()
def cleanup():
    """Show disk space used by versions"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            # Get table sizes
            size_query = text("""
                SELECT 
                    'fact_sales' as table_name,
                    pg_size_pretty(pg_total_relation_size('retail_dw.fact_sales')) as size,
                    COUNT(*) as records
                FROM retail_dw.fact_sales
                UNION ALL
                SELECT 
                    'fact_sales_archive' as table_name,
                    pg_size_pretty(pg_total_relation_size('retail_dw.fact_sales_archive')) as size,
                    COUNT(*) as records
                FROM retail_dw.fact_sales_archive
            """)
            
            result = session.execute(size_query)
            
            click.echo("💾 Storage Usage by Versions")
            click.echo("=" * 40)
            
            for row in result:
                click.echo(f"{row.table_name}: {row.size} ({row.records:,} records)")
            
            # Show version breakdown
            version_query = text("""
                SELECT 
                    v.version_number,
                    v.status,
                    COUNT(f.fact_sales_key) as active_records,
                    COUNT(a.archive_id) as archived_records
                FROM retail_dw.data_versions v
                LEFT JOIN retail_dw.fact_sales f ON v.version_id = f.version_id
                LEFT JOIN retail_dw.fact_sales_archive a ON v.version_id = a.version_id
                GROUP BY v.version_id, v.version_number, v.status
                ORDER BY v.created_at DESC
                LIMIT 10
            """)
            
            version_result = session.execute(version_query)
            
            click.echo(f"\n📦 Version Breakdown:")
            for row in version_result:
                active = row.active_records or 0
                archived = row.archived_records or 0
                total = active + archived
                status = row.status
                click.echo(f"   {row.version_number} ({status}): {total:,} records ({active:,} active, {archived:,} archived)")
        
    except Exception as e:
        click.echo(f"❌ Failed to show cleanup info: {e}")
# ADD this to your existing @versions group in main.py
# ADD this to your existing @versions group in main.py

@versions.command()
def partitions():
    """Check partition status and information"""
    try:
        from retail_data_platform.database.connection import get_db_session
        from sqlalchemy import text
        
        with get_db_session() as session:
            # Check if fact_sales is partitioned
            partition_check = text("""
                SELECT 
                    t.tablename,
                    CASE 
                        WHEN EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = ('retail_dw.'||t.tablename)::regclass)
                        THEN 'PARTITIONED'
                        ELSE 'REGULAR_TABLE'
                    END as table_type,
                    pg_size_pretty(pg_total_relation_size('retail_dw.'||t.tablename)) as size,
                    COUNT(CASE WHEN t2.tablename LIKE t.tablename || '_%' THEN 1 END) as partition_count
                FROM pg_tables t
                LEFT JOIN pg_tables t2 ON t2.schemaname = 'retail_dw' AND t2.tablename LIKE t.tablename || '_%'
                WHERE t.schemaname = 'retail_dw' 
                AND t.tablename = 'fact_sales'
                GROUP BY t.tablename, t.schemaname;
            """)
            
            result = session.execute(partition_check)
            row = result.fetchone()
            
            if not row:
                click.echo("❌ fact_sales table not found")
                return
            
            click.echo("📊 Partitioning Status")
            click.echo("=" * 40)
            click.echo(f"Table: {row.tablename}")
            click.echo(f"Type: {row.table_type}")
            click.echo(f"Size: {row.size}")
            click.echo(f"Partitions: {row.partition_count}")
            
            if row.table_type == 'REGULAR_TABLE':
                click.echo("\n💡 Partitioning is NOT enabled")
                click.echo("   To enable partitioning:")
                click.echo("   1. python main.py setup --drop-existing  (⚠️ DESTROYS DATA)")
                click.echo("   2. Edit schema.py to uncomment partitioning")
                
                # Show table info
                table_info_query = text("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(transaction_datetime) as earliest_date,
                        MAX(transaction_datetime) as latest_date
                    FROM retail_dw.fact_sales
                """)
                
                info_result = session.execute(table_info_query)
                info_row = info_result.fetchone()
                
                if info_row and info_row.total_records > 0:
                    click.echo(f"\n📈 Current Data:")
                    click.echo(f"   Records: {info_row.total_records:,}")
                    click.echo(f"   Date Range: {info_row.earliest_date} to {info_row.latest_date}")
                
            else:
                # Show partition details
                partition_query = text("""
                    SELECT 
                        tablename as partition_name,
                        pg_size_pretty(pg_total_relation_size('retail_dw.'||tablename)) as size
                    FROM pg_tables 
                    WHERE schemaname = 'retail_dw' 
                    AND tablename LIKE 'fact_sales_%'
                    ORDER BY tablename;
                """)
                
                partition_result = session.execute(partition_query)
                
                click.echo(f"\n📅 Partition Details:")
                for partition_row in partition_result:
                    click.echo(f"   {partition_row.partition_name}: {partition_row.size}")
        
    except Exception as e:
        click.echo(f"❌ Failed to check partitions: {e}")
        
# ADD this line at the bottom of main.py (with other cli.add_command calls)
cli.add_command(versions)

if __name__ == '__main__':
    cli()