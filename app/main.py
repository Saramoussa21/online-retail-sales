"""
Retail Data Platform - Main Application """

import os
import click
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from retail_data_platform.etl.pipeline import run_retail_csv_etl
from retail_data_platform.database.schema import schema_manager
from retail_data_platform.database.connection import db_manager
from retail_data_platform.config.config_manager import ConfigManager, get_config
from retail_data_platform.utils.logging_config import configure_logging, get_logger
from retail_data_platform.scheduling.job_manager import job_manager
from retail_data_platform.performance.optimization import performance_optimizer
from retail_data_platform.metadata.catalog import metadata_manager

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

@click.group()
@click.option('--config', type=str, help='Configuration file path')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              default='INFO', help='Logging level')
@click.option('--environment', type=str, help='Environment (development/production)')
def cli(config: Optional[str], log_level: str, environment: Optional[str]):
    """Retail Data Platform"""
    if environment:
        os.environ['ENVIRONMENT'] = environment

    os.environ['LOG_LEVEL'] = log_level
    configure_logging()

    if config:
        config_manager = ConfigManager(config)
        from retail_data_platform.config import config_manager as global_config
        global_config._config = None
        global_config.config_path = config

    logger = get_logger(__name__)
    logger.info("Retail Data Platform initialized",
                log_level=log_level,
                environment=environment or 'default')


@cli.command()
@click.option('--drop-existing', is_flag=True, help='Drop existing schema before creating (DESTRUCTIVE)')
def setup(drop_existing: bool):
    """Setup database schema and initial data"""
    logger = get_logger(__name__)
    try:
        logger.info("Starting database setup")
        if not db_manager.test_connection():
            click.echo("❌ Database connection failed. Please check your configuration.", err=True)
            sys.exit(1)
        if drop_existing:
            click.echo("⚠️  Dropping existing schema...")
            if click.confirm('This will delete all existing data. Are you sure?'):
                schema_manager.drop_schema(confirm=True)
                click.echo("✅ Schema dropped")
            else:
                click.echo("Setup cancelled")
                return
        click.echo("🔧 Creating database schema...")
        schema_manager.setup_complete_schema()
        click.echo("✅ Database schema created")
    except Exception as e:
        logger.error(f"Database setup failed: {e}", exc_info=True)
        click.echo(f"❌ Setup failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def test():
    """Test database connectivity and configuration"""
    logger = get_logger(__name__)
    try:
        click.echo("🔧 Testing system configuration...")
        config = get_config()
        click.echo(f"✅ Configuration loaded: {getattr(config, 'environment', 'unknown')}")
        if db_manager.test_connection():
            click.echo("✅ Database connection successful")
        else:
            click.echo("❌ Database connection failed")
            sys.exit(1)
        click.echo("✅ System test completed")
    except Exception as e:
        logger.error(f"System test failed: {e}", exc_info=True)
        click.echo(f"❌ System test failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--source', required=True, type=click.Path(exists=True), help='Source CSV file path')
@click.option('--job-name', type=str, help='Custom job name')
@click.option('--batch-size', type=int, default=1000, help='Batch size for processing')
def etl(source: str, job_name: Optional[str], batch_size: int):
    """Execute ETL pipeline for retail data"""
    logger = get_logger(__name__)
    try:
        if not job_name:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_name = f"retail_etl_{timestamp}"
        click.echo(f"Starting ETL job: {job_name}")
        metrics = run_retail_csv_etl(source, job_name)
        job_id = getattr(metrics, "job_id", metrics.get("job_id") if isinstance(metrics, dict) else None)
        status = getattr(metrics, "status", metrics.get("status") if isinstance(metrics, dict) else None)
        records_loaded = getattr(metrics, "records_loaded", metrics.get("records_loaded", 0) if isinstance(metrics, dict) else 0)
        duration = getattr(metrics, "total_duration", metrics.get("total_duration", 0.0) if isinstance(metrics, dict) else 0.0)
        click.echo(f"Job ID: {job_id}")
        click.echo(f"Status: {getattr(status, 'value', status)}")
        click.echo(f"Records Loaded: {records_loaded}")
        click.echo(f"Duration: {duration:.2f}s")
        if getattr(metrics, "version_number", None):
            click.echo(f"Version: {metrics.version_number}")
        if getattr(metrics, "version_id", None):
            click.echo(f"Version ID: {metrics.version_id}")
        if getattr(status, "value", status) != "SUCCESS":
            click.echo("❌ ETL job failed", err=True)
            sys.exit(1)
    except Exception as e:
        logger.error(f"ETL execution failed: {e}", exc_info=True)
        click.echo(f"❌ ETL failed: {e}", err=True)
        sys.exit(1)


# Schedule group
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
    click.echo(f"Daily job '{name}' scheduled at {time}")

@schedule.command()
def list():
    """List all scheduled jobs"""
    job_manager.list_jobs()

@schedule.command()
def start():
    """Start the scheduler (daemon)"""
    job_manager.start_scheduler()
    click.echo("Scheduler started (Ctrl+C to stop)")

cli.add_command(schedule)


# Performance groug
@click.group()
def performance():
    """Performance commands"""
    pass

@performance.command()
@click.option('--query', help='Specific query to analyze')
def analyze(query):
    """Analyze query performance (single query)"""
    if not query:
        query = "SELECT COUNT(*) as total_sales FROM retail_dw.fact_sales"
    result = performance_optimizer.optimize_query_with_cache(query)
    if 'error' in result:
        click.echo(f"❌ Query failed: {result['error']}")
        return
    click.echo(f"Execution Time: {result.get('execution_time_ms', 0):.2f} ms")
    click.echo(f"Rows Returned: {result.get('rows_returned', 0)}")
    click.echo(f"Cache Hit: {result.get('cache_hit', False)}")

@performance.command()
def cache_stats():
    """Show cache performance statistics"""
    stats = performance_optimizer.get_cache_performance()
    cache_stats = stats.get('cache_statistics', {})
    click.echo(f"Total Entries: {cache_stats.get('total_entries', 0)}")
    click.echo(f"Hit Ratio: {stats.get('cache_efficiency', {}).get('hit_ratio', 0):.1f}%")


cli.add_command(performance)

# Metadata group
@click.group()
def metadata():
    """Metadata & lineage"""
    pass

@metadata.command()
@click.option('--table', help='Specific table name')
def tables(table):
    """Show table information (columns / sizes)"""
    table_info = metadata_manager.catalog.get_table_info(table)
    table_sizes = metadata_manager.catalog.get_table_sizes()
    if table:
        click.echo(f"Table: {table}")
        cols = [c for c in table_info if c['table_name'] == table]
        for col in cols:
            click.echo(f"  {col['column_name']}: {col['data_type']}")
    else:
        click.echo("Tables summary:")
        names = sorted({c['table_name'] for c in table_info})
        for n in names:
            size = next((s['table_size'] for s in table_sizes if s['table_name']==n), 'Unknown')
            click.echo(f"  {n}: size={size}")

@metadata.command()
def lineage():
    """Show recent data lineage (last 5)"""
    recent = metadata_manager.lineage.get_recent_lineage(5)
    if not recent:
        click.echo("No lineage found")
        return
    for job in recent:
        click.echo(f"{job['etl_job_name']}: {job['records_processed']} records -> {job['target_table']}")

@metadata.command()
@click.option('--filename', default=None, help='Output filename (optional)')
@click.option('--complete', is_flag=True, help='Export complete metadata repository (includes dictionary, lineage, sizes)')
def export(filename, complete):
    """Export metadata (data dictionary or complete repository) to JSON"""
    try:
        if complete:
            path = metadata_manager.export_complete_repository(filename)
        else:
            path = metadata_manager.export_dictionary(filename)
        click.echo(f"✅ Metadata exported to: {path}")
    except Exception as e:
        from retail_data_platform.utils.logging_config import get_logger
        get_logger(__name__).error(f"Failed to export metadata: {e}", exc_info=True)
        click.echo(f"❌ Export failed: {e}", err=True)

cli.add_command(metadata)


# Quality group
@click.group()
def quality():
    """Data quality"""
    pass
@quality.command()
@click.option('--table', default='fact_sales', help='Table name to check')
def check(table):
    """Run quick quality checks on a table"""
    from retail_data_platform.monitoring.quality import create_quality_monitor
    # import alert manager lazily so CLI still works if alerts are not configured
    from retail_data_platform.monitoring.alerts import quality_alert_manager
    monitor = create_quality_monitor("cli_quick")
    from retail_data_platform.database.connection import get_db_session
    from sqlalchemy import text
    with get_db_session() as session:
        rows = session.execute(text(f"SELECT * FROM retail_dw.{table} LIMIT 500"))
        data = [dict(r) for r in rows.mappings()]
    if not data:
        click.echo("No data found")
        return
    results = monitor.check_data_quality(data, table)
    passed = sum(1 for r in results if getattr(r, 'is_threshold_met', False))
    click.echo(f"Checks run: {len(results)}, Passed: {passed}")

    # Persist metrics and trigger alerts
    try:
        monitor.persist_quality_metrics()
        summary = monitor.get_quality_summary()
        # summary contains overall_score etc. Alerts are logged by the alert manager;
        # this is non-destructive and configurable later to send emails/slack
        quality_alert_manager.check_and_alert(summary, table_name=table)
        anomalies = monitor.detect_quality_anomalies()
        quality_alert_manager.check_anomalies(anomalies)
    except Exception as e:
        # Avoid failing the CLI if alerts/persistence can't run; log and continue
        from retail_data_platform.utils.logging_config import get_logger
        get_logger(__name__).warning(f"Quality persistence/alerts failed: {e}")

@quality.command()
@click.option('--table', default='fact_sales', help='Table name')
def report(table):
    """Generate detailed quality report (short)"""
    from retail_data_platform.monitoring.quality import create_quality_monitor
    from retail_data_platform.monitoring.alerts import quality_alert_manager
    monitor = create_quality_monitor()
    from retail_data_platform.database.connection import get_db_session
    from sqlalchemy import text
    with get_db_session() as session:
        rows = session.execute(text(f"SELECT * FROM retail_dw.{table} LIMIT 1000"))
        data = [dict(r) for r in rows.mappings()]
    if not data:
        click.echo("No data found")
        return
    monitor.check_data_quality(data, table)
    report_text = monitor.generate_quality_report()
    click.echo(report_text)

    # Persist and alert (best-effort)
    try:
        monitor.persist_quality_metrics()
        summary = monitor.get_quality_summary()
        quality_alert_manager.check_and_alert(summary, table_name=table)
        anomalies = monitor.detect_quality_anomalies()
        quality_alert_manager.check_anomalies(anomalies)
    except Exception as e:
        from retail_data_platform.utils.logging_config import get_logger
        get_logger(__name__).warning(f"Quality persistence/alerts failed: {e}")
cli.add_command(quality)


# Alerts group
@click.group()
def alerts():
    """Alerting commands"""
    pass

@alerts.command()
@click.option('--table', default=None, help='Table to detect anomalies for (optional)')
@click.option('--show', is_flag=True, help='Print anomalies to stdout')
def run(table, show):
    """Run anomaly detection and trigger alerts"""
    from retail_data_platform.monitoring.quality import create_quality_monitor
    from retail_data_platform.monitoring.alerts import quality_alert_manager
    monitor = create_quality_monitor("cli_alerts")
    anomalies = monitor.detect_quality_anomalies() or []
    if table:
        anomalies = [a for a in anomalies if a.get('table_name') == table]
    # trigger alerts
    quality_alert_manager.check_anomalies(anomalies)
    click.echo(f"Processed {len(anomalies)} anomalies and triggered alerts (see logs)")
    if show and anomalies:
        click.echo("Anomalies:")
        for a in anomalies:
            click.echo(f" - table={a.get('table_name')} metric={a.get('metric_name')} current={a.get('current_score')} prev={a.get('previous_score')} drop={a.get('score_drop'):.2f} severity={a.get('severity')}")

@alerts.command()
@click.option('--level', default='CRITICAL', type=click.Choice(['INFO','WARNING','ERROR','CRITICAL']))
@click.option('--message', default='Test alert from CLI')
def test(level, message):
    """Send a test alert (logs and optionally persists)"""
    from retail_data_platform.monitoring.alerts import quality_alert_manager
    quality_alert_manager.send_db_alert(message)
    click.echo("Test alert sent")

cli.add_command(alerts)


# Versions group 
@click.group()
def versions():
    """Data versions"""
    pass

@versions.command()
@click.option('--limit', default=10, help='Number of versions to show')
def list(limit):
    """List recent data versions"""
    from retail_data_platform.database.connection import get_db_session
    from sqlalchemy import text
    with get_db_session() as session:
        q = text("SELECT version_number, records_count, status, created_at FROM retail_dw.data_versions ORDER BY created_at DESC LIMIT :limit")
        rows = session.execute(q, {'limit': limit})
        for r in rows:
            click.echo(f"{r.version_number} | {r.records_count or 0} rows | {r.status} | {r.created_at}")

@versions.command()
@click.argument('version_number')
def show(version_number):
    """Show details for a specific version"""
    from retail_data_platform.database.connection import get_db_session
    from sqlalchemy import text
    with get_db_session() as session:
        row = session.execute(text("SELECT * FROM retail_dw.data_versions WHERE version_number = :v"), {'v': version_number}).fetchone()
        if not row:
            click.echo("Version not found")
            return
        click.echo(f"{row.version_number} | {row.records_count or 0} rows | {row.status} | {row.created_at}")

cli.add_command(versions)

if __name__ == '__main__':
    cli()