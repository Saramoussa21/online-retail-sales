# ... ...
"""
Database Schema Creation and Management (cleaned)

Responsibilities:
- create schema + ORM tables (Base.metadata.create_all)
- create safe additional constraints / indexes
- provide an explicit helper to create monthly partitions for a given datetime range
- create a DEFAULT partition (catch-all) if needed

Usage:
    from retail_data_platform.database.schema import schema_manager
    schema_manager.create_schema()
    schema_manager.create_tables()
    # BEFORE bulk inserting rows: ensure partitions for the date range in your batch
    schema_manager.ensure_monthly_partitions_for_range(min_dt, max_dt)
"""
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from sqlalchemy import text

from .connection import db_manager
from ..utils.logging_config import get_logger
from ..config.config_manager import get_config

logger = get_logger(__name__)


class SchemaManager:
    """Lightweight schema manager that applies a single SQL file for setup.

    This avoids embedding complex partition DDL in Python/ORM and makes setup
    deterministic: modify the SQL file (`database/setup.sql`) and re-run setup.
    """

    def __init__(self):
        cfg = get_config()
        self.schema_name = getattr(cfg.database, "schema", "retail_dw")
        self.project_root = Path(__file__).parent

    def apply_sql_file(self, sql_path: Optional[str] = None) -> None:
        """Execute the SQL file located at `sql_path` or default `setup.sql`."""
        if sql_path is None:
            sql_path = str(self.project_root / "setup.sql")
        p = Path(sql_path)
        if not p.exists():
            raise FileNotFoundError(f"SQL schema file not found: {sql_path}")
        sql = p.read_text(encoding="utf-8")
        # execute whole SQL content via db_manager
        db_manager.execute_query(sql)

    def ensure_partitions_for_range(self, min_dt: datetime, max_dt: datetime) -> None:
        """Call server-side helper if you need to create partitions on demand.

        The SQL file defines a function `retail_dw.create_yearly_partitions(start_year, end_year)`.
        """
        if min_dt is None or max_dt is None:
            raise ValueError("min_dt and max_dt required")
        start_year = min_dt.year
        end_year = max_dt.year
        sql = f"SELECT retail_dw.create_yearly_partitions({start_year}, {end_year});"
        db_manager.execute_query(sql)

    def setup_complete_schema(self) -> None:
        """Backwards-compatible entry point used by main.setup().

        This will apply the single SQL file and ensure any auxiliary objects.
        """
        # apply the SQL file (this creates schema, tables, functions, indexes)
        self.apply_sql_file()

    def drop_schema(self, confirm: bool = False) -> None:
        """Drop the configured schema (dangerous). Requires confirm=True."""
        if not confirm:
            raise ValueError("Must set confirm=True to drop schema")
        db_manager.execute_query(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        logger.warning("Schema %s dropped", self.schema_name)


schema_manager = SchemaManager()
