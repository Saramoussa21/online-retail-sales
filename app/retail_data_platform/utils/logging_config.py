"""Logging configuration used by the app
"""

import os
import logging
from typing import Dict, Any

import structlog

try:
    import colorama
    colorama.init()
except Exception:
    colorama = None

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "console").lower()  # 'console' or 'json'


def _get_level(name: str) -> int:
    return getattr(logging, name, logging.INFO)


def _configure_stdlib() -> None:
    """Configure the standard library logging root logger."""
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.addHandler(handler)
    root.setLevel(_get_level(LOG_LEVEL))

    noisy = ["sqlalchemy", "sqlalchemy.engine", "alembic", "urllib3"]
    for name in noisy:
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        if lg.handlers:
            lg.handlers.clear()
        lg.propagate = False


def _configure_structlog() -> None:
    """Configure structlog for compact console or JSON output."""
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if LOG_FORMAT == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Console renderer with colors when available and compact output
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def configure_logging() -> None:
    """Apply logging configuration (idempotent)."""
    _configure_stdlib()
    _configure_structlog()


def get_logger(name: str) -> structlog.BoundLogger:
    """Return a structlog logger bound to `name`."""
    return structlog.get_logger(name)


class ETLLogger:
    """Small wrapper used across the project for consistent, concise logs.

    Usage:
        log = ETLLogger("pipeline")
        log.set_context(job_id=..., job_name=...)
        log.info("Started ETL job")
    """

    def __init__(self, component: str):
        self._logger = get_logger(f"etl.{component}")
        self._context: Dict[str, Any] = {}

    def set_context(self, **kwargs: Any) -> None:
        self._context.update(kwargs)

    def clear_context(self) -> None:
        self._context = {}

    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        merged = {**self._context, **kwargs}
        getattr(self._logger, level)(message, **merged)

    def info(self, message: str, **kwargs: Any) -> None:
        self._log("info", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._log("warning", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._log("error", message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        self._log("debug", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        self._log("critical", message, **kwargs)

    def log_etl_step(self, step: str, status: str, **metrics: Any) -> None:
        self.info(f"ETL step {step} -> {status}", step=step, status=status, **metrics)

    def log_performance(self, operation: str, duration: float, records: int = None) -> None:
        """Compatibility helper used by ingestion/pipeline code to log perf metrics."""
        data: Dict[str, Any] = {"operation": operation, "duration": duration}
        if records is not None:
            data["records"] = records
            data["records_per_second"] = records / duration if duration > 0 else None
        self.info(f"Performance: {operation}", **data)

    def log_data_quality(self, table: str, metrics: Dict[str, Any]) -> None:
        """Compatibility helper to log data quality metrics."""
        self.info(f"Data quality for {table}", table=table, quality_metrics=metrics)


configure_logging()
