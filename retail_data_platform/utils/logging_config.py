"""
Logging Configuration Module

Provides structured logging with different output formats
and levels for development and production environments.
"""

import logging
import logging.config
import structlog
from typing import Dict, Any
from pathlib import Path


def configure_logging(log_level: str = "INFO", log_format: str = "structured") -> None:
    """
    Configure application logging with structured output
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('structured' or 'standard')
    """
    
    # Configure standard library logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Configure structlog
    if log_format == "structured":
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    else:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.dev.ConsoleRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a configured logger instance
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return structlog.get_logger(name)



class ETLLogger:
    """Specialized logger for ETL operations with context management"""
    
    def __init__(self, component: str):
        self.logger = get_logger(f"etl.{component}")
        self.context = {}
    
    def set_context(self, **kwargs) -> None:
        """Set persistent context for all log messages"""
        self.context.update(kwargs)
    
    def clear_context(self) -> None:
        """Clear all context"""
        self.context = {}
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with context"""
        # 🔥 FIX: Remove duplicate keys from kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if key not in self.context:
                safe_kwargs[key] = value
        
        self.logger.info(message, **self.context, **safe_kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context"""
        # 🔥 FIX: Remove duplicate keys from kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if key not in self.context:
                safe_kwargs[key] = value
        
        self.logger.warning(message, **self.context, **safe_kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with context"""
        # 🔥 FIX: Remove duplicate keys from kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if key not in self.context:
                safe_kwargs[key] = value
        
        self.logger.error(message, **self.context, **safe_kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context"""
        # 🔥 FIX: Remove duplicate keys from kwargs
        safe_kwargs = {}
        for key, value in kwargs.items():
            if key not in self.context:
                safe_kwargs[key] = value
        
        self.logger.debug(message, **self.context, **safe_kwargs)
    
    def log_etl_step(self, step: str, status: str, **metrics) -> None:
        """Log ETL step completion with metrics"""
        # 🔥 FIX: Remove duplicate keys from metrics
        safe_metrics = {}
        for key, value in metrics.items():
            if key not in self.context:
                safe_metrics[key] = value
        
        self.info(
            f"ETL step completed: {step}",
            step=step,
            status=status,
            **safe_metrics
        )
    
    def log_data_quality(self, table: str, metrics: Dict[str, Any]) -> None:
        """Log data quality metrics"""
        self.info(
            f"Data quality check completed for {table}",
            table=table,
            quality_metrics=metrics
        )
    
    def log_performance(self, operation: str, duration: float, records: int = None) -> None:
        """Log performance metrics"""
        log_data = {
            "operation": operation,
            "duration_seconds": duration,
            "records_per_second": records / duration if records and duration > 0 else None
        }
        if records:
            log_data["records_processed"] = records
        
        # 🔥 FIX: Remove duplicate keys from log_data
        safe_log_data = {}
        for key, value in log_data.items():
            if key not in self.context:
                safe_log_data[key] = value
            
        self.info(f"Performance metrics for {operation}", **safe_log_data)