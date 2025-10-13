"""
Database Connection and Session Management

Handles PostgreSQL connections with connection pooling,
transaction management, and retry logic.
"""

import time
from contextlib import contextmanager
from typing import Generator, Optional, Any, Dict
from sqlalchemy import create_engine, text, event, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError
from sqlalchemy.pool import QueuePool

from ..config.config_manager import get_config
from ..utils.logging_config import get_logger


logger = get_logger(__name__)


class DatabaseManager:
    """
    Database connection manager with pooling and retry logic
    """
    
    def __init__(self):
        self.config = get_config()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    @property
    def engine(self) -> Engine:
        """Get database engine, creating if necessary"""
        if self._engine is None:
            self._create_engine()
        return self._engine
    
    def _create_engine(self) -> None:
        """Create SQLAlchemy engine with connection pooling"""
        db_config = self.config.database
        
        engine_kwargs = {
            'poolclass': QueuePool,
            'pool_size': db_config.pool_size,
            'max_overflow': db_config.max_overflow,
            'pool_pre_ping': True,  # Validate connections before use
            'pool_recycle': 3600,   # Recycle connections after 1 hour
            'echo': self.config.debug,  # Log SQL in debug mode
        }
        
        self._engine = create_engine(
            db_config.connection_string,
            **engine_kwargs
        )
        
        # Add event listeners for monitoring
        self._setup_engine_events()
        
        logger.info("Database engine created", 
                   host=db_config.host, 
                   database=db_config.database)
    
    def _setup_engine_events(self) -> None:
        """Setup SQLAlchemy event listeners for monitoring"""
        
        @event.listens_for(self._engine, "connect")
        def set_search_path(dbapi_connection, connection_record):
            """Set schema search path on connection"""
            schema = self.config.database.schema
            if schema != "public":
                with dbapi_connection.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {schema}, public")
        
        @event.listens_for(self._engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Log slow queries in debug mode"""
            if self.config.debug:
                context._query_start_time = time.time()
        
        @event.listens_for(self._engine, "after_cursor_execute")
        def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            """Log query execution time in debug mode"""
            if self.config.debug and hasattr(context, '_query_start_time'):
                total = time.time() - context._query_start_time
                if total > 1.0:  # Log queries taking more than 1 second
                    logger.warning("Slow query detected", 
                                 duration=total, 
                                 query=statement[:100])
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory, creating if necessary"""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions with automatic cleanup
        
        Yields:
            Database session
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Database session error", error=str(e), exc_info=True)
            raise
        finally:
            session.close()
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query with optional parameters and retry logic"""
        import time
        import traceback
        
        for attempt in range(self.config.etl.max_retries):
            try:
                with self.get_session() as session:
                    result = session.execute(text(query), parameters or {})
                    session.commit()
                    
                    # Check if this is a statement that returns rows
                    try:
                        # Try to fetch results - will work for SELECT statements
                        return result.fetchall()
                    except Exception:
                        # For DDL statements (ALTER, CREATE, DROP) that don't return rows
                        return None
                        
            except Exception as e:
                logger.error(
                    "Database session error",
                    error=str(e),
                    exception=traceback.format_exc()
                )
                
                if attempt < self.config.etl.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Query failed, retrying in {wait_time}s",
                        attempt=attempt + 1,
                        error=str(e)
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        "Query failed after all retries",
                        query=query[:100] + "..." if len(query) > 100 else query,
                        error=str(e)
                    )
                    raise
    
    def test_connection(self) -> bool:
        """
        Test database connectivity
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error("Database connection test failed", error=str(e))
            return False
    
    def create_schema_if_not_exists(self) -> None:
        """Create the application schema if it doesn't exist"""
        schema_name = self.config.database.schema
        if schema_name != "public":
            try:
                with self.get_session() as session:
                    session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                logger.info(f"Schema {schema_name} created or already exists")
            except Exception as e:
                logger.error(f"Failed to create schema {schema_name}", error=str(e))
                raise
    
    def close_all_connections(self) -> None:
        """Close all database connections"""
        if self._engine:
            self._engine.dispose()
            logger.info("All database connections closed")


# Global database manager instance
db_manager = DatabaseManager()

def get_db_session() -> Generator[Session, None, None]:
    """Get database session - convenience function"""
    return db_manager.get_session()

def execute_sql(query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
    """Execute SQL query - convenience function"""
    return db_manager.execute_query(query, parameters)