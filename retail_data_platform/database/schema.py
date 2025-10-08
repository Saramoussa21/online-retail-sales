"""
Database Schema Creation and Management

Handles database schema creation, table creation,
and initial data setup for the retail data warehouse.
"""

from typing import List, Dict, Any
from sqlalchemy import text, MetaData
from sqlalchemy.exc import ProgrammingError

from .connection import db_manager
from .models import Base, DimDate, DimCustomer, DimProduct, FactSales
from ..utils.logging_config import get_logger
from ..config.config_manager import get_config
from sqlalchemy import text, MetaData
from .connection import db_manager  

logger = get_logger(__name__)


class SchemaManager:
    """
    Manages database schema operations including creation,
    migration, and maintenance tasks.
    """
    
    def __init__(self):
        self.config = get_config()
        self.schema_name = self.config.database.schema
        self.logger = get_logger(__name__) 

    def create_schema(self) -> None:
        """Create the retail data warehouse schema"""
        try:
            db_manager.create_schema_if_not_exists()
            logger.info(f"Schema {self.schema_name} ready")
        except Exception as e:
            logger.error(f"Failed to create schema {self.schema_name}", error=str(e))
            raise
    
    def create_tables(self) -> None:
        """Create all tables defined in the models"""
        try:
            # Create all tables
            Base.metadata.create_all(db_manager.engine)
            logger.info("All tables created successfully")
            
            # Create additional constraints and functions
            self._create_additional_constraints()
            self._create_helper_functions()
            
        except Exception as e:
            logger.error("Failed to create tables", error=str(e))
            raise
    def execute_sql(self, sql: str) -> None:
        """Execute SQL command safely"""
        try:
            db_manager.execute_query(sql)
            self.logger.debug("SQL executed successfully")
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            raise

    def create_date_dimension(self):
        """Create date dimension table"""
        try:
            
            self.logger.info("Date dimension table ready")
        except Exception as e:
            self.logger.error(f"Failed to create date dimension: {e}")
            raise

    def create_customer_dimension(self):
        """Create customer dimension table"""
        try:
    
            self.logger.info("Customer dimension table ready")
        except Exception as e:
            self.logger.error(f"Failed to create customer dimension: {e}")
            raise

    def create_product_dimension(self):
        """Create product dimension table"""
        try:
           
            self.logger.info("Product dimension table ready")
        except Exception as e:
            self.logger.error(f"Failed to create product dimension: {e}")
            raise

    def create_sales_fact_table(self):
        """Create sales fact table"""
        try:
        
            self.logger.info("Sales fact table ready")
        except Exception as e:
            self.logger.error(f"Failed to create sales fact table: {e}")
            raise

    def create_quality_monitoring_tables(self):
        """Create quality monitoring tables (optional)"""
        try:
           
            self.logger.info("Quality monitoring tables ready")
        except Exception as e:
            self.logger.warning(f"Quality monitoring tables creation failed: {e}")
            
    def _create_additional_constraints(self) -> None:
        """Create basic database constraints and indexes"""
        
        constraints_sql = [
            # Ensure line_total calculation is correct
            f"""
            ALTER TABLE {self.schema_name}.fact_sales 
            ADD CONSTRAINT chk_line_total_calculation 
            CHECK (line_total = quantity * unit_price);
            """,
            
            # Ensure SCD Type 2 integrity for customers
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_one_current 
            ON {self.schema_name}.dim_customer (customer_id) 
            WHERE is_current = true;
            """
        ]
        
        for sql in constraints_sql:
            try:
                db_manager.execute_query(sql)
                logger.debug("Executed constraint SQL", sql=sql[:100])
            except ProgrammingError as e:
                if "already exists" not in str(e):
                    logger.warning("Failed to create constraint", sql=sql[:100], error=str(e))
    
    def _create_helper_functions(self) -> None:
        """Create basic indexes for performance optimization"""
        
        indexes_sql = [
            # Create indexes for better query performance
            f"""
            CREATE INDEX IF NOT EXISTS idx_fact_sales_date 
            ON {self.schema_name}.fact_sales (date_key);
            """,
            
            f"""
            CREATE INDEX IF NOT EXISTS idx_fact_sales_customer 
            ON {self.schema_name}.fact_sales (customer_key);
            """,
            
            f"""
            CREATE INDEX IF NOT EXISTS idx_fact_sales_product 
            ON {self.schema_name}.fact_sales (product_key);
            """,
            
            f"""
            CREATE INDEX IF NOT EXISTS idx_dim_customer_id 
            ON {self.schema_name}.dim_customer (customer_id);
            """,
            
            f"""
            CREATE INDEX IF NOT EXISTS idx_dim_product_stock_code 
            ON {self.schema_name}.dim_product (stock_code);
            """
        ]
        
        for sql in indexes_sql:
            try:
                db_manager.execute_query(sql)
                logger.debug("Created index", sql=sql[:100])
            except Exception as e:
                logger.warning("Failed to create index", error=str(e))
    
    def create_basic_indexes(self) -> None:
        """Create basic indexes for performance optimization"""
        
        indexes_sql = [
            # performance indexes
            f"""
            CREATE INDEX IF NOT EXISTS idx_fact_sales_year 
            ON {self.schema_name}.fact_sales 
            USING BTREE (EXTRACT(YEAR FROM transaction_datetime));
            """
        ]
        
        for sql in indexes_sql:
            try:
                db_manager.execute_query(sql)
                logger.debug("Created index", sql=sql[:100])
            except Exception as e:
                logger.warning("Failed to create index", error=str(e))
    

    def create_versioning_tables(self):
        """Create data versioning and lineage tables"""
        versioning_sql = """
        -- Data Version Control Tables
        CREATE TABLE IF NOT EXISTS retail_dw.data_versions (
            version_id SERIAL PRIMARY KEY,
            version_number VARCHAR(20) NOT NULL,
            version_type VARCHAR(20) NOT NULL DEFAULT 'INCREMENTAL',
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            created_by VARCHAR(100) DEFAULT 'system',
            description TEXT,
            source_file VARCHAR(255),
            records_count BIGINT DEFAULT 0,
            file_hash VARCHAR(64),
            status VARCHAR(20) DEFAULT 'ACTIVE',
            etl_job_id VARCHAR(100),
            
            CONSTRAINT unique_version_number UNIQUE (version_number)
        );

        -- Archive table for historical fact_sales snapshots
        CREATE TABLE IF NOT EXISTS retail_dw.fact_sales_archive (
            archive_id SERIAL PRIMARY KEY,
            version_id INTEGER REFERENCES retail_dw.data_versions(version_id),
            archived_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            
            -- Copy all columns from fact_sales
            fact_sales_key BIGINT,
            customer_key BIGINT,
            product_key BIGINT,
            date_key INTEGER,
            invoice_no VARCHAR(50),
            stock_code VARCHAR(50),
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            line_total DECIMAL(12,2),
            transaction_datetime TIMESTAMP WITHOUT TIME ZONE,
            batch_id VARCHAR(100),
            created_at TIMESTAMP WITHOUT TIME ZONE,
            updated_at TIMESTAMP WITHOUT TIME ZONE,
            data_source VARCHAR(50)
        );
        """
        
        # Execute the core versioning tables first
        self.execute_sql(versioning_sql)
        
        # Only add version columns if tables exist
        self._add_version_columns_safely()
        
        # Create views and indexes
        self._create_versioning_views_and_indexes()
        
        self.logger.info("✅ Versioning tables and views created")
    
    def _add_version_columns_safely(self):
        """Add version columns only if tables exist"""
        tables_to_check = ['fact_sales', 'dim_customer', 'dim_product']
        
        for table_name in tables_to_check:
            try:
                # Check if table exists using the same connection method
                check_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'retail_dw' 
                    AND table_name = '{table_name}'
                );
                """
                
                # Use the same connection method consistently
                with db_manager.get_session() as session:
                    result = session.execute(text(check_sql))
                    table_exists = result.scalar()
                    
                    if table_exists:
                        # Check if version_id column already exists
                        column_check_sql = f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_schema = 'retail_dw' 
                            AND table_name = '{table_name}'
                            AND column_name = 'version_id'
                        );
                        """
                        
                        column_result = session.execute(text(column_check_sql))
                        column_exists = column_result.scalar()
                        
                        if not column_exists:
                            alter_sql = f"""
                            ALTER TABLE retail_dw.{table_name} 
                            ADD COLUMN version_id INTEGER,
                            ADD COLUMN version_created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
                            """
                            session.execute(text(alter_sql))
                            session.commit()
                            self.logger.info(f"✅ Added version columns to {table_name}")
                        else:
                            self.logger.info(f"✅ Version columns already exist in {table_name}")
                            
                        # Now add the foreign key constraint separately
                        try:
                            fk_sql = f"""
                            ALTER TABLE retail_dw.{table_name} 
                            ADD CONSTRAINT fk_{table_name}_version 
                            FOREIGN KEY (version_id) REFERENCES retail_dw.data_versions(version_id);
                            """
                            session.execute(text(fk_sql))
                            session.commit()
                            self.logger.debug(f"✅ Added foreign key constraint to {table_name}")
                        except Exception as fk_error:
                           
                            self.logger.debug(f"Foreign key constraint for {table_name}: {fk_error}")
                            
                    else:
                        self.logger.warning(f"⚠️ Table {table_name} doesn't exist, skipping version columns")
                        
            except Exception as e:
                self.logger.error(f"Failed to add version columns to {table_name}: {e}")
                

    def _create_versioning_views_and_indexes(self):
        """Create versioning views and indexes"""
        views_sql = """
        -- Create useful views for version management
        CREATE OR REPLACE VIEW retail_dw.v_current_version AS
        SELECT 
            version_id,
            version_number,
            version_type,
            created_at,
            description,
            records_count,
            etl_job_id,
            'CURRENT' as version_status
        FROM retail_dw.data_versions 
        WHERE status = 'ACTIVE'
        ORDER BY created_at DESC 
        LIMIT 1;

        CREATE OR REPLACE VIEW retail_dw.v_version_history AS
        SELECT 
            v.version_number,
            v.version_type,
            v.created_at,
            v.records_count,
            v.status,
            v.description,
            v.etl_job_id,
            COALESCE(sa.archived_records, 0) as archived_records
        FROM retail_dw.data_versions v
        LEFT JOIN (
            SELECT version_id, COUNT(*) as archived_records
            FROM retail_dw.fact_sales_archive 
            GROUP BY version_id
        ) sa ON v.version_id = sa.version_id
        ORDER BY v.created_at DESC;

        CREATE OR REPLACE VIEW retail_dw.v_version_comparison AS
        SELECT 
            v1.version_number as current_version,
            v1.records_count as current_records,
            v1.created_at as current_date,
            LAG(v1.version_number) OVER (ORDER BY v1.created_at) as previous_version,
            LAG(v1.records_count) OVER (ORDER BY v1.created_at) as previous_records,
            LAG(v1.created_at) OVER (ORDER BY v1.created_at) as previous_date,
            v1.records_count - LAG(v1.records_count) OVER (ORDER BY v1.created_at) as record_change
        FROM retail_dw.data_versions v1
        ORDER BY v1.created_at DESC;
        """
        
        self.execute_sql(views_sql)
        
        
        self._create_versioning_indexes_safely()

    def create_simple_partitioning(self):
        """Create simple, efficient partitioning for fact_sales"""
        try:
           
            check_table_sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'retail_dw' 
            AND table_name = 'fact_sales';
            """
            
            result = db_manager.execute_query(check_table_sql)
            table_exists = len(result) > 0
            
            if table_exists:
                self.logger.info("fact_sales table already exists - skipping partitioning")
                self.logger.warning("To enable partitioning, use --drop-existing flag")
            else:
                self.logger.info("Creating new partitioned fact_sales table")
                self._create_fresh_partitioned_table()
                
        except Exception as e:
            self.logger.error(f"Partitioning setup failed: {e}")
            raise

    def _create_fresh_partitioned_table(self):
        """Create new partitioned table from scratch"""
        partition_sql = """
        -- Create partitioned fact_sales table
        CREATE TABLE retail_dw.fact_sales (
            fact_sales_key BIGSERIAL,
            customer_key BIGINT NOT NULL,
            product_key BIGINT NOT NULL,
            date_key INTEGER NOT NULL,
            invoice_no VARCHAR(50) NOT NULL,
            stock_code VARCHAR(50) NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0,
            unit_price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            line_total DECIMAL(12,2) NOT NULL DEFAULT 0.00,
            transaction_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            batch_id VARCHAR(100),
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            data_source VARCHAR(50) DEFAULT 'CSV',
            
            -- Version tracking
            version_id INTEGER,
            version_created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            
            -- Primary key MUST include partition key
            CONSTRAINT fact_sales_pkey PRIMARY KEY (fact_sales_key, transaction_datetime)
        ) PARTITION BY RANGE (transaction_datetime);
        
        -- Create 3 simple partitions
        -- Historical data (2010-2011)
        CREATE TABLE retail_dw.fact_sales_historical PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2010-01-01') TO ('2012-01-01');
        
        -- Current year (2025)
        CREATE TABLE retail_dw.fact_sales_current PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
        
        -- Future data
        CREATE TABLE retail_dw.fact_sales_future PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2026-01-01') TO ('2030-01-01');
        
        -- Essential indexes only
        CREATE INDEX idx_fact_sales_historical_date ON retail_dw.fact_sales_historical (transaction_datetime);
        CREATE INDEX idx_fact_sales_historical_customer ON retail_dw.fact_sales_historical (customer_key);
        CREATE INDEX idx_fact_sales_historical_product ON retail_dw.fact_sales_historical (product_key);
        
        CREATE INDEX idx_fact_sales_current_date ON retail_dw.fact_sales_current (transaction_datetime);
        CREATE INDEX idx_fact_sales_current_customer ON retail_dw.fact_sales_current (customer_key);
        CREATE INDEX idx_fact_sales_current_product ON retail_dw.fact_sales_current (product_key);
        
        CREATE INDEX idx_fact_sales_future_date ON retail_dw.fact_sales_future (transaction_datetime);
        CREATE INDEX idx_fact_sales_future_customer ON retail_dw.fact_sales_future (customer_key);
        CREATE INDEX idx_fact_sales_future_product ON retail_dw.fact_sales_future (product_key);
        """
        
       
        db_manager.execute_query(partition_sql)
        self.logger.info("✅ Partitioned fact_sales table created with 3 partitions")


    def _create_versioning_indexes_safely(self):
        """Create versioning indexes only for existing tables and columns"""
        index_configs = [
            ('fact_sales', 'version_id'),
            ('dim_customer', 'version_id'),
            ('dim_product', 'version_id'),
            ('fact_sales_archive', 'version_id'),
            ('data_versions', 'status'),
            ('data_versions', 'created_at')
        ]
        
        for table_name, column_name in index_configs:
            try:
               
                column_check_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'retail_dw' 
                    AND table_name = '{table_name}'
                    AND column_name = '{column_name}'
                );
                """
                
                with db_manager.get_session() as session:
                    result = session.execute(text(column_check_sql))
                    column_exists = result.scalar()
                    
                    if column_exists:
                        index_name = f"idx_{table_name.replace('_', '')}_{column_name.replace('_', '')}"
                        index_sql = f"""
                        CREATE INDEX IF NOT EXISTS {index_name} 
                        ON retail_dw.{table_name} ({column_name});
                        """
                        session.execute(text(index_sql))
                        session.commit()
                        self.logger.debug(f"✅ Created index {index_name}")
                    else:
                        self.logger.debug(f"⚠️ Column {table_name}.{column_name} doesn't exist, skipping index")
                        
            except Exception as e:
                self.logger.warning(f"Failed to create index on {table_name}.{column_name}: {e}")
                


    def _add_partition_function_only(self):
        """Add partitioning function to existing table (safe approach)"""
        partition_function_sql = """
        -- Create function for automatic partition creation
        CREATE OR REPLACE FUNCTION retail_dw.create_monthly_partition(table_date DATE)
        RETURNS TEXT AS $$
        DECLARE
            partition_name TEXT;
            start_date TEXT;
            end_date TEXT;
        BEGIN
            -- Generate partition name: fact_sales_y2025m10
            partition_name := 'fact_sales_y' || TO_CHAR(table_date, 'YYYY') || 'm' || TO_CHAR(table_date, 'MM');
            
            -- Generate date range
            start_date := TO_CHAR(DATE_TRUNC('month', table_date), 'YYYY-MM-DD');
            end_date := TO_CHAR(DATE_TRUNC('month', table_date) + INTERVAL '1 month', 'YYYY-MM-DD');
            
            -- Check if we need to convert main table to partitioned
            IF NOT EXISTS (
                SELECT 1 FROM pg_partitioned_table 
                WHERE partrelid = 'retail_dw.fact_sales'::regclass
            ) THEN
                -- Table exists but not partitioned - we'll handle this later
                RAISE NOTICE 'Table exists but not partitioned yet';
                RETURN 'EXISTING_TABLE';
            END IF;
            
            -- Create partition if it doesn't exist
            EXECUTE format('CREATE TABLE IF NOT EXISTS retail_dw.%I PARTITION OF retail_dw.fact_sales FOR VALUES FROM (%L) TO (%L)',
                        partition_name, start_date, end_date);
            
            -- Create only ESSENTIAL indexes (just 3!)
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_date ON retail_dw.%I (transaction_datetime)', 
                        partition_name, partition_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_customer ON retail_dw.%I (customer_key)', 
                        partition_name, partition_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_product ON retail_dw.%I (product_key)', 
                        partition_name, partition_name);
            
            RETURN partition_name;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        self.execute_sql(partition_function_sql)
        self.logger.info("✅ Partition function added (table preserved)")

    def _create_fresh_partitioned_table(self):
        """Create new partitioned table from scratch"""
        fresh_partition_sql = """
        -- Create partitioned fact_sales table
        CREATE TABLE retail_dw.fact_sales (
            fact_sales_key BIGSERIAL,
            customer_key BIGINT NOT NULL,
            product_key BIGINT NOT NULL,
            date_key INTEGER NOT NULL,
            invoice_no VARCHAR(50) NOT NULL,
            stock_code VARCHAR(50) NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 0,
            unit_price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            line_total DECIMAL(12,2) NOT NULL DEFAULT 0.00,
            transaction_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            batch_id VARCHAR(100),
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            data_source VARCHAR(50) DEFAULT 'CSV',
            
            -- Version tracking
            version_id INTEGER,
            version_created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            
            -- Primary key MUST include partition key
            CONSTRAINT fact_sales_pkey PRIMARY KEY (fact_sales_key, transaction_datetime)
        ) PARTITION BY RANGE (transaction_datetime);
        
        -- Create just 3 ESSENTIAL partitions (current + historical)
        -- Historical data (2010-2011)
        CREATE TABLE retail_dw.fact_sales_historical PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2010-01-01') TO ('2012-01-01');
        
        -- Current year
        CREATE TABLE retail_dw.fact_sales_current PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
        
        -- Future
        CREATE TABLE retail_dw.fact_sales_future PARTITION OF retail_dw.fact_sales
            FOR VALUES FROM ('2026-01-01') TO ('2030-01-01');
        
        -- Only 3 ESSENTIAL indexes per partition
        -- Historical partition indexes
        CREATE INDEX idx_fact_sales_historical_date ON retail_dw.fact_sales_historical (transaction_datetime);
        CREATE INDEX idx_fact_sales_historical_customer ON retail_dw.fact_sales_historical (customer_key);
        CREATE INDEX idx_fact_sales_historical_product ON retail_dw.fact_sales_historical (product_key);
        
        -- Current partition indexes
        CREATE INDEX idx_fact_sales_current_date ON retail_dw.fact_sales_current (transaction_datetime);
        CREATE INDEX idx_fact_sales_current_customer ON retail_dw.fact_sales_current (customer_key);
        CREATE INDEX idx_fact_sales_current_product ON retail_dw.fact_sales_current (product_key);
        
        -- Future partition indexes
        CREATE INDEX idx_fact_sales_future_date ON retail_dw.fact_sales_future (transaction_datetime);
        CREATE INDEX idx_fact_sales_future_customer ON retail_dw.fact_sales_future (customer_key);
        CREATE INDEX idx_fact_sales_future_product ON retail_dw.fact_sales_future (product_key);
        """
        
        self.execute_sql(fresh_partition_sql)
        self.logger.info("✅ Simple partitioned table created (3 partitions, 9 indexes total)")
        
  
    def setup_complete_schema(self):
        """Setup complete data warehouse schema with simple partitioning"""
        try:
            self.logger.info("Setting up complete data warehouse schema")
            
            # Step 1: Create schema
            self.create_schema()
            
            # Step 2: Create ALL tables using SQLAlchemy models
            self.create_tables()  # ✅ This creates fact_sales, dim_*, etc.
            
            # Step 3: Create versioning tables and add version columns
            self.create_versioning_tables() 
            
            self.create_simple_partitioning()  
            
            # Step 5: Create basic indexes for dimensions only
            self.create_basic_indexes()
            
            self.logger.info("✅ Schema setup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Schema setup failed: {e}")
            raise

    def drop_schema(self, confirm: bool = False) -> None:
        """Drop the entire schema - use with caution!"""
        if not confirm:
            raise ValueError("Must set confirm=True to drop schema")
        
        try:
            db_manager.execute_query(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
            logger.warning(f"Schema {self.schema_name} dropped")
        except Exception as e:
            logger.error("Failed to drop schema", error=str(e))
            raise


# Initialize schema manager
schema_manager = SchemaManager()