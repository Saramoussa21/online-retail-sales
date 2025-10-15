-- Single SQL bootstrap for retail_dw schema and partitioned fact_sales (monthly partitions 2010-2011)
CREATE SCHEMA IF NOT EXISTS retail_dw;

-- create enum type if not exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
                 WHERE t.typname = 'transactiontype' AND n.nspname = 'retail_dw') THEN
    CREATE TYPE retail_dw.transactiontype AS ENUM ( 'SALE','RETURN','ADJUSTMENT','FEE','SHIPPING','DISCOUNT','DONATION','SERVICE',
  'VOUCHER_SALE','VOUCHER_REDEMPTION','OTHER');
  END IF;
END
$$;

-- dimension tables
CREATE TABLE IF NOT EXISTS retail_dw.dim_date (
  date_key integer PRIMARY KEY,
  date_value date NOT NULL,
  year integer,
  quarter integer,
  month integer,
  week integer,
  day_of_year integer,
  day_of_month integer,
  day_of_week integer,
  month_name varchar(20),
  day_name varchar(20),
  quarter_name varchar(10),
  is_weekend boolean,
  is_holiday boolean
);

CREATE TABLE IF NOT EXISTS retail_dw.dim_customer (
  customer_key bigserial PRIMARY KEY,
  customer_id varchar(100) UNIQUE,
  country varchar(100),
  effective_date timestamp without time zone,
  expiry_date timestamp without time zone,
  -- versioning fields
  version_id integer NULL,
  version_created_at timestamp without time zone NULL,
  is_current boolean,
  created_at timestamp without time zone,
  updated_at timestamp without time zone,
  data_source varchar(50)
);

CREATE TABLE IF NOT EXISTS retail_dw.dim_product (
  product_key bigserial PRIMARY KEY,
  stock_code varchar(50) UNIQUE,
  description text,
  -- versioning fields
  version_id integer NULL,
  version_created_at timestamp without time zone NULL,
  category varchar(100),
  subcategory varchar(100),
  is_active boolean default true,
  is_gift boolean default false,
  created_at timestamp without time zone,
  updated_at timestamp without time zone,
  data_source varchar(50)
);

-- data_versions: track ETL versions and stats (columns used by pipeline)
CREATE TABLE IF NOT EXISTS retail_dw.data_versions (
  version_id serial PRIMARY KEY,
  version_number varchar(50) NOT NULL,
  version_type varchar(50) DEFAULT 'INCREMENTAL',
  description text,
  source_file varchar(500),
  file_hash varchar(100),
  etl_job_id varchar(100),
  status varchar(20),
  records_count bigint,
  created_at timestamp without time zone DEFAULT now()
);

-- dim_transaction_type: static lookup table for transaction types (match enum values)
CREATE TABLE dim_transaction_type (
    transaction_type   VARCHAR(50) PRIMARY KEY,
    effect_sign        SMALLINT NOT NULL CHECK (effect_sign IN (-1, 0, 1)),
    description        VARCHAR(255) NOT NULL,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- data_lineage: detailed ETL lineage/audit (match models.DataLineage)
CREATE TABLE IF NOT EXISTS retail_dw.data_lineage (
  lineage_id uuid PRIMARY KEY,
  source_system varchar(100) NOT NULL,
  source_table varchar(100) NOT NULL,
  source_file varchar(500),
  target_table varchar(100) NOT NULL,
  etl_job_name varchar(100) NOT NULL,
  batch_id varchar(100) NOT NULL,
  records_processed bigint NOT NULL,
  records_inserted bigint NOT NULL,
  records_updated bigint NOT NULL,
  records_rejected bigint NOT NULL DEFAULT 0,
  start_time timestamp without time zone NOT NULL,
  end_time timestamp without time zone NOT NULL,
  duration_seconds integer NOT NULL,
  status varchar(20) NOT NULL,
  error_message varchar(1000),
  job_metadata jsonb,
  created_at timestamp without time zone DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lineage_batch_id ON retail_dw.data_lineage (batch_id);
CREATE INDEX IF NOT EXISTS idx_lineage_target_table ON retail_dw.data_lineage (target_table);
CREATE INDEX IF NOT EXISTS idx_lineage_start_time ON retail_dw.data_lineage (start_time);
CREATE INDEX IF NOT EXISTS idx_lineage_status ON retail_dw.data_lineage (status);

CREATE TABLE IF NOT EXISTS retail_dw.data_quality_metrics (
  metric_id uuid PRIMARY KEY,
  table_name varchar(100) NOT NULL,
  column_name varchar(100),
  metric_name varchar(100) NOT NULL,
  metric_value numeric(15,4) NOT NULL,
  threshold_value numeric(15,4),
  is_threshold_met boolean,
  batch_id varchar(100) NOT NULL,
  measured_at timestamp without time zone NOT NULL DEFAULT now(),
  details jsonb
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_table_metric ON retail_dw.data_quality_metrics (table_name, metric_name);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_batch_id ON retail_dw.data_quality_metrics (batch_id);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_measured_at ON retail_dw.data_quality_metrics (measured_at);

-- parent partitioned fact table (monthly partitions by transaction_datetime)
CREATE TABLE IF NOT EXISTS retail_dw.fact_sales (
  sales_key bigserial NOT NULL,
  date_key integer NOT NULL,
  -- versioning fields (populated by pipeline)
  version_id integer NULL,
  version_created_at timestamp without time zone NULL,
  customer_key bigint NOT NULL,
  product_key bigint NOT NULL,
  invoice_no bigint NOT NULL,
  transaction_type retail_dw.transactiontype NOT NULL,
  quantity integer NOT NULL ,
  unit_price numeric(10,2) NOT NULL CHECK (unit_price >= 0),
  line_total numeric(15,2) NOT NULL CHECK (line_total = (quantity::numeric * unit_price)),
  transaction_datetime timestamp without time zone NOT NULL,
  created_at timestamp without time zone NOT NULL,
  batch_id varchar(100),
  data_source varchar(50) NOT NULL,
  PRIMARY KEY (sales_key, transaction_datetime)
) PARTITION BY RANGE (transaction_datetime);

-- Create DEFAULT partition to avoid missing-partition errors
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'retail_dw' AND c.relname = 'fact_sales_default') THEN
    EXECUTE 'CREATE TABLE retail_dw.fact_sales_default PARTITION OF retail_dw.fact_sales DEFAULT';
  END IF;
END
$$;

-- Create yearly partition helper and create partitions for 2010-2011
CREATE OR REPLACE FUNCTION retail_dw.create_yearly_partitions(start_year INT, end_year INT)
RETURNS void AS $$
DECLARE
  y INT := start_year;
  part_name TEXT;
  start_dt DATE;
  end_dt DATE;
BEGIN
  WHILE y <= end_year LOOP
    start_dt := make_date(y, 1, 1);
    end_dt := make_date(y + 1, 1, 1);
    part_name := format('fact_sales_y%s', y::text);
    BEGIN
      EXECUTE format($part$CREATE TABLE IF NOT EXISTS retail_dw.%I PARTITION OF retail_dw.fact_sales
                      FOR VALUES FROM (TIMESTAMP %L) TO (TIMESTAMP %L)$part$, part_name, start_dt::text, end_dt::text);
      EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_transaction_datetime ON retail_dw.%I (transaction_datetime)', part_name, part_name);
      EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_customer_key ON retail_dw.%I (customer_key)', part_name, part_name);
      EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_product_key ON retail_dw.%I (product_key)', part_name, part_name);
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Could not create partition % for % - %: %', part_name, start_dt, end_dt, SQLERRM;
    END;
    y := y + 1;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- create partitions for the initial range (2010-2011)
SELECT retail_dw.create_yearly_partitions(2010, 2011);

-- helpful indexes on dimensions
CREATE INDEX IF NOT EXISTS idx_dim_customer_customer_id ON retail_dw.dim_customer (customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_stock_code ON retail_dw.dim_product (stock_code);
CREATE INDEX IF NOT EXISTS idx_dim_date_value ON retail_dw.dim_date (date_value);


-- optional alerts table for persisted alert history
CREATE TABLE IF NOT EXISTS retail_dw.data_quality_alerts (
  alert_id serial PRIMARY KEY,
  alert_time timestamp without time zone DEFAULT now(),
  severity varchar(20),
  message text,
  metadata jsonb
);

-- ----------------------------------------------------------------------
-- Foreign key constraints (idempotent): ensure facts reference dims & versions
-- Use DO blocks to avoid errors if constraints already exist on an existing DB
-- ----------------------------------------------------------------------

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fact_sales_date') THEN
    EXECUTE 'ALTER TABLE retail_dw.fact_sales
             ADD CONSTRAINT fk_fact_sales_date
             FOREIGN KEY (date_key) REFERENCES retail_dw.dim_date (date_key)';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not add fk_fact_sales_date: %', SQLERRM;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fact_sales_customer') THEN
    EXECUTE 'ALTER TABLE retail_dw.fact_sales
             ADD CONSTRAINT fk_fact_sales_customer
             FOREIGN KEY (customer_key) REFERENCES retail_dw.dim_customer (customer_key) ON DELETE RESTRICT';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not add fk_fact_sales_customer: %', SQLERRM;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fact_sales_product') THEN
    EXECUTE 'ALTER TABLE retail_dw.fact_sales
             ADD CONSTRAINT fk_fact_sales_product
             FOREIGN KEY (product_key) REFERENCES retail_dw.dim_product (product_key) ON DELETE RESTRICT';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not add fk_fact_sales_product: %', SQLERRM;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fact_sales_version') THEN
    -- version_id is nullable; cascade on delete is not desired, set NULL instead
    EXECUTE 'ALTER TABLE retail_dw.fact_sales
             ADD CONSTRAINT fk_fact_sales_version
             FOREIGN KEY (version_id) REFERENCES retail_dw.data_versions (version_id) ON DELETE SET NULL';
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Could not add fk_fact_sales_version: %', SQLERRM;
END
$$;

INSERT INTO dim_transaction_type (transaction_type, effect_sign, description) VALUES
('SALE',               1,  'Normal sale (positive)'),
('RETURN',            -1,  'Customer return'),
('ADJUSTMENT_IN',      1,  'Stock added'),
('ADJUSTMENT_OUT',    -1,  'Stock removed'),
('DISCOUNT',          -1,  'Promotion or markdown'),
('DISCOUNT_REVERSAL',  1,  'Reversal of discount'),
('FEE',               -1,  'Cost fee'),
('FEE_REVERSAL',       1,  'Reversal of fee'),
('SHIPPING_CHARGE',    1,  'Customer paid postage'),
('SHIPPING_REFUND',   -1,  'Refunded postage'),
('VOUCHER_SALE',       0,  'Neutral'),
('VOUCHER_REDEMPTION',-1,  'Redeemed voucher'),
('DONATION',          -1,  'Charitable expense'),
('SERVICE',            1,  'Service income');

-- ----------------------------------------------------------------------
-- Trigger and audit table for fact_sales changes (audit history)
CREATE TABLE retail_dw.fact_sales_archive (
  archive_id   BIGSERIAL PRIMARY KEY,
  sales_key    BIGINT NOT NULL,
  operation    TEXT NOT NULL CHECK (operation IN ('UPDATE','DELETE')),
  version_id   INTEGER,                                  -- align types
  changed_at   TIMESTAMP NOT NULL DEFAULT now(),
  changed_by   TEXT,
  payload      JSONB NOT NULL,
  CONSTRAINT fk_fsa_version FOREIGN KEY (version_id)
    REFERENCES retail_dw.data_versions(version_id)
);

CREATE INDEX IF NOT EXISTS ix_fsa_sales_key  ON retail_dw.fact_sales_archive(sales_key);
CREATE INDEX IF NOT EXISTS ix_fsa_changed_at ON retail_dw.fact_sales_archive(changed_at);

-- 3) Recreate trigger function (no UUID casts)
CREATE OR REPLACE FUNCTION retail_dw.trg_fact_sales_audit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    INSERT INTO retail_dw.fact_sales_archive (sales_key, operation, version_id, changed_by, payload)
    VALUES (OLD.sales_key,
            'UPDATE',
            COALESCE(NEW.version_id, OLD.version_id),
            session_user,
            to_jsonb(OLD));
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO retail_dw.fact_sales_archive (sales_key, operation, version_id, changed_by, payload)
    VALUES (OLD.sales_key,
            'DELETE',
            OLD.version_id,
            session_user,
            to_jsonb(OLD));
    RETURN OLD;
  END IF;

  RETURN NULL;
END
$$;

-- 4) Reattach trigger
CREATE TRIGGER fact_sales_audit_trg
BEFORE UPDATE OR DELETE ON retail_dw.fact_sales
FOR EACH ROW
EXECUTE FUNCTION retail_dw.trg_fact_sales_audit();

