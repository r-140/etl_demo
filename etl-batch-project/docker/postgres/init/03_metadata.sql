-- ETL Metadata database schema
CREATE DATABASE IF NOT EXISTS etl_metadata;

\c etl_metadata;

-- Delta extraction metadata (watermarks)
CREATE TABLE IF NOT EXISTS etl_metadata (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    watermark_value VARCHAR(255),
    record_count BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'SUCCESS',
    extraction_time TIMESTAMP,
    additional_properties JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(customer_id, source_table, strategy_name)
);

CREATE INDEX IF NOT EXISTS idx_metadata_customer ON etl_metadata(customer_id);
CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON etl_metadata(customer_id, source_table, strategy_name);

-- Job execution tracking
CREATE TABLE IF NOT EXISTS etl_job_executions (
    id SERIAL PRIMARY KEY,
    execution_id UUID DEFAULT gen_random_uuid(),
    pipeline_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    stage VARCHAR(50),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL', 'SKIPPED')),
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    records_processed BIGINT DEFAULT 0,
    records_quarantined BIGINT DEFAULT 0,
    duration_ms BIGINT,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_executions_pipeline ON etl_job_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_executions_customer ON etl_job_executions(customer_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON etl_job_executions(status, start_time);

-- Quarantine records tracking
CREATE TABLE IF NOT EXISTS etl_quarantine_summary (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    pipeline_id VARCHAR(100),
    rule_name VARCHAR(100),
    violation_count BIGINT DEFAULT 0,
    quarantine_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_customer ON etl_quarantine_summary(customer_id, quarantine_date);
