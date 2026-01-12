-- Propwise Metadata Database
-- Pipeline orchestration and tracking
-- ==========================================

-- ===========================================
-- CREATE SCHEMAS
-- ===========================================

CREATE SCHEMA IF NOT EXISTS pipeline;
CREATE SCHEMA IF NOT EXISTS lineage;

-- ===========================================
-- PIPELINE TRACKING TABLES
-- ===========================================

-- Pipeline definitions
CREATE TABLE IF NOT EXISTS pipeline.pipelines (
    pipeline_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    source_system VARCHAR(100),
    target_system VARCHAR(100),
    schedule VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline runs
CREATE TABLE IF NOT EXISTS pipeline.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipeline.pipelines(pipeline_id),
    dag_run_id VARCHAR(100),
    status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed', 'skipped'
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_read INTEGER,
    records_written INTEGER,
    records_rejected INTEGER,
    error_message TEXT,
    run_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Task runs (within a pipeline)
CREATE TABLE IF NOT EXISTS pipeline.task_runs (
    task_run_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline.pipeline_runs(run_id),
    task_name VARCHAR(100) NOT NULL,
    task_type VARCHAR(50), -- 'extract', 'transform', 'load', 'quality_check'
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER,
    error_message TEXT,
    task_metadata JSONB
);

-- ===========================================
-- SCHEMA REGISTRY
-- ===========================================

-- Schema versions for source tables
CREATE TABLE IF NOT EXISTS pipeline.schema_registry (
    schema_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL,
    schema_definition JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    UNIQUE(table_name, schema_version)
);

-- ===========================================
-- DATA LINEAGE
-- ===========================================

-- Data assets
CREATE TABLE IF NOT EXISTS lineage.data_assets (
    asset_id SERIAL PRIMARY KEY,
    asset_name VARCHAR(255) NOT NULL,
    asset_type VARCHAR(50), -- 'table', 'file', 'bucket'
    location VARCHAR(500),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Lineage relationships
CREATE TABLE IF NOT EXISTS lineage.lineage_edges (
    edge_id SERIAL PRIMARY KEY,
    source_asset_id INTEGER REFERENCES lineage.data_assets(asset_id),
    target_asset_id INTEGER REFERENCES lineage.data_assets(asset_id),
    transformation_type VARCHAR(100),
    pipeline_id INTEGER REFERENCES pipeline.pipelines(pipeline_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- INDEXES
-- ===========================================

CREATE INDEX idx_pipeline_runs_status ON pipeline.pipeline_runs(status);
CREATE INDEX idx_pipeline_runs_start ON pipeline.pipeline_runs(start_time);
CREATE INDEX idx_task_runs_run_id ON pipeline.task_runs(run_id);
CREATE INDEX idx_schema_registry_table ON pipeline.schema_registry(table_name);

-- ===========================================
-- INSERT DEFAULT PIPELINE DEFINITIONS
-- ===========================================

INSERT INTO pipeline.pipelines (pipeline_name, description, source_system, target_system, schedule) VALUES
('propwise_leads_etl', 'ETL pipeline for leads data', 'MySQL', 'PostgreSQL DWH', '@daily'),
('propwise_sales_etl', 'ETL pipeline for sales data', 'MySQL', 'PostgreSQL DWH', '@daily'),
('propwise_datalake_ingestion', 'Raw data ingestion to MinIO', 'MySQL', 'MinIO', '@daily')
ON CONFLICT (pipeline_name) DO NOTHING;

-- ===========================================
-- GRANTS
-- ===========================================

GRANT ALL ON SCHEMA pipeline TO metadata_user;
GRANT ALL ON SCHEMA lineage TO metadata_user;
GRANT ALL ON ALL TABLES IN SCHEMA pipeline TO metadata_user;
GRANT ALL ON ALL TABLES IN SCHEMA lineage TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pipeline TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA lineage TO metadata_user;

