-- ================================================
-- CURATED LAYER: Step 1 - Create Staging Views
-- ================================================
-- Creates external table views over MinIO staging parquet files
-- These views allow Trino to read the processed data from MinIO

-- Create schema for staging views (if using Hive connector)
-- CREATE SCHEMA IF NOT EXISTS hive.staging;

-- Note: For file-based Hive connector, we create external tables
-- pointing to the MinIO staging bucket parquet files

-- External table for processed leads
-- Trino reads directly from parquet files in MinIO
CREATE TABLE IF NOT EXISTS hive.default.processed_leads (
    op VARCHAR,
    extraction_time TIMESTAMP,
    gpk VARCHAR,
    env VARCHAR,
    region VARCHAR,
    lead_id VARCHAR,
    date_of_last_request TIMESTAMP,
    is_buyer BOOLEAN,
    is_seller BOOLEAN,
    best_time_to_call VARCHAR,
    budget DOUBLE,
    lead_created_at TIMESTAMP,
    lead_updated_at TIMESTAMP,
    agent_id VARCHAR,
    location VARCHAR,
    date_of_last_contact TIMESTAMP,
    lead_status VARCHAR,
    is_commercial BOOLEAN,
    is_merged BOOLEAN,
    area_id VARCHAR,
    compound_id VARCHAR,
    developer_id VARCHAR,
    has_meeting BOOLEAN,
    do_not_call BOOLEAN,
    lead_type_id VARCHAR,
    customer_id VARCHAR,
    contact_method VARCHAR,
    lead_source VARCHAR,
    campaign VARCHAR,
    lead_type VARCHAR,
    year INTEGER,
    month INTEGER,
    _batch_id VARCHAR,
    _processed_at TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://staging/processed_leads/'
);

-- External table for processed sales
CREATE TABLE IF NOT EXISTS hive.default.processed_sales (
    op VARCHAR,
    extraction_time TIMESTAMP,
    gpk VARCHAR,
    env VARCHAR,
    region VARCHAR,
    sale_id VARCHAR,
    lead_id VARCHAR,
    unit_value DOUBLE,
    unit_location VARCHAR,
    expected_value DOUBLE,
    actual_value DOUBLE,
    sale_date TIMESTAMP,
    reservation_update_date TIMESTAMP,
    contract_date TIMESTAMP,
    property_type_id VARCHAR,
    area_id VARCHAR,
    compound_id VARCHAR,
    sale_category VARCHAR,
    payment_years INTEGER,
    property_type VARCHAR,
    year INTEGER,
    month INTEGER,
    _batch_id VARCHAR,
    _processed_at TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://staging/processed_sales/'
);

