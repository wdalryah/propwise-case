-- Propwise Data Warehouse Schema
-- ================================
-- Star Schema with SCD-2 Dimensions
-- Best Practices: Surrogate Keys, Traceability, Idempotency

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension (Standard Calendar)
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk SERIAL PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Lead Status Dimension
CREATE TABLE IF NOT EXISTS dim_lead_status (
    lead_status_sk SERIAL PRIMARY KEY,
    status_code VARCHAR(100) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    status_category VARCHAR(50),  -- New, Active, Converted, Lost
    is_active BOOLEAN DEFAULT TRUE,
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(status_code, effective_from)
);

-- Lead Source Dimension
CREATE TABLE IF NOT EXISTS dim_lead_source (
    lead_source_sk SERIAL PRIMARY KEY,
    source_code VARCHAR(100) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    source_category VARCHAR(50),  -- Digital, Referral, Offline, etc.
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_code, effective_from)
);

-- Lead Type Dimension
CREATE TABLE IF NOT EXISTS dim_lead_type (
    lead_type_sk SERIAL PRIMARY KEY,
    type_code VARCHAR(100) NOT NULL,
    type_name VARCHAR(100) NOT NULL,
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(type_code, effective_from)
);

-- Agent Dimension (Sales Agents)
CREATE TABLE IF NOT EXISTS dim_agent (
    agent_sk SERIAL PRIMARY KEY,
    agent_id VARCHAR(50) NOT NULL,  -- Natural/Business Key
    agent_name VARCHAR(200),
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(agent_id, effective_from)
);

-- Property Type Dimension
CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_sk SERIAL PRIMARY KEY,
    type_code VARCHAR(100) NOT NULL,
    type_name VARCHAR(100) NOT NULL,
    is_commercial BOOLEAN DEFAULT FALSE,
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(type_code, effective_from)
);

-- Area/Location Dimension
CREATE TABLE IF NOT EXISTS dim_area (
    area_sk SERIAL PRIMARY KEY,
    area_id VARCHAR(50) NOT NULL,  -- Natural Key
    area_name VARCHAR(200),
    compound_id VARCHAR(50),
    compound_name VARCHAR(200),
    developer_id VARCHAR(50),
    developer_name VARCHAR(200),
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(area_id, effective_from)
);

-- Campaign Dimension
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_sk SERIAL PRIMARY KEY,
    campaign_code VARCHAR(200) NOT NULL,
    campaign_name VARCHAR(200),
    campaign_type VARCHAR(100),
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(campaign_code, effective_from)
);

-- Sale Category Dimension
CREATE TABLE IF NOT EXISTS dim_sale_category (
    sale_category_sk SERIAL PRIMARY KEY,
    category_code VARCHAR(100) NOT NULL,
    category_name VARCHAR(100) NOT NULL,
    -- SCD-2 columns
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    -- Traceability
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(category_code, effective_from)
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact Lead (Grain: 1 row per lead)
CREATE TABLE IF NOT EXISTS fact_lead (
    lead_sk SERIAL PRIMARY KEY,
    -- Natural Key (Business Key)
    lead_id VARCHAR(50) NOT NULL,
    -- Dimension Foreign Keys (Surrogate Keys)
    created_date_sk INT REFERENCES dim_date(date_sk),
    updated_date_sk INT REFERENCES dim_date(date_sk),
    last_contact_date_sk INT REFERENCES dim_date(date_sk),
    lead_status_sk INT REFERENCES dim_lead_status(lead_status_sk),
    lead_source_sk INT REFERENCES dim_lead_source(lead_source_sk),
    lead_type_sk INT REFERENCES dim_lead_type(lead_type_sk),
    agent_sk INT REFERENCES dim_agent(agent_sk),
    area_sk INT REFERENCES dim_area(area_sk),
    campaign_sk INT REFERENCES dim_campaign(campaign_sk),
    -- Measures
    budget DECIMAL(15,2),
    -- Flags
    is_buyer BOOLEAN,
    is_seller BOOLEAN,
    is_commercial BOOLEAN,
    has_meeting BOOLEAN,
    do_not_call BOOLEAN,
    is_merged BOOLEAN,
    -- Degenerate Dimensions
    contact_method VARCHAR(100),
    best_time_to_call VARCHAR(100),
    location VARCHAR(500),
    customer_id VARCHAR(50),
    -- Traceability (Source Tracking)
    raw_gpk VARCHAR(200),  -- Global Primary Key from raw
    source_system VARCHAR(50) DEFAULT 'propwise',
    -- Load Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50),
    -- Versioning for idempotency
    source_updated_at TIMESTAMP,
    record_hash VARCHAR(64),
    -- Unique constraint for idempotency
    UNIQUE(lead_id)
);

-- Fact Sale (Grain: 1 row per sale/contract)
CREATE TABLE IF NOT EXISTS fact_sale (
    sale_sk SERIAL PRIMARY KEY,
    -- Natural Key (Business Key)
    sale_id VARCHAR(50) NOT NULL,
    -- Link to Lead
    lead_sk INT REFERENCES fact_lead(lead_sk),
    lead_id VARCHAR(50),  -- Degenerate for easy querying
    -- Dimension Foreign Keys (Surrogate Keys)
    reservation_date_sk INT REFERENCES dim_date(date_sk),
    contract_date_sk INT REFERENCES dim_date(date_sk),
    update_date_sk INT REFERENCES dim_date(date_sk),
    property_type_sk INT REFERENCES dim_property_type(property_type_sk),
    area_sk INT REFERENCES dim_area(area_sk),
    sale_category_sk INT REFERENCES dim_sale_category(sale_category_sk),
    -- Measures (Additive)
    unit_value DECIMAL(15,2),
    expected_value DECIMAL(15,2),
    actual_value DECIMAL(15,2),
    payment_years INT,
    -- Degenerate Dimensions
    unit_location VARCHAR(500),
    -- Traceability (Source Tracking)
    raw_gpk VARCHAR(200),
    source_system VARCHAR(50) DEFAULT 'propwise',
    -- Load Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50),
    -- Versioning for idempotency
    source_updated_at TIMESTAMP,
    record_hash VARCHAR(64),
    -- Unique constraint for idempotency
    UNIQUE(sale_id)
);

-- ============================================
-- ETL METADATA TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS etl_load_log (
    load_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    load_type VARCHAR(50) NOT NULL,  -- full, incremental
    batch_id VARCHAR(50),
    rows_inserted INT DEFAULT 0,
    rows_updated INT DEFAULT 0,
    rows_deleted INT DEFAULT 0,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',  -- running, success, failed
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX IF NOT EXISTS idx_fact_lead_created_date ON fact_lead(created_date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_lead_status ON fact_lead(lead_status_sk);
CREATE INDEX IF NOT EXISTS idx_fact_lead_source ON fact_lead(lead_source_sk);
CREATE INDEX IF NOT EXISTS idx_fact_lead_agent ON fact_lead(agent_sk);
CREATE INDEX IF NOT EXISTS idx_fact_lead_lead_id ON fact_lead(lead_id);

CREATE INDEX IF NOT EXISTS idx_fact_sale_reservation_date ON fact_sale(reservation_date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sale_contract_date ON fact_sale(contract_date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sale_lead ON fact_sale(lead_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sale_sale_id ON fact_sale(sale_id);

-- ============================================
-- POPULATE DATE DIMENSION (2015-2030)
-- ============================================

INSERT INTO dim_date (date_actual, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend)
SELECT 
    d::DATE as date_actual,
    EXTRACT(YEAR FROM d)::INT as year,
    EXTRACT(QUARTER FROM d)::INT as quarter,
    EXTRACT(MONTH FROM d)::INT as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(WEEK FROM d)::INT as week,
    EXTRACT(DAY FROM d)::INT as day_of_month,
    EXTRACT(DOW FROM d)::INT as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend
FROM generate_series('2015-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_actual) DO NOTHING;

-- ============================================
-- DEFAULT DIMENSION RECORDS (Unknown/N/A)
-- ============================================

INSERT INTO dim_lead_status (lead_status_sk, status_code, status_name, status_category) 
VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown')
ON CONFLICT DO NOTHING;

INSERT INTO dim_lead_source (lead_source_sk, source_code, source_name, source_category) 
VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown')
ON CONFLICT DO NOTHING;

INSERT INTO dim_lead_type (lead_type_sk, type_code, type_name) 
VALUES (-1, 'UNKNOWN', 'Unknown')
ON CONFLICT DO NOTHING;

INSERT INTO dim_agent (agent_sk, agent_id, agent_name) 
VALUES (-1, 'UNKNOWN', 'Unknown Agent')
ON CONFLICT DO NOTHING;

INSERT INTO dim_property_type (property_type_sk, type_code, type_name) 
VALUES (-1, 'UNKNOWN', 'Unknown')
ON CONFLICT DO NOTHING;

INSERT INTO dim_area (area_sk, area_id, area_name) 
VALUES (-1, 'UNKNOWN', 'Unknown Area')
ON CONFLICT DO NOTHING;

INSERT INTO dim_campaign (campaign_sk, campaign_code, campaign_name) 
VALUES (-1, 'UNKNOWN', 'Unknown Campaign')
ON CONFLICT DO NOTHING;

INSERT INTO dim_sale_category (sale_category_sk, category_code, category_name) 
VALUES (-1, 'UNKNOWN', 'Unknown')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dwh_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dwh_user;
