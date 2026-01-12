-- Propwise Quality Framework Schema
-- Data Quality Rules, Dimensions, Profiling & Governance
-- ==========================================

-- ===========================================
-- CREATE SCHEMAS
-- ===========================================

CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS profiling;
CREATE SCHEMA IF NOT EXISTS governance;

-- ===========================================
-- SCHEMA REGISTRY (Extended)
-- ===========================================

-- Schema field definitions (detailed column-level registry)
CREATE TABLE IF NOT EXISTS pipeline.schema_fields (
    field_id SERIAL PRIMARY KEY,
    schema_id INTEGER REFERENCES pipeline.schema_registry(schema_id),
    field_name VARCHAR(255) NOT NULL,
    field_type VARCHAR(50) NOT NULL,  -- string, integer, float, date, timestamp, boolean, json
    is_nullable BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_foreign_key BOOLEAN DEFAULT FALSE,
    default_value TEXT,
    description TEXT,
    ordinal_position INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Schema change history
CREATE TABLE IF NOT EXISTS pipeline.schema_changes (
    change_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    change_type VARCHAR(50) NOT NULL,  -- column_added, column_removed, type_changed, constraint_changed
    old_value JSONB,
    new_value JSONB,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP
);

-- ===========================================
-- QUALITY DIMENSIONS (Thresholds & Rules)
-- ===========================================

-- Quality dimensions define WHAT we measure
CREATE TABLE IF NOT EXISTS quality.dimensions (
    dimension_id SERIAL PRIMARY KEY,
    dimension_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    category VARCHAR(50),  -- completeness, accuracy, consistency, timeliness, validity, uniqueness
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quality thresholds per table/column
CREATE TABLE IF NOT EXISTS quality.thresholds (
    threshold_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),  -- NULL means table-level threshold
    dimension_id INTEGER REFERENCES quality.dimensions(dimension_id),
    threshold_type VARCHAR(50) NOT NULL,  -- min, max, equals, between, percentage
    threshold_value DECIMAL(10, 4),
    threshold_min DECIMAL(10, 4),
    threshold_max DECIMAL(10, 4),
    severity VARCHAR(20) DEFAULT 'warning',  -- info, warning, error, critical
    is_blocking BOOLEAN DEFAULT FALSE,  -- If TRUE, pipeline stops on violation
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- QUALITY RULES
-- ===========================================

-- Quality rules definitions
CREATE TABLE IF NOT EXISTS quality.rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL UNIQUE,
    rule_type VARCHAR(50) NOT NULL,  -- null_check, type_check, range_check, regex_check, custom_sql, referential
    description TEXT,
    dimension_id INTEGER REFERENCES quality.dimensions(dimension_id),
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),  -- NULL for table-level rules
    rule_definition JSONB NOT NULL,  -- Contains rule parameters
    severity VARCHAR(20) DEFAULT 'warning',
    is_blocking BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rule execution history
CREATE TABLE IF NOT EXISTS quality.rule_executions (
    execution_id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES quality.rules(rule_id),
    run_id INTEGER REFERENCES pipeline.pipeline_runs(run_id),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- passed, failed, error, skipped
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    pass_rate DECIMAL(5, 4),
    error_message TEXT,
    sample_failures JSONB  -- Sample of failed records for debugging
);

-- ===========================================
-- PROFILING
-- ===========================================

-- Table profiling results
CREATE TABLE IF NOT EXISTS profiling.table_profiles (
    profile_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    profile_date DATE NOT NULL,
    row_count BIGINT,
    column_count INTEGER,
    size_bytes BIGINT,
    profiling_duration_seconds DECIMAL(10, 2),
    profile_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name, profile_date)
);

-- Column profiling results
CREATE TABLE IF NOT EXISTS profiling.column_profiles (
    column_profile_id SERIAL PRIMARY KEY,
    profile_id INTEGER REFERENCES profiling.table_profiles(profile_id),
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(50),
    
    -- Completeness metrics
    null_count BIGINT,
    null_percentage DECIMAL(5, 4),
    non_null_count BIGINT,
    
    -- Uniqueness metrics
    distinct_count BIGINT,
    distinct_percentage DECIMAL(5, 4),
    duplicate_count BIGINT,
    
    -- Numeric statistics (for numeric columns)
    min_value DECIMAL(20, 6),
    max_value DECIMAL(20, 6),
    mean_value DECIMAL(20, 6),
    std_dev DECIMAL(20, 6),
    median_value DECIMAL(20, 6),
    
    -- String statistics (for string columns)
    min_length INTEGER,
    max_length INTEGER,
    avg_length DECIMAL(10, 2),
    
    -- Pattern analysis
    top_values JSONB,  -- Top N most frequent values
    pattern_frequency JSONB,  -- Regex pattern distribution
    
    -- Data quality flags
    has_nulls BOOLEAN,
    has_duplicates BOOLEAN,
    has_outliers BOOLEAN,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- GOVERNANCE
-- ===========================================

-- Data classification (PII, sensitive, etc.)
CREATE TABLE IF NOT EXISTS governance.data_classification (
    classification_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    classification_type VARCHAR(50) NOT NULL,  -- PII, PHI, financial, sensitive, public
    sensitivity_level INTEGER DEFAULT 1,  -- 1 (low) to 5 (critical)
    requires_encryption BOOLEAN DEFAULT FALSE,
    requires_masking BOOLEAN DEFAULT FALSE,
    retention_days INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name, column_name)
);

-- Data owners
CREATE TABLE IF NOT EXISTS governance.data_owners (
    owner_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    owner_name VARCHAR(255) NOT NULL,
    owner_email VARCHAR(255),
    steward_name VARCHAR(255),
    steward_email VARCHAR(255),
    department VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- OBSERVABILITY TABLES
-- ===========================================

-- Metrics store
CREATE TABLE IF NOT EXISTS quality.metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,  -- counter, gauge, histogram
    metric_value DECIMAL(20, 6),
    labels JSONB,  -- Key-value pairs for metric dimensions
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts
CREATE TABLE IF NOT EXISTS quality.alerts (
    alert_id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,  -- quality_violation, pipeline_failure, schema_drift, threshold_breach
    severity VARCHAR(20) NOT NULL,  -- info, warning, error, critical
    source VARCHAR(100) NOT NULL,  -- pipeline name or component
    title VARCHAR(255) NOT NULL,
    message TEXT,
    alert_metadata JSONB,
    status VARCHAR(20) DEFAULT 'open',  -- open, acknowledged, resolved
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Logs
CREATE TABLE IF NOT EXISTS quality.logs (
    log_id SERIAL PRIMARY KEY,
    log_level VARCHAR(20) NOT NULL,  -- DEBUG, INFO, WARNING, ERROR, CRITICAL
    logger_name VARCHAR(100),
    source VARCHAR(100),
    message TEXT NOT NULL,
    exception TEXT,
    log_metadata JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================
-- INDEXES
-- ===========================================

CREATE INDEX idx_schema_fields_schema ON pipeline.schema_fields(schema_id);
CREATE INDEX idx_schema_changes_table ON pipeline.schema_changes(table_name);
CREATE INDEX idx_thresholds_table ON quality.thresholds(table_name);
CREATE INDEX idx_rules_table ON quality.rules(table_name);
CREATE INDEX idx_rule_executions_rule ON quality.rule_executions(rule_id);
CREATE INDEX idx_rule_executions_run ON quality.rule_executions(run_id);
CREATE INDEX idx_table_profiles_table ON profiling.table_profiles(table_name);
CREATE INDEX idx_column_profiles_profile ON profiling.column_profiles(profile_id);
CREATE INDEX idx_metrics_name ON quality.metrics(metric_name);
CREATE INDEX idx_metrics_timestamp ON quality.metrics(timestamp);
CREATE INDEX idx_alerts_status ON quality.alerts(status);
CREATE INDEX idx_alerts_severity ON quality.alerts(severity);
CREATE INDEX idx_logs_level ON quality.logs(log_level);
CREATE INDEX idx_logs_timestamp ON quality.logs(timestamp);

-- ===========================================
-- INSERT DEFAULT QUALITY DIMENSIONS
-- ===========================================

INSERT INTO quality.dimensions (dimension_name, description, category) VALUES
('completeness', 'Measures the extent to which data is not missing', 'completeness'),
('accuracy', 'Measures correctness of data values', 'accuracy'),
('consistency', 'Measures uniformity of data across systems', 'consistency'),
('timeliness', 'Measures data freshness and availability', 'timeliness'),
('validity', 'Measures conformance to defined formats/rules', 'validity'),
('uniqueness', 'Measures absence of duplicates', 'uniqueness'),
('integrity', 'Measures referential integrity between datasets', 'integrity')
ON CONFLICT (dimension_name) DO NOTHING;

-- ===========================================
-- INSERT DEFAULT QUALITY RULES FOR LEADS
-- ===========================================

-- Leads table rules
INSERT INTO quality.rules (rule_name, rule_type, description, table_name, column_name, rule_definition, severity, is_blocking) VALUES
-- Completeness rules
('leads_id_not_null', 'null_check', 'Lead ID must not be null', 'leads', 'id', 
 '{"check": "not_null", "threshold": 1.0}', 'critical', TRUE),
 
('leads_status_not_null', 'null_check', 'Status must not be null', 'leads', 'status_name', 
 '{"check": "not_null", "threshold": 0.95}', 'warning', FALSE),

-- Validity rules  
('leads_budget_positive', 'range_check', 'Budget must be positive or null', 'leads', 'budget',
 '{"min": 0, "max": null, "allow_null": true}', 'warning', FALSE),

('leads_email_format', 'regex_check', 'Email must be valid format', 'leads', 'buyer',
 '{"pattern": ".*", "description": "Boolean TRUE/FALSE check"}', 'info', FALSE),

-- Type rules
('leads_no_json_in_location', 'type_check', 'Location must be plain text, no JSON', 'leads', 'location',
 '{"expected_type": "string", "disallow_json": true}', 'warning', FALSE),

-- Uniqueness rules
('leads_id_unique', 'uniqueness_check', 'Lead IDs should be unique', 'leads', 'id',
 '{"threshold": 0.99}', 'warning', FALSE),

-- Freshness rules
('leads_data_freshness', 'freshness_check', 'Data should not be older than 24 hours', 'leads', 'updated_at',
 '{"max_age_hours": 24}', 'warning', FALSE)
 
ON CONFLICT (rule_name) DO NOTHING;

-- Sales table rules
INSERT INTO quality.rules (rule_name, rule_type, description, table_name, column_name, rule_definition, severity, is_blocking) VALUES
('sales_lead_id_not_null', 'null_check', 'Sales must have lead reference', 'sales', 'lead_id',
 '{"check": "not_null", "threshold": 1.0}', 'critical', TRUE),

('sales_price_positive', 'range_check', 'Price must be positive', 'sales', 'price',
 '{"min": 0, "max": null, "allow_null": false}', 'error', TRUE),

('sales_referential_integrity', 'referential', 'Lead ID must exist in leads table', 'sales', 'lead_id',
 '{"reference_table": "leads", "reference_column": "id"}', 'error', FALSE)
 
ON CONFLICT (rule_name) DO NOTHING;

-- ===========================================
-- INSERT DEFAULT THRESHOLDS
-- ===========================================

INSERT INTO quality.thresholds (table_name, column_name, dimension_id, threshold_type, threshold_value, severity, is_blocking) VALUES
-- Completeness thresholds
('leads', 'id', (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'completeness'), 'percentage', 1.0000, 'critical', TRUE),
('leads', 'status_name', (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'completeness'), 'percentage', 0.9000, 'warning', FALSE),
('leads', 'budget', (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'completeness'), 'percentage', 0.8000, 'info', FALSE),

-- Uniqueness thresholds
('leads', 'id', (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'uniqueness'), 'percentage', 0.9900, 'warning', FALSE),
('sales', 'id', (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'uniqueness'), 'percentage', 0.9900, 'warning', FALSE),

-- Validity thresholds (for row counts)
('leads', NULL, (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'validity'), 'min', 100.0000, 'error', TRUE),
('sales', NULL, (SELECT dimension_id FROM quality.dimensions WHERE dimension_name = 'validity'), 'min', 50.0000, 'error', TRUE)

ON CONFLICT DO NOTHING;

-- ===========================================
-- GRANTS
-- ===========================================

GRANT ALL ON SCHEMA quality TO metadata_user;
GRANT ALL ON SCHEMA profiling TO metadata_user;
GRANT ALL ON SCHEMA governance TO metadata_user;
GRANT ALL ON ALL TABLES IN SCHEMA quality TO metadata_user;
GRANT ALL ON ALL TABLES IN SCHEMA profiling TO metadata_user;
GRANT ALL ON ALL TABLES IN SCHEMA governance TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA quality TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA profiling TO metadata_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA governance TO metadata_user;

