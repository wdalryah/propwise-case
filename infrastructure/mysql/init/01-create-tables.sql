-- Propwise Source Database - MySQL
-- Raw operational data (unchanged from source)
-- NO PRIMARY KEY - allows duplicates (raw data as-is)
-- ==========================================

USE propwise_source;

-- ===========================================
-- LEADS TABLE - Raw source data (no PK)
-- ===========================================
DROP TABLE IF EXISTS leads;
CREATE TABLE leads (
    id VARCHAR(50),
    date_of_last_request VARCHAR(100),
    buyer VARCHAR(20),
    seller VARCHAR(20),
    best_time_to_call VARCHAR(500),
    budget VARCHAR(100),
    created_at VARCHAR(100),
    updated_at VARCHAR(100),
    user_id VARCHAR(50),
    location VARCHAR(500),
    date_of_last_contact VARCHAR(100),
    status_name VARCHAR(200),
    commercial VARCHAR(20),
    merged VARCHAR(20),
    area_id VARCHAR(50),
    compound_id VARCHAR(50),
    developer_id VARCHAR(50),
    meeting_flag VARCHAR(20),
    do_not_call VARCHAR(20),
    lead_type_id VARCHAR(50),
    customer_id VARCHAR(50),
    method_of_contact VARCHAR(200),
    lead_source VARCHAR(200),
    campaign VARCHAR(1000),
    lead_type VARCHAR(100),
    INDEX idx_leads_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===========================================
-- SALES TABLE - Raw source data (no PK)
-- ===========================================
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
    id VARCHAR(50),
    lead_id VARCHAR(50),
    unit_value VARCHAR(100),
    unit_location VARCHAR(500),
    expected_value VARCHAR(100),
    actual_value VARCHAR(100),
    date_of_reservation VARCHAR(100),
    reservation_update_date VARCHAR(100),
    date_of_contraction VARCHAR(100),
    property_type_id VARCHAR(50),
    area_id VARCHAR(50),
    compound_id VARCHAR(50),
    sale_category VARCHAR(200),
    years_of_payment VARCHAR(50),
    property_type VARCHAR(200),
    INDEX idx_sales_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
