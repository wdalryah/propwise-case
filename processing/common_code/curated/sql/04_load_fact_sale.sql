-- ================================================
-- CURATED LAYER: Step 4 - Load Fact Sale
-- ================================================
-- Loads fact_sale table with dimension surrogate keys
-- Grain: 1 row per sale/reservation
-- Idempotent: Delete-insert pattern

-- Delete existing records that will be refreshed
DELETE FROM dwh.public.fact_sale
WHERE sale_id IN (
    SELECT DISTINCT TRIM(sale_id)
    FROM hive.default.processed_sales
    WHERE sale_id IS NOT NULL AND TRIM(sale_id) != ''
);

-- Insert fact records with dimension lookups
INSERT INTO dwh.public.fact_sale (
    sale_id,
    lead_sk,
    lead_id,
    reservation_date_sk,
    contract_date_sk,
    update_date_sk,
    property_type_sk,
    area_sk,
    sale_category_sk,
    unit_value,
    expected_value,
    actual_value,
    payment_years,
    unit_location,
    raw_gpk,
    source_system,
    batch_id,
    source_updated_at,
    record_hash
)
SELECT
    TRIM(s.sale_id) as sale_id,
    -- Link to fact_lead (if exists)
    fl.lead_sk as lead_sk,
    TRIM(s.lead_id) as lead_id,
    -- Date dimension lookups
    d_reservation.date_sk as reservation_date_sk,
    d_contract.date_sk as contract_date_sk,
    d_update.date_sk as update_date_sk,
    -- Dimension surrogate keys (with UNKNOWN fallback)
    COALESCE(dim_prop.property_type_sk, -1) as property_type_sk,
    COALESCE(dim_area.area_sk, -1) as area_sk,
    COALESCE(dim_cat.sale_category_sk, -1) as sale_category_sk,
    -- Measures (additive)
    s.unit_value,
    s.expected_value,
    s.actual_value,
    s.payment_years,
    -- Degenerate dimension
    s.unit_location,
    -- Traceability
    s.gpk as raw_gpk,
    'propwise' as source_system,
    s._batch_id as batch_id,
    s.reservation_update_date as source_updated_at,
    -- Record hash for change detection
    MD5(CONCAT(
        COALESCE(s.sale_category, ''), '|',
        COALESCE(CAST(s.unit_value AS VARCHAR), ''), '|',
        COALESCE(CAST(s.actual_value AS VARCHAR), ''), '|',
        COALESCE(s.property_type, '')
    )) as record_hash
FROM hive.default.processed_sales s
-- Link to fact_lead for referential integrity
LEFT JOIN dwh.public.fact_lead fl 
    ON TRIM(s.lead_id) = fl.lead_id
-- Date dimension joins
LEFT JOIN dwh.public.dim_date d_reservation 
    ON CAST(s.sale_date AS DATE) = d_reservation.date_actual
LEFT JOIN dwh.public.dim_date d_contract 
    ON CAST(s.contract_date AS DATE) = d_contract.date_actual
LEFT JOIN dwh.public.dim_date d_update 
    ON CAST(s.reservation_update_date AS DATE) = d_update.date_actual
-- Dimension joins (current records only)
LEFT JOIN dwh.public.dim_property_type dim_prop 
    ON TRIM(s.property_type) = dim_prop.type_code 
    AND dim_prop.is_current = TRUE
LEFT JOIN dwh.public.dim_area dim_area 
    ON TRIM(s.area_id) = dim_area.area_id 
    AND dim_area.is_current = TRUE
LEFT JOIN dwh.public.dim_sale_category dim_cat 
    ON TRIM(s.sale_category) = dim_cat.category_code 
    AND dim_cat.is_current = TRUE
WHERE s.sale_id IS NOT NULL 
  AND TRIM(s.sale_id) != ''
  AND s.op != 'D';  -- Exclude deleted records

