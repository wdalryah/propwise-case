-- ================================================
-- CURATED LAYER: Step 3 - Load Fact Lead
-- ================================================
-- Loads fact_lead table with dimension surrogate keys
-- Grain: 1 row per lead (latest version)
-- Idempotent: Uses MERGE pattern (INSERT ... ON CONFLICT UPDATE)

-- Note: PostgreSQL via Trino doesn't support MERGE directly
-- We use a staging approach: truncate-insert or upsert pattern

-- For idempotency, we delete existing records that will be updated
-- then insert all records from staging
DELETE FROM dwh.public.fact_lead
WHERE lead_id IN (
    SELECT DISTINCT TRIM(lead_id)
    FROM hive.default.processed_leads
    WHERE lead_id IS NOT NULL AND TRIM(lead_id) != ''
);

-- Insert fact records with dimension lookups
INSERT INTO dwh.public.fact_lead (
    lead_id,
    created_date_sk,
    updated_date_sk,
    last_contact_date_sk,
    lead_status_sk,
    lead_source_sk,
    lead_type_sk,
    agent_sk,
    area_sk,
    campaign_sk,
    budget,
    is_buyer,
    is_seller,
    is_commercial,
    has_meeting,
    do_not_call,
    is_merged,
    contact_method,
    best_time_to_call,
    location,
    customer_id,
    raw_gpk,
    source_system,
    batch_id,
    source_updated_at,
    record_hash
)
SELECT
    TRIM(l.lead_id) as lead_id,
    -- Date dimension lookups
    d_created.date_sk as created_date_sk,
    d_updated.date_sk as updated_date_sk,
    d_contact.date_sk as last_contact_date_sk,
    -- Dimension surrogate keys (with UNKNOWN fallback)
    COALESCE(dim_status.lead_status_sk, -1) as lead_status_sk,
    COALESCE(dim_source.lead_source_sk, -1) as lead_source_sk,
    COALESCE(dim_type.lead_type_sk, -1) as lead_type_sk,
    COALESCE(dim_agent.agent_sk, -1) as agent_sk,
    COALESCE(dim_area.area_sk, -1) as area_sk,
    COALESCE(dim_campaign.campaign_sk, -1) as campaign_sk,
    -- Measures
    l.budget,
    -- Flags
    l.is_buyer,
    l.is_seller,
    l.is_commercial,
    l.has_meeting,
    l.do_not_call,
    l.is_merged,
    -- Degenerate dimensions
    l.contact_method,
    l.best_time_to_call,
    l.location,
    l.customer_id,
    -- Traceability
    l.gpk as raw_gpk,
    'propwise' as source_system,
    l._batch_id as batch_id,
    l.lead_updated_at as source_updated_at,
    -- Record hash for change detection (simplified)
    MD5(CONCAT(
        COALESCE(l.lead_status, ''), '|',
        COALESCE(l.lead_source, ''), '|',
        COALESCE(l.agent_id, ''), '|',
        COALESCE(CAST(l.budget AS VARCHAR), ''), '|',
        COALESCE(l.location, '')
    )) as record_hash
FROM hive.default.processed_leads l
-- Date dimension joins
LEFT JOIN dwh.public.dim_date d_created 
    ON CAST(l.lead_created_at AS DATE) = d_created.date_actual
LEFT JOIN dwh.public.dim_date d_updated 
    ON CAST(l.lead_updated_at AS DATE) = d_updated.date_actual
LEFT JOIN dwh.public.dim_date d_contact 
    ON CAST(l.date_of_last_contact AS DATE) = d_contact.date_actual
-- Dimension joins (current records only)
LEFT JOIN dwh.public.dim_lead_status dim_status 
    ON TRIM(l.lead_status) = dim_status.status_code 
    AND dim_status.is_current = TRUE
LEFT JOIN dwh.public.dim_lead_source dim_source 
    ON TRIM(l.lead_source) = dim_source.source_code 
    AND dim_source.is_current = TRUE
LEFT JOIN dwh.public.dim_lead_type dim_type 
    ON TRIM(l.lead_type) = dim_type.type_code 
    AND dim_type.is_current = TRUE
LEFT JOIN dwh.public.dim_agent dim_agent 
    ON TRIM(l.agent_id) = dim_agent.agent_id 
    AND dim_agent.is_current = TRUE
LEFT JOIN dwh.public.dim_area dim_area 
    ON TRIM(l.area_id) = dim_area.area_id 
    AND dim_area.is_current = TRUE
LEFT JOIN dwh.public.dim_campaign dim_campaign 
    ON TRIM(l.campaign) = dim_campaign.campaign_code 
    AND dim_campaign.is_current = TRUE
WHERE l.lead_id IS NOT NULL 
  AND TRIM(l.lead_id) != ''
  AND l.op != 'D';  -- Exclude deleted records

