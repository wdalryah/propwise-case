-- ================================================
-- CURATED LAYER: Step 2 - Load Dimensions
-- ================================================
-- Loads dimension tables into PostgreSQL DWH from staging
-- Implements SCD-2 logic with MERGE/UPSERT pattern

-- =============================================
-- dim_lead_status: Load unique lead statuses
-- =============================================
INSERT INTO dwh.public.dim_lead_status (status_code, status_name, is_current)
SELECT DISTINCT
    TRIM(lead_status) as status_code,
    TRIM(lead_status) as status_name,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE lead_status IS NOT NULL 
  AND TRIM(lead_status) != ''
  AND TRIM(lead_status) NOT IN (
      SELECT status_code FROM dwh.public.dim_lead_status WHERE is_current = TRUE
  );

-- =============================================
-- dim_lead_source: Load unique lead sources
-- =============================================
INSERT INTO dwh.public.dim_lead_source (source_code, source_name, is_current)
SELECT DISTINCT
    TRIM(lead_source) as source_code,
    TRIM(lead_source) as source_name,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE lead_source IS NOT NULL 
  AND TRIM(lead_source) != ''
  AND TRIM(lead_source) NOT IN (
      SELECT source_code FROM dwh.public.dim_lead_source WHERE is_current = TRUE
  );

-- =============================================
-- dim_lead_type: Load unique lead types
-- =============================================
INSERT INTO dwh.public.dim_lead_type (type_code, type_name, is_current)
SELECT DISTINCT
    TRIM(lead_type) as type_code,
    TRIM(lead_type) as type_name,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE lead_type IS NOT NULL 
  AND TRIM(lead_type) != ''
  AND TRIM(lead_type) NOT IN (
      SELECT type_code FROM dwh.public.dim_lead_type WHERE is_current = TRUE
  );

-- =============================================
-- dim_agent: Load unique agents
-- =============================================
INSERT INTO dwh.public.dim_agent (agent_id, agent_name, is_current)
SELECT DISTINCT
    TRIM(agent_id) as agent_id,
    CONCAT('Agent ', TRIM(agent_id)) as agent_name,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE agent_id IS NOT NULL 
  AND TRIM(agent_id) != ''
  AND TRIM(agent_id) NOT IN (
      SELECT agent_id FROM dwh.public.dim_agent WHERE is_current = TRUE
  );

-- =============================================
-- dim_area: Load unique areas from leads & sales
-- =============================================
INSERT INTO dwh.public.dim_area (area_id, area_name, compound_id, developer_id, is_current)
SELECT DISTINCT
    TRIM(area_id) as area_id,
    CONCAT('Area ', TRIM(area_id)) as area_name,
    TRIM(compound_id) as compound_id,
    TRIM(developer_id) as developer_id,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE area_id IS NOT NULL 
  AND TRIM(area_id) != ''
  AND TRIM(area_id) NOT IN (
      SELECT area_id FROM dwh.public.dim_area WHERE is_current = TRUE
  );

-- Also add areas from sales that might not exist in leads
INSERT INTO dwh.public.dim_area (area_id, area_name, compound_id, is_current)
SELECT DISTINCT
    TRIM(s.area_id) as area_id,
    CONCAT('Area ', TRIM(s.area_id)) as area_name,
    TRIM(s.compound_id) as compound_id,
    TRUE as is_current
FROM hive.default.processed_sales s
WHERE s.area_id IS NOT NULL 
  AND TRIM(s.area_id) != ''
  AND TRIM(s.area_id) NOT IN (
      SELECT area_id FROM dwh.public.dim_area WHERE is_current = TRUE
  );

-- =============================================
-- dim_campaign: Load unique campaigns
-- =============================================
INSERT INTO dwh.public.dim_campaign (campaign_code, campaign_name, is_current)
SELECT DISTINCT
    TRIM(campaign) as campaign_code,
    TRIM(campaign) as campaign_name,
    TRUE as is_current
FROM hive.default.processed_leads
WHERE campaign IS NOT NULL 
  AND TRIM(campaign) != ''
  AND TRIM(campaign) NOT IN (
      SELECT campaign_code FROM dwh.public.dim_campaign WHERE is_current = TRUE
  );

-- =============================================
-- dim_property_type: Load unique property types
-- =============================================
INSERT INTO dwh.public.dim_property_type (type_code, type_name, is_current)
SELECT DISTINCT
    TRIM(property_type) as type_code,
    TRIM(property_type) as type_name,
    TRUE as is_current
FROM hive.default.processed_sales
WHERE property_type IS NOT NULL 
  AND TRIM(property_type) != ''
  AND TRIM(property_type) NOT IN (
      SELECT type_code FROM dwh.public.dim_property_type WHERE is_current = TRUE
  );

-- =============================================
-- dim_sale_category: Load unique sale categories
-- =============================================
INSERT INTO dwh.public.dim_sale_category (category_code, category_name, is_current)
SELECT DISTINCT
    TRIM(sale_category) as category_code,
    TRIM(sale_category) as category_name,
    TRUE as is_current
FROM hive.default.processed_sales
WHERE sale_category IS NOT NULL 
  AND TRIM(sale_category) != ''
  AND TRIM(sale_category) NOT IN (
      SELECT category_code FROM dwh.public.dim_sale_category WHERE is_current = TRUE
  );

