-- ============================================================
-- PROPWISE BUSINESS QUERIES
-- Author: Mohamed Alryah - abdalrhman.alryah@linux.com
-- ============================================================
-- 
-- CONNECTION DETAILS:
-- 
-- TRINO (Staging Data):
--   Host: localhost
--   Port: 8090
--   Database: hive
--   Username: trino
--   Password: (empty)
--   JDBC URL: jdbc:trino://localhost:8090/hive
-- 
-- POSTGRESQL DWH:
--   Host: localhost
--   Port: 5432
--   Database: propwise_dwh
--   Username: dwh_user
--   Password: dwh123
--   JDBC URL: jdbc:postgresql://localhost:5432/propwise_dwh
-- 
-- ============================================================

-- ============================================================
-- SECTION 1: STAGING DATA QUERIES (Trino - hive catalog)
-- ============================================================

-- 1.1 Total leads by status
SELECT 
    status_name,
    COUNT(*) as lead_count,
    ROUND(AVG(CAST(budget AS DOUBLE)), 2) as avg_budget
FROM hive.staging.processed_leads
GROUP BY status_name
ORDER BY lead_count DESC;

-- 1.2 Leads by source (marketing attribution)
SELECT 
    lead_source,
    COUNT(*) as lead_count,
    COUNT(CASE WHEN buyer = 'TRUE' THEN 1 END) as buyers,
    COUNT(CASE WHEN seller = 'TRUE' THEN 1 END) as sellers
FROM hive.staging.processed_leads
GROUP BY lead_source
ORDER BY lead_count DESC;

-- 1.3 Monthly lead trends
SELECT 
    YEAR(CAST(lead_created_at AS DATE)) as year,
    MONTH(CAST(lead_created_at AS DATE)) as month,
    COUNT(*) as new_leads
FROM hive.staging.processed_leads
WHERE lead_created_at IS NOT NULL
GROUP BY 
    YEAR(CAST(lead_created_at AS DATE)),
    MONTH(CAST(lead_created_at AS DATE))
ORDER BY year, month;

-- 1.4 Top campaigns by lead count
SELECT 
    campaign,
    COUNT(*) as leads,
    COUNT(DISTINCT location) as unique_locations
FROM hive.staging.processed_leads
WHERE campaign IS NOT NULL AND campaign != ''
GROUP BY campaign
ORDER BY leads DESC
LIMIT 10;

-- 1.5 Sales summary
SELECT 
    COUNT(*) as total_sales,
    SUM(CAST(sale_price AS DOUBLE)) as total_revenue,
    AVG(CAST(sale_price AS DOUBLE)) as avg_sale_price,
    MIN(CAST(sale_price AS DOUBLE)) as min_sale,
    MAX(CAST(sale_price AS DOUBLE)) as max_sale
FROM hive.staging.processed_sales
WHERE sale_price IS NOT NULL;

-- 1.6 Sales by category
SELECT 
    sales_category,
    COUNT(*) as sale_count,
    SUM(CAST(sale_price AS DOUBLE)) as total_value
FROM hive.staging.processed_sales
WHERE sales_category IS NOT NULL
GROUP BY sales_category
ORDER BY total_value DESC;

-- 1.7 Lead to Sale conversion rate
SELECT 
    l.lead_source,
    COUNT(DISTINCT l.id) as total_leads,
    COUNT(DISTINCT s.lead_id) as converted_leads,
    ROUND(COUNT(DISTINCT s.lead_id) * 100.0 / NULLIF(COUNT(DISTINCT l.id), 0), 2) as conversion_rate_pct
FROM hive.staging.processed_leads l
LEFT JOIN hive.staging.processed_sales s ON l.id = s.lead_id
GROUP BY l.lead_source
HAVING COUNT(DISTINCT l.id) > 10
ORDER BY conversion_rate_pct DESC;

-- 1.8 Agent performance
SELECT 
    user_id as agent_id,
    COUNT(*) as leads_handled,
    COUNT(CASE WHEN status_name LIKE '%Converted%' OR status_name LIKE '%Won%' THEN 1 END) as converted,
    ROUND(AVG(CAST(budget AS DOUBLE)), 2) as avg_budget,
    ROUND(COUNT(CASE WHEN status_name LIKE '%Converted%' OR status_name LIKE '%Won%' THEN 1 END) * 100.0 / COUNT(*), 2) as conversion_rate
FROM hive.staging.processed_leads
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY leads_handled DESC
LIMIT 20;

-- 1.9 Lead quality analysis (buyer vs seller)
SELECT 
    CASE 
        WHEN buyer = 'TRUE' AND seller = 'TRUE' THEN 'Both'
        WHEN buyer = 'TRUE' THEN 'Buyer'
        WHEN seller = 'TRUE' THEN 'Seller'
        ELSE 'Unknown'
    END as lead_type,
    COUNT(*) as count,
    ROUND(AVG(CAST(budget AS DOUBLE)), 2) as avg_budget
FROM hive.staging.processed_leads
GROUP BY 
    CASE 
        WHEN buyer = 'TRUE' AND seller = 'TRUE' THEN 'Both'
        WHEN buyer = 'TRUE' THEN 'Buyer'
        WHEN seller = 'TRUE' THEN 'Seller'
        ELSE 'Unknown'
    END
ORDER BY count DESC;

-- 1.10 Geographic distribution
SELECT 
    location,
    COUNT(*) as lead_count,
    COUNT(DISTINCT lead_source) as source_diversity
FROM hive.staging.processed_leads
WHERE location IS NOT NULL AND location != ''
GROUP BY location
ORDER BY lead_count DESC
LIMIT 15;

-- ============================================================
-- SECTION 2: DWH QUERIES (PostgreSQL - dwh catalog)
-- ============================================================

-- 2.1 Check dimension data
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL
SELECT 'dim_lead_status', COUNT(*) FROM dim_lead_status
UNION ALL
SELECT 'dim_lead_source', COUNT(*) FROM dim_lead_source
UNION ALL
SELECT 'dim_lead_type', COUNT(*) FROM dim_lead_type
UNION ALL
SELECT 'dim_agent', COUNT(*) FROM dim_agent
UNION ALL
SELECT 'dim_property_type', COUNT(*) FROM dim_property_type
UNION ALL
SELECT 'dim_area', COUNT(*) FROM dim_area
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM dim_campaign
UNION ALL
SELECT 'dim_sale_category', COUNT(*) FROM dim_sale_category
UNION ALL
SELECT 'fact_lead', COUNT(*) FROM fact_lead
UNION ALL
SELECT 'fact_sale', COUNT(*) FROM fact_sale;

-- 2.2 Date dimension sample
SELECT * FROM dim_date 
WHERE date_actual BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY date_actual
LIMIT 10;

-- 2.3 Lead fact with dimensions (when loaded)
SELECT 
    fl.lead_id,
    dd.date_actual as created_date,
    dls.status_name,
    dlsrc.source_name,
    da.agent_name,
    fl.budget
FROM fact_lead fl
LEFT JOIN dim_date dd ON fl.created_date_sk = dd.date_sk
LEFT JOIN dim_lead_status dls ON fl.lead_status_sk = dls.lead_status_sk
LEFT JOIN dim_lead_source dlsrc ON fl.lead_source_sk = dlsrc.lead_source_sk
LEFT JOIN dim_agent da ON fl.agent_sk = da.agent_sk
LIMIT 20;

-- 2.4 Sales fact with dimensions (when loaded)
SELECT 
    fs.sale_id,
    dd.date_actual as reservation_date,
    dpt.type_name as property_type,
    darea.area_name,
    dsc.category_name as sale_category,
    fs.unit_value,
    fs.actual_value
FROM fact_sale fs
LEFT JOIN dim_date dd ON fs.reservation_date_sk = dd.date_sk
LEFT JOIN dim_property_type dpt ON fs.property_type_sk = dpt.property_type_sk
LEFT JOIN dim_area darea ON fs.area_sk = darea.area_sk
LEFT JOIN dim_sale_category dsc ON fs.sale_category_sk = dsc.sale_category_sk
LIMIT 20;

-- 2.5 SCD-2 history check
SELECT 
    agent_id,
    agent_name,
    is_current,
    effective_from,
    effective_to
FROM dim_agent
WHERE agent_id IN (SELECT agent_id FROM dim_agent GROUP BY agent_id HAVING COUNT(*) > 1)
ORDER BY agent_id, effective_from;

-- ============================================================
-- SECTION 3: DATA QUALITY QUERIES
-- ============================================================

-- 3.1 Null analysis (Trino - staging)
SELECT 
    'leads' as dataset,
    COUNT(*) as total_rows,
    SUM(CASE WHEN budget IS NULL THEN 1 ELSE 0 END) as null_budget,
    SUM(CASE WHEN status_name IS NULL THEN 1 ELSE 0 END) as null_status,
    SUM(CASE WHEN lead_source IS NULL THEN 1 ELSE 0 END) as null_source
FROM hive.staging.processed_leads;

-- 3.2 Duplicate check (Trino - staging)
SELECT 
    id,
    COUNT(*) as occurrence
FROM hive.staging.processed_leads
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY occurrence DESC
LIMIT 10;

-- 3.3 ETL Load history (PostgreSQL DWH)
SELECT 
    load_id,
    table_name,
    load_type,
    records_loaded,
    load_status,
    load_timestamp,
    error_message
FROM etl_load_log
ORDER BY load_timestamp DESC
LIMIT 20;

-- ============================================================
-- END OF QUERIES
-- ============================================================
