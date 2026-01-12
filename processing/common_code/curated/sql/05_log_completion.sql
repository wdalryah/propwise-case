-- ================================================
-- CURATED LAYER: Step 5 - Log ETL Completion
-- ================================================
-- Records the ETL load completion in the metadata table

INSERT INTO dwh.public.etl_load_log (
    table_name,
    load_type,
    batch_id,
    rows_inserted,
    status,
    end_time
)
VALUES 
(
    'fact_lead',
    'incremental',
    DATE_FORMAT(CURRENT_TIMESTAMP, '%Y%m%d_%H%i%s'),
    (SELECT COUNT(*) FROM dwh.public.fact_lead),
    'success',
    CURRENT_TIMESTAMP
),
(
    'fact_sale',
    'incremental',
    DATE_FORMAT(CURRENT_TIMESTAMP, '%Y%m%d_%H%i%s'),
    (SELECT COUNT(*) FROM dwh.public.fact_sale),
    'success',
    CURRENT_TIMESTAMP
);

