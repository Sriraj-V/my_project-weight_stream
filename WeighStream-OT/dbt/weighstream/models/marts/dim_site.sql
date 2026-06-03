{{
  config(
    materialized = 'table',
    schema       = 'gold',
    file_format  = 'parquet'
  )
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY site_id) AS site_sk,
    site_id,
    CASE site_id
        WHEN 'SITE_A' THEN 'Charlotte Plant'
        WHEN 'SITE_B' THEN 'Atlanta Facility'
        WHEN 'SITE_C' THEN 'Houston Depot'
        WHEN 'SITE_D' THEN 'Nashville Hub'
        ELSE site_id
    END AS site_name,
    CASE site_id
        WHEN 'SITE_A' THEN 'Southeast'
        WHEN 'SITE_B' THEN 'Southeast'
        WHEN 'SITE_C' THEN 'South'
        WHEN 'SITE_D' THEN 'Southeast'
        ELSE 'Unknown'
    END AS region,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM (
    SELECT DISTINCT site_id
    FROM {{ ref('stg_weigh_readings') }}
)
