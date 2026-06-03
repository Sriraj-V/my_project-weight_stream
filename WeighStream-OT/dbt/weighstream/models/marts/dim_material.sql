{{
  config(
    materialized = 'table',
    schema       = 'gold',
    file_format  = 'parquet'
  )
}}

SELECT
    ROW_NUMBER() OVER (ORDER BY material_code) AS material_sk,
    material_code,
    CASE material_code
        WHEN 'STEEL_COIL'      THEN 'Ferrous Metal'
        WHEN 'ALUMINUM_BILLET' THEN 'Non-Ferrous Metal'
        WHEN 'COPPER_WIRE'     THEN 'Non-Ferrous Metal'
        WHEN 'PLASTIC_PELLET'  THEN 'Polymer'
        WHEN 'RUBBER_BLOCK'    THEN 'Elastomer'
        ELSE 'Unknown'
    END AS material_category,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM (
    SELECT DISTINCT material_code
    FROM {{ ref('stg_weigh_readings') }}
)
