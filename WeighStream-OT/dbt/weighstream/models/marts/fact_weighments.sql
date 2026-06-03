{{
  config(
    materialized  = 'incremental',
    schema        = 'gold',
    unique_key    = 'event_id',
    incremental_strategy = 'merge',
    file_format   = 'parquet'
  )
}}

/*
  fact_weighments — grain: one row per validated weigh event.
  Joined to current dimension surrogate keys.
*/

WITH stg AS (
    SELECT * FROM {{ ref('stg_weigh_readings') }}
    {% if is_incremental() %}
    WHERE silver_ts > (SELECT MAX(silver_ts) FROM {{ this }})
    {% endif %}
),

dim_dev AS (
    SELECT device_id, device_sk
    FROM {{ ref('dim_device') }}
    WHERE is_current = TRUE
),

dim_mat AS (SELECT material_code, material_sk FROM {{ ref('dim_material') }}),
dim_sit AS (SELECT site_id,       site_sk       FROM {{ ref('dim_site') }})

SELECT
    stg.event_id,
    dim_dev.device_sk,
    dim_mat.material_sk,
    dim_sit.site_sk,
    stg.gross_weight_kg,
    stg.tare_weight_kg,
    stg.net_weight_kg,
    stg.unit_of_measure,
    stg.device_status,
    stg.operator_id,
    stg.shift,
    stg.is_late,
    stg.event_ts,
    stg.ingest_ts,
    stg.silver_ts,
    CURRENT_TIMESTAMP AS gold_loaded_at
FROM stg
LEFT JOIN dim_dev ON stg.device_id       = dim_dev.device_id
LEFT JOIN dim_mat ON stg.material_code   = dim_mat.material_code
LEFT JOIN dim_sit ON stg.site_id         = dim_sit.site_id
