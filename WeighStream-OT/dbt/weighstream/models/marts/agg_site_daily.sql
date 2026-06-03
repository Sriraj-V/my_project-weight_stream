{{
  config(
    materialized  = 'incremental',
    schema        = 'gold',
    unique_key    = ['site_id', 'event_date'],
    incremental_strategy = 'merge',
    file_format   = 'parquet'
  )
}}

SELECT
    site_id,
    CAST(event_ts AS DATE)          AS event_date,
    COUNT(*)                        AS total_readings,
    SUM(net_weight_kg)              AS total_net_weight_kg,
    AVG(net_weight_kg)              AS avg_net_weight_kg,
    MAX(net_weight_kg)              AS max_net_weight_kg,
    COUNT(DISTINCT device_id)       AS active_devices,
    COUNT(DISTINCT material_code)   AS distinct_materials,
    COUNT_IF(device_status='FAULT') AS fault_readings,
    COUNT_IF(is_late = TRUE)        AS late_readings,
    CURRENT_TIMESTAMP               AS refreshed_at
FROM {{ ref('stg_weigh_readings') }}
{% if is_incremental() %}
WHERE silver_ts > (SELECT MAX(refreshed_at) FROM {{ this }})
{% endif %}
GROUP BY 1, 2
