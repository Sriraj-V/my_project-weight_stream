{{
  config(
    materialized = 'table',
    schema       = 'gold',
    file_format  = 'parquet'
  )
}}

/*
  dim_device — current snapshot of device attributes.
  SCD Type-2 history is managed by the snapshots/dim_device.sql snapshot.
  This view always shows the current (is_current = TRUE) record.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} AS device_sk,
    device_id,
    site_id,
    device_status,
    max_capacity_kg,
    calibration_state,
    updated_at,
    TRUE AS is_current
FROM (
    SELECT
        device_id,
        site_id,
        device_status,
        -- derive max_capacity from device_id naming convention SCL-SITE-NN
        CASE CAST(SPLIT_PART(device_id, '-', 3) AS INTEGER)
            WHEN 1 THEN 5000
            WHEN 2 THEN 10000
            WHEN 3 THEN 2000
            ELSE 0
        END AS max_capacity_kg,
        CASE device_status
            WHEN 'OK'        THEN 'CALIBRATED'
            WHEN 'CALIB_WARN' THEN 'DUE_SOON'
            WHEN 'FAULT'     THEN 'UNCALIBRATED'
        END AS calibration_state,
        MAX(silver_ts) AS updated_at
    FROM {{ ref('stg_weigh_readings') }}
    GROUP BY 1, 2, 3
)
