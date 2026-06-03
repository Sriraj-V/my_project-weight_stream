{% snapshot dim_device_snapshot %}

{{
  config(
    target_schema  = 'gold',
    target_database = 'iceberg',
    unique_key     = 'device_id',
    strategy       = 'timestamp',
    updated_at     = 'updated_at',
    invalidate_hard_deletes = True
  )
}}

/*
  SCD Type-2 snapshot of device attributes.
  dbt adds: dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to.
  Rename in fact_weighments join to: valid_from, valid_to, is_current.
*/

SELECT
    device_id,
    site_id,
    device_status,
    CASE CAST(SPLIT_PART(device_id, '-', 3) AS INTEGER)
        WHEN 1 THEN 5000
        WHEN 2 THEN 10000
        WHEN 3 THEN 2000
        ELSE 0
    END AS max_capacity_kg,
    CASE device_status
        WHEN 'OK'         THEN 'CALIBRATED'
        WHEN 'CALIB_WARN' THEN 'DUE_SOON'
        WHEN 'FAULT'      THEN 'UNCALIBRATED'
    END AS calibration_state,
    MAX(silver_ts) AS updated_at
FROM {{ ref('stg_weigh_readings') }}
GROUP BY 1, 2, 3

{% endsnapshot %}
