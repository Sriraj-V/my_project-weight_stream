{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

/*
  stg_weigh_readings — lightweight staging view on top of Silver.
  Casts / renames columns and excludes rows that failed DQ flags.
*/

SELECT
    event_id,
    device_id,
    site_id,
    material_code,
    CAST(gross_weight    AS DECIMAL(12, 3))  AS gross_weight_kg,
    CAST(tare_weight     AS DECIMAL(12, 3))  AS tare_weight_kg,
    CAST(net_weight      AS DECIMAL(12, 3))  AS net_weight_kg,
    unit_of_measure,
    device_status,
    operator_id,
    shift,
    CAST(event_ts        AS TIMESTAMP(6))    AS event_ts,
    CAST(ingest_ts       AS TIMESTAMP(6))    AS ingest_ts,
    dq_gross_ok,
    dq_net_matches,
    is_late,
    CAST(silver_ts       AS TIMESTAMP(6))    AS silver_ts
FROM {{ source('silver', 'weigh_readings_clean') }}
WHERE
    dq_gross_ok    = TRUE
    AND dq_net_matches = TRUE
