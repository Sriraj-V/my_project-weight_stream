{{
  config(
    materialized  = 'incremental',
    schema        = 'gold',
    unique_key    = ['site_id', 'window_start'],
    incremental_strategy = 'merge',
    file_format   = 'parquet'
  )
}}

/*
  agg_throughput_5m — readings-per-5-minute and net-weight totals per site.
  Used by the Streamlit throughput chart.
*/

SELECT
    site_id,
    date_trunc('minute', event_ts) - INTERVAL '1' SECOND *
        (CAST(minute(event_ts) AS INTEGER) % 5 * 60 + second(event_ts))  AS window_start,
    date_trunc('minute', event_ts) - INTERVAL '1' SECOND *
        (CAST(minute(event_ts) AS INTEGER) % 5 * 60 + second(event_ts))
        + INTERVAL '5' MINUTE                                              AS window_end,
    COUNT(*)                                  AS reading_count,
    SUM(net_weight_kg)                        AS total_net_weight_kg,
    AVG(net_weight_kg)                        AS avg_net_weight_kg,
    COUNT_IF(device_status = 'FAULT')         AS fault_count,
    COUNT_IF(is_late = TRUE)                  AS late_count,
    CURRENT_TIMESTAMP                         AS refreshed_at
FROM {{ ref('stg_weigh_readings') }}
{% if is_incremental() %}
WHERE silver_ts > (SELECT MAX(refreshed_at) FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3
