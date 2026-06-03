-- =============================================================================
-- 03_silver_dedup_enrich.sql
-- Deduplicate Bronze → produce exactly-one Silver row per event_id.
-- Uses ROW_NUMBER() over a processing-time window keyed on event_id,
-- keeping only rn = 1 (first occurrence).
-- =============================================================================

USE CATALOG iceberg_catalog;

-- ── Silver: clean, deduplicated table ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.weigh_readings_clean (
    event_id        STRING,
    device_id       STRING,
    site_id         STRING,
    material_code   STRING,
    gross_weight    DOUBLE,
    tare_weight     DOUBLE,
    net_weight      DOUBLE,
    unit_of_measure STRING,
    device_status   STRING,
    operator_id     STRING,
    shift           STRING,
    event_ts        TIMESTAMP(6) WITH LOCAL TIME ZONE,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH LOCAL TIME ZONE,
    dq_gross_ok     BOOLEAN,    -- gross_weight in plausible range
    dq_net_matches  BOOLEAN,    -- net_weight ≈ gross - tare
    is_late         BOOLEAN,    -- arrived > 5 min after event_ts
    silver_ts       TIMESTAMP(6) WITH LOCAL TIME ZONE,
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (site_id, device_status)
WITH (
    'connector'            = 'iceberg',
    'catalog-name'         = 'iceberg_catalog',
    'database-name'        = 'silver',
    'table-name'           = 'weigh_readings_clean',
    'write.format.default' = 'parquet',
    'write.upsert.enabled' = 'true'    -- idempotent upsert on PK
);

-- ── Deduplicate + enrich ──────────────────────────────────────────────────────
INSERT INTO silver.weigh_readings_clean
SELECT
    event_id,
    device_id,
    site_id,
    material_code,
    gross_weight,
    tare_weight,
    net_weight,
    unit_of_measure,
    device_status,
    operator_id,
    shift,
    TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_ts) * 1000, 3)     AS event_ts,
    kafka_partition,
    kafka_offset,
    ingest_ts,
    -- data-quality flags
    gross_weight BETWEEN 1 AND 15000                                    AS dq_gross_ok,
    ABS(net_weight - (gross_weight - tare_weight)) < 0.01               AS dq_net_matches,
    ingest_ts > TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_ts) * 1000, 3)
                + INTERVAL '5' MINUTE                                    AS is_late,
    CURRENT_TIMESTAMP                                                    AS silver_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingest_ts ASC
        ) AS rn
    FROM bronze.weigh_readings_raw
)
WHERE rn = 1;
