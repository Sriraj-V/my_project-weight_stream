-- =============================================================================
-- 02_iceberg_sink_bronze.sql
-- Land ALL events into Bronze (lossless, append-only).
-- Malformed / out-of-range events go to a dead-letter table (reject).
-- =============================================================================

USE CATALOG iceberg_catalog;

-- ── Bronze: main raw table ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.weigh_readings_raw (
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
    event_ts        STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH LOCAL TIME ZONE,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector'                = 'iceberg',
    'catalog-name'             = 'iceberg_catalog',
    'database-name'            = 'bronze',
    'table-name'               = 'weigh_readings_raw',
    'write.format.default'     = 'parquet',
    'write.target-file-size-bytes' = '134217728',
    'write.upsert.enabled'     = 'false'
);

-- ── Bronze: dead-letter / reject table ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.weigh_readings_reject (
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
    event_ts        STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH LOCAL TIME ZONE,
    reject_reason   STRING
) WITH (
    'connector'                = 'iceberg',
    'catalog-name'             = 'iceberg_catalog',
    'database-name'            = 'bronze',
    'table-name'               = 'weigh_readings_reject',
    'write.format.default'     = 'parquet'
);

-- ── Insert: valid events → bronze.weigh_readings_raw ─────────────────────────
INSERT INTO bronze.weigh_readings_raw
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
    event_ts,
    kafka_partition,
    kafka_offset,
    ingest_ts
FROM default_catalog.default_database.kafka_weigh_readings_raw
WHERE
    -- schema nullability guard
    event_id      IS NOT NULL
    AND device_id IS NOT NULL
    -- physical plausibility
    AND gross_weight  >= 0
    AND tare_weight   >= 0
    AND gross_weight  >= tare_weight
    -- known status codes
    AND device_status IN ('OK', 'CALIB_WARN', 'FAULT')
    -- known shifts
    AND shift         IN ('DAY', 'EVENING', 'NIGHT');

-- ── Insert: invalid events → dead-letter ─────────────────────────────────────
INSERT INTO bronze.weigh_readings_reject
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
    event_ts,
    kafka_partition,
    kafka_offset,
    ingest_ts,
    CASE
        WHEN event_id IS NULL OR device_id IS NULL THEN 'NULL_KEY_FIELD'
        WHEN gross_weight < 0 OR tare_weight < 0    THEN 'NEGATIVE_WEIGHT'
        WHEN gross_weight < tare_weight             THEN 'GROSS_LT_TARE'
        WHEN device_status NOT IN ('OK','CALIB_WARN','FAULT') THEN 'UNKNOWN_STATUS'
        WHEN shift NOT IN ('DAY','EVENING','NIGHT') THEN 'UNKNOWN_SHIFT'
        ELSE 'OTHER'
    END AS reject_reason
FROM default_catalog.default_database.kafka_weigh_readings_raw
WHERE
    event_id      IS NULL
    OR device_id  IS NULL
    OR gross_weight < 0
    OR tare_weight  < 0
    OR gross_weight < tare_weight
    OR device_status NOT IN ('OK', 'CALIB_WARN', 'FAULT')
    OR shift         NOT IN ('DAY', 'EVENING', 'NIGHT');
