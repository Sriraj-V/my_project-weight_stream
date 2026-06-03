-- =============================================================================
-- 01_kafka_source.sql
-- Declare the Kafka source table (typed JSON → Flink row).
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default'    = '2';

-- ── Kafka connector JARs (pre-installed in image via flink/lib) ───────────────
-- flink-sql-connector-kafka-*.jar
-- flink-sql-connector-hive-*.jar  (for Iceberg catalog bridge)
-- iceberg-flink-runtime-*.jar

-- ── Iceberg REST catalog registration ────────────────────────────────────────
CREATE CATALOG iceberg_catalog WITH (
  'type'                    = 'iceberg',
  'catalog-type'            = 'rest',
  'uri'                     = 'http://iceberg-rest:8181',
  'warehouse'               = 's3://warehouse/',
  'io-impl'                 = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'             = 'http://minio:9000',
  's3.path-style-access'    = 'true',
  's3.access-key-id'        = 'minioadmin',
  's3.secret-access-key'    = 'minioadmin'
);

USE CATALOG iceberg_catalog;

-- ── Raw Kafka source ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_weigh_readings_raw (
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
    -- Kafka metadata
    kafka_partition INT          METADATA FROM 'partition',
    kafka_offset    BIGINT       METADATA FROM 'offset',
    ingest_ts       TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR ingest_ts AS ingest_ts - INTERVAL '30' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'weigh.readings.raw',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-weighstream',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
);
