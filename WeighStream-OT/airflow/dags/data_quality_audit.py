"""
data_quality_audit.py

Airflow DAG — hourly data-quality audit that queries Trino directly and fails
the DAG if any threshold is breached. Checks:

  - Silver row count (freshness)
  - Reject rate  (bronze rejects / total bronze events)
  - Null rate on critical Silver columns
  - Gold fact count vs Silver (reconciliation gap)
  - Freshness lag (max ingest_ts vs now)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

# ── Thresholds ─────────────────────────────────────────────────────────────────
MAX_REJECT_RATE   = 0.10   # fail if >10% of bronze events are rejects
MAX_NULL_RATE     = 0.01   # fail if >1% of Silver rows have null event_id
MAX_FRESHNESS_LAG = 600    # fail if latest Silver row > 10 min old (seconds)
MAX_GOLD_GAP_PCT  = 0.05   # fail if Gold is missing >5% of Silver rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_trino():
    """Return a trino.dbapi connection."""
    import trino  # pip install trino
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog="iceberg",
        schema="silver",
    )


def _query(sql: str) -> list[tuple]:
    conn = _get_trino()
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


# ── Audit tasks ───────────────────────────────────────────────────────────────

def check_reject_rate(**ctx):
    rows = _query("""
        SELECT
            (SELECT CAST(COUNT(*) AS DOUBLE) FROM bronze.weigh_readings_reject) /
            NULLIF(
                (SELECT CAST(COUNT(*) AS DOUBLE) FROM bronze.weigh_readings_raw) +
                (SELECT CAST(COUNT(*) AS DOUBLE) FROM bronze.weigh_readings_reject),
                0
            ) AS reject_rate
    """)
    rate = rows[0][0] or 0.0
    print(f"Reject rate: {rate:.2%}")
    if rate > MAX_REJECT_RATE:
        raise ValueError(f"Reject rate {rate:.2%} exceeds threshold {MAX_REJECT_RATE:.2%}")


def check_null_rate(**ctx):
    rows = _query("""
        SELECT
            CAST(COUNT_IF(event_id IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0)
        FROM silver.weigh_readings_clean
    """)
    rate = rows[0][0] or 0.0
    print(f"Silver null rate (event_id): {rate:.4%}")
    if rate > MAX_NULL_RATE:
        raise ValueError(f"Null rate {rate:.4%} exceeds threshold {MAX_NULL_RATE:.4%}")


def check_freshness(**ctx):
    import time
    rows = _query("""
        SELECT
            CAST(
                (CAST(now() AS DOUBLE) - MAX(CAST(ingest_ts AS DOUBLE))) / 1000
            AS DOUBLE)
        FROM silver.weigh_readings_clean
    """)
    lag_seconds = rows[0][0] or 9999
    print(f"Silver freshness lag: {lag_seconds:.0f}s")
    if lag_seconds > MAX_FRESHNESS_LAG:
        raise ValueError(
            f"Freshness lag {lag_seconds:.0f}s exceeds threshold {MAX_FRESHNESS_LAG}s"
        )


def check_gold_gap(**ctx):
    rows = _query("""
        SELECT
            silver_cnt,
            gold_cnt,
            CAST(silver_cnt - gold_cnt AS DOUBLE) / NULLIF(silver_cnt, 0) AS gap_pct
        FROM (
            SELECT
                (SELECT COUNT(*) FROM silver.weigh_readings_clean) AS silver_cnt,
                (SELECT COUNT(*) FROM gold.fact_weighments)         AS gold_cnt
        )
    """)
    silver, gold, gap = rows[0]
    print(f"Silver={silver}  Gold={gold}  Gap={gap:.2%}")
    if gap > MAX_GOLD_GAP_PCT:
        raise ValueError(
            f"Gold gap {gap:.2%} ({silver-gold} rows) exceeds threshold {MAX_GOLD_GAP_PCT:.2%}"
        )


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "weighstream",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_quality_audit",
    description="Hourly DQ audit: reject rate, null rate, freshness, Gold gap",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weighstream", "dq", "audit"],
) as dag:

    t_reject   = PythonOperator(task_id="check_reject_rate",   python_callable=check_reject_rate)
    t_null     = PythonOperator(task_id="check_null_rate",     python_callable=check_null_rate)
    t_fresh    = PythonOperator(task_id="check_freshness",     python_callable=check_freshness)
    t_gold_gap = PythonOperator(task_id="check_gold_gap",      python_callable=check_gold_gap)

    # all checks independent — run in parallel
    [t_reject, t_null, t_fresh, t_gold_gap]
