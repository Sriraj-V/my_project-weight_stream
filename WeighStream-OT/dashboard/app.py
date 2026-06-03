"""
WeighStream-OT — Streamlit Monitoring Dashboard

Reads from Gold via Trino. Auto-refreshes every 30 seconds.
"""

import os
import time

import pandas as pd
import streamlit as st
import trino

# ── Config ────────────────────────────────────────────────────────────────────
TRINO_HOST    = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA", "gold")
REFRESH_SECS  = 30

st.set_page_config(
    page_title="WeighStream-OT",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Trino connection ──────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="streamlit",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def query(sql: str) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql(sql, conn)


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("⚖️ WeighStream-OT")
st.sidebar.caption("Real-time OT weighing analytics")
auto_refresh = st.sidebar.toggle("Auto-refresh (30 s)", value=True)
site_filter = st.sidebar.multiselect(
    "Sites",
    options=["SITE_A", "SITE_B", "SITE_C", "SITE_D"],
    default=["SITE_A", "SITE_B", "SITE_C", "SITE_D"],
)
site_in = "', '".join(site_filter) if site_filter else "SITE_A"

# ── KPI cards ─────────────────────────────────────────────────────────────────
st.title("WeighStream-OT — Live Monitor")

kpi_sql = f"""
SELECT
    COUNT(*)                               AS total_readings,
    SUM(net_weight_kg)                     AS total_net_kg,
    AVG(net_weight_kg)                     AS avg_net_kg,
    COUNT_IF(device_status = 'FAULT')      AS fault_count,
    COUNT_IF(is_late = TRUE)               AS late_count,
    DATE_DIFF('second',
        MAX(ingest_ts), NOW())             AS freshness_lag_s
FROM iceberg.gold.fact_weighments f
JOIN iceberg.gold.dim_site s ON f.site_sk = s.site_sk
WHERE s.site_id IN ('{site_in}')
"""

try:
    kpis = query(kpi_sql).iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Readings",    f"{int(kpis['total_readings']):,}")
    c2.metric("Total Net (kg)",    f"{kpis['total_net_kg']:,.0f}")
    c3.metric("Avg Net (kg)",      f"{kpis['avg_net_kg']:,.1f}")
    c4.metric("Fault Readings",    int(kpis['fault_count']))
    c5.metric("Late Arrivals",     int(kpis['late_count']))
    c6.metric("Freshness Lag (s)", f"{kpis['freshness_lag_s']:.0f}")
except Exception as e:
    st.warning(f"KPI query failed — is Gold populated? ({e})")

st.divider()

# ── Throughput over time ───────────────────────────────────────────────────────
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Readings / 5 min")
    tput_sql = f"""
    SELECT
        window_start,
        site_id,
        reading_count
    FROM iceberg.gold.agg_throughput_5m
    WHERE site_id IN ('{site_in}')
    ORDER BY window_start DESC
    LIMIT 288
    """
    try:
        tput = query(tput_sql)
        if not tput.empty:
            tput["window_start"] = pd.to_datetime(tput["window_start"])
            pivot = tput.pivot(index="window_start", columns="site_id", values="reading_count").fillna(0)
            st.line_chart(pivot)
        else:
            st.info("No throughput data yet.")
    except Exception as e:
        st.error(str(e))

with col_right:
    st.subheader("🏭 Net Weight by Site (today)")
    daily_sql = f"""
    SELECT site_id, SUM(total_net_weight_kg) AS total_kg
    FROM iceberg.gold.agg_site_daily
    WHERE event_date = CURRENT_DATE
      AND site_id IN ('{site_in}')
    GROUP BY 1
    ORDER BY 2 DESC
    """
    try:
        daily = query(daily_sql)
        if not daily.empty:
            st.bar_chart(daily.set_index("site_id")["total_kg"])
        else:
            st.info("No daily data yet.")
    except Exception as e:
        st.error(str(e))

# ── Net-weight distribution ───────────────────────────────────────────────────
st.subheader("📊 Net Weight Distribution (last 500 readings)")
dist_sql = f"""
SELECT net_weight_kg, material_code
FROM iceberg.gold.fact_weighments f
JOIN iceberg.gold.dim_site  s ON f.site_sk  = s.site_sk
JOIN iceberg.gold.dim_material m ON f.material_sk = m.material_sk
WHERE s.site_id IN ('{site_in}')
ORDER BY f.event_ts DESC
LIMIT 500
"""
try:
    dist = query(dist_sql)
    if not dist.empty:
        st.scatter_chart(dist, x="material_code", y="net_weight_kg", color="material_code")
    else:
        st.info("No distribution data yet.")
except Exception as e:
    st.error(str(e))

# ── Device status breakdown ───────────────────────────────────────────────────
st.subheader("🔧 Device Status")
dev_sql = f"""
SELECT d.device_id, d.site_id, d.device_status, d.calibration_state,
       d.max_capacity_kg, d.updated_at
FROM iceberg.gold.dim_device d
WHERE d.site_id IN ('{site_in}')
  AND d.is_current = TRUE
ORDER BY d.site_id, d.device_id
"""
try:
    dev = query(dev_sql)
    if not dev.empty:
        def _color(row):
            return ["background-color: #ffcccc"] * len(row) if row["device_status"] == "FAULT" \
                else ["background-color: #fff3cc"] * len(row) if row["device_status"] == "CALIB_WARN" \
                else [""] * len(row)
        st.dataframe(dev.style.apply(_color, axis=1), use_container_width=True)
    else:
        st.info("No device data yet.")
except Exception as e:
    st.error(str(e))

# ── SCD2 device history inspector ────────────────────────────────────────────
st.subheader("🕐 SCD2 Device History")
device_ids_sql = "SELECT DISTINCT device_id FROM iceberg.gold.dim_device ORDER BY 1"
try:
    device_ids = query(device_ids_sql)["device_id"].tolist()
    selected_device = st.selectbox("Select device", options=device_ids)
    if selected_device:
        hist_sql = f"""
        SELECT device_id, device_status, calibration_state,
               dbt_valid_from AS valid_from, dbt_valid_to AS valid_to,
               CASE WHEN dbt_valid_to IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END AS record_type
        FROM iceberg.gold.dim_device_snapshot
        WHERE device_id = '{selected_device}'
        ORDER BY valid_from DESC
        """
        try:
            hist = query(hist_sql)
            st.dataframe(hist, use_container_width=True)
        except Exception:
            st.info("Snapshot table not populated yet — run `make dbt-snapshot`.")
except Exception as e:
    st.error(str(e))

# ── DQ / reject metrics ───────────────────────────────────────────────────────
st.subheader("🚨 Data Quality — Reject Rate")
dq_sql = """
SELECT
    (SELECT COUNT(*) FROM iceberg.bronze.weigh_readings_raw)    AS bronze_ok,
    (SELECT COUNT(*) FROM iceberg.bronze.weigh_readings_reject) AS bronze_reject
"""
try:
    dq = query(dq_sql).iloc[0]
    total = dq["bronze_ok"] + dq["bronze_reject"]
    reject_pct = dq["bronze_reject"] / total * 100 if total else 0
    st.progress(min(int(reject_pct), 100), text=f"Reject rate: {reject_pct:.2f}%")
    dq_cols = st.columns(3)
    dq_cols[0].metric("Bronze OK",     f"{int(dq['bronze_ok']):,}")
    dq_cols[1].metric("Bronze Reject", f"{int(dq['bronze_reject']):,}")
    dq_cols[2].metric("Reject %",      f"{reject_pct:.2f}%")
except Exception as e:
    st.error(str(e))

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(REFRESH_SECS)
    st.rerun()
