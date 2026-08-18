import os
import textwrap
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession
from streamlit_autorefresh import st_autorefresh


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Cambridge Police Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = "/mnt/d/AmitTrivediProj"

ANALYTICS_PATH = os.path.join(
    PROJECT_ROOT,
    "sparkModule",
    "output",
    "analytics",
)

BY_TYPE_PATH = os.path.join(ANALYTICS_PATH, "by_type")
BY_SUBTYPE_PATH = os.path.join(ANALYTICS_PATH, "by_subtype")
BY_LOCATION_PATH = os.path.join(ANALYTICS_PATH, "by_location")
BY_HOUR_PATH = os.path.join(ANALYTICS_PATH, "by_hour")
BY_MONTH_PATH = os.path.join(ANALYTICS_PATH, "by_month")

AUTO_REFRESH_INTERVAL_MS = 60 * 1000  # 60 seconds


# ============================================================
# HTML RENDER HELPER
# ============================================================
# IMPORTANT: Markdown treats any line indented 4+ spaces as a
# code block, which caused raw HTML to display as literal text.
# textwrap.dedent() strips that leading indentation before the
# string reaches Streamlit's markdown/HTML renderer.

def render_html(html: str) -> None:
    st.markdown(textwrap.dedent(html), unsafe_allow_html=True)


# ============================================================
# CUSTOM CSS
# ============================================================

render_html(
    """
    <style>

    .stApp {
        background:
            radial-gradient(
                circle at top right,
                rgba(30, 64, 175, 0.15),
                transparent 35%
            ),
            #080d18;
        color: #f8fafc;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1450px;
    }

    .dashboard-header {
        background: linear-gradient(135deg, #111827, #172554);
        border: 1px solid rgba(148, 163, 184, 0.18);
        border-radius: 20px;
        padding: 32px 38px;
        margin-bottom: 28px;
        box-shadow: 0 15px 40px rgba(0, 0, 0, 0.35);
    }

    .dashboard-title {
        font-size: 42px;
        font-weight: 800;
        letter-spacing: -1px;
        color: #f8fafc;
        margin-bottom: 8px;
    }

    .dashboard-subtitle {
        font-size: 16px;
        color: #94a3b8;
    }

    .status-row {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-top: 18px;
        flex-wrap: wrap;
    }

    .status-pill {
        display: inline-block;
        padding: 7px 14px;
        border-radius: 999px;
        background: rgba(34, 197, 94, 0.12);
        border: 1px solid rgba(34, 197, 94, 0.3);
        color: #4ade80;
        font-size: 13px;
        font-weight: 600;
    }

    .status-pill.stale {
        background: rgba(248, 113, 113, 0.12);
        border: 1px solid rgba(248, 113, 113, 0.3);
        color: #f87171;
    }

    .meta-text {
        color: #64748b;
        font-size: 12.5px;
    }

    .kpi-card {
        background: linear-gradient(145deg, #111827, #0f172a);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 18px;
        padding: 24px;
        min-height: 150px;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease;
        overflow-wrap: anywhere;
    }

    .kpi-card:hover {
        transform: translateY(-3px);
    }

    .kpi-label {
        color: #94a3b8;
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 12px;
    }

    .kpi-value {
        color: #f8fafc;
        font-size: 32px;
        font-weight: 800;
        line-height: 1.1;
    }

    .kpi-description {
        color: #64748b;
        font-size: 12px;
        margin-top: 10px;
    }

    .section-title {
        font-size: 22px;
        font-weight: 700;
        color: #f8fafc;
        margin-top: 28px;
        margin-bottom: 14px;
    }

    .section-subtitle {
        color: #64748b;
        font-size: 13px;
        margin-bottom: 18px;
    }

    [data-testid="stSidebar"] {
        background: #0b1120;
        border-right: 1px solid rgba(148, 163, 184, 0.12);
    }

    [data-testid="stMetric"] {
        background: #111827;
        border: 1px solid rgba(148, 163, 184, 0.12);
        padding: 15px;
        border-radius: 14px;
    }

    #MainMenu {
        visibility: hidden;
    }

    footer {
        visibility: hidden;
    }

    </style>
    """
)


# ============================================================
# SPARK SESSION (singleton — never recreated on refresh)
# ============================================================

@st.cache_resource
def get_spark():
    return (
        SparkSession
        .builder
        .appName("CambridgePoliceDashboard")
        .master("local[*]")
        .getOrCreate()
    )


spark = get_spark()


# ============================================================
# HELPERS
# ============================================================

def read_parquet(path):
    if not os.path.exists(path):
        return pd.DataFrame()

    df = spark.read.parquet(path)
    return df.toPandas()


def find_count_column(df):
    possible_columns = [
        "total_records",
        "count",
        "Count",
        "total",
        "Total",
        "incident_count",
        "record_count",
    ]

    for column in possible_columns:
        if column in df.columns:
            return column

    return None


def total_from_df(df):
    if df.empty:
        return 0

    count_column = find_count_column(df)

    if count_column:
        return int(
            pd.to_numeric(df[count_column], errors="coerce")
            .fillna(0)
            .sum()
        )

    return len(df)


def find_dimension_column(df):
    if df.empty:
        return None

    count_column = find_count_column(df)

    for column in df.columns:
        if column != count_column:
            return column

    return df.columns[0]


def get_data_version() -> float:
    """
    Returns the latest modification time across every file in the
    analytics output tree. Used as a cache key so load_data() only
    re-reads Parquet when Airflow/PySpark has actually written new
    output, instead of on every auto-refresh tick.
    """
    latest = 0.0

    if not os.path.exists(ANALYTICS_PATH):
        return latest

    for root, _dirs, files in os.walk(ANALYTICS_PATH):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                mtime = os.path.getmtime(fpath)
                if mtime > latest:
                    latest = mtime
            except OSError:
                continue

    return latest


# ============================================================
# AUTO-REFRESH
# ============================================================
# Triggers a script rerun every AUTO_REFRESH_INTERVAL_MS.
# Requires: pip install streamlit-autorefresh --break-system-packages

st_autorefresh(interval=AUTO_REFRESH_INTERVAL_MS, key="pipeline_autorefresh")


# ============================================================
# LOAD DATA
# ============================================================
# data_version changes only when Parquet files change, so the
# cache is reused between refresh ticks when nothing new landed.

@st.cache_data(ttl=300, show_spinner=False)
def load_data(_version: float):
    by_type = read_parquet(BY_TYPE_PATH)
    by_subtype = read_parquet(BY_SUBTYPE_PATH)
    by_location = read_parquet(BY_LOCATION_PATH)
    by_hour = read_parquet(BY_HOUR_PATH)
    by_month = read_parquet(BY_MONTH_PATH)

    return (by_type, by_subtype, by_location, by_hour, by_month)


data_version = get_data_version()

(
    by_type,
    by_subtype,
    by_location,
    by_hour,
    by_month,
) = load_data(data_version)

data_available = data_version > 0 and not by_type.empty


# ============================================================
# HEADER
# ============================================================

status_class = "status-pill" if data_available else "status-pill stale"
status_text = "● PIPELINE DATA AVAILABLE" if data_available else "● NO DATA DETECTED"

render_html(
    f"""
    <div class="dashboard-header">
        <div class="dashboard-title">🚔 Cambridge Police Intelligence</div>
        <div class="dashboard-subtitle">
            Operational analytics platform powered by PySpark + Apache Airflow
        </div>
        <div class="status-row">
            <div class="{status_class}">{status_text}</div>
        </div>
    </div>
    """
)


# ============================================================
# LAST UPDATED / DATA STATUS
# ============================================================

now_str = datetime.now().strftime("%d %b %Y • %H:%M:%S")

if data_version > 0:
    data_updated_str = datetime.fromtimestamp(data_version).strftime(
        "%d %b %Y • %H:%M:%S"
    )
else:
    data_updated_str = "No pipeline output found yet"

meta_col1, meta_col2, meta_col3 = st.columns(3)

with meta_col1:
    render_html(f'<div class="meta-text">Last dashboard data check: {now_str}</div>')

with meta_col2:
    render_html(f'<div class="meta-text">Data last updated: {data_updated_str}</div>')

with meta_col3:
    render_html(
        '<div class="meta-text">Pipeline: PySpark + Airflow '
        f'&nbsp;•&nbsp; Data status: {"● LIVE / AVAILABLE" if data_available else "● WAITING"}'
        '</div>'
    )


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_records = total_from_df(by_type)

total_locations = len(by_location) if not by_location.empty else 0

arrests = 0

if not by_type.empty:
    type_column = find_dimension_column(by_type)
    count_column = find_count_column(by_type)

    if type_column:
        temp = by_type.copy()

        temp["_type_clean"] = (
            temp[type_column].astype(str).str.lower().str.strip()
        )

        arrest_mask = temp["_type_clean"].str.contains("arrest", na=False)

        if count_column:
            arrests = int(
                pd.to_numeric(
                    temp.loc[arrest_mask, count_column], errors="coerce"
                )
                .fillna(0)
                .sum()
            )
        else:
            arrests = int(arrest_mask.sum())


# ============================================================
# KPI ROW
# ============================================================

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    render_html(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Records</div>
            <div class="kpi-value">{total_records:,}</div>
            <div class="kpi-description">Processed police records</div>
        </div>
        """
    )

with kpi2:
    render_html(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Arrests</div>
            <div class="kpi-value">{arrests:,}</div>
            <div class="kpi-description">Arrest-related records</div>
        </div>
        """
    )

with kpi3:
    render_html(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Locations</div>
            <div class="kpi-value">{total_locations:,}</div>
            <div class="kpi-description">Active reporting locations</div>
        </div>
        """
    )

with kpi4:
    render_html(
        """
        <div class="kpi-card">
            <div class="kpi-label">Data Engine</div>
            <div class="kpi-value">PySpark</div>
            <div class="kpi-description">Distributed processing layer</div>
        </div>
        """
    )


# ============================================================
# TYPE ANALYSIS
# ============================================================

render_html('<div class="section-title">Incident Intelligence</div>')
render_html(
    '<div class="section-subtitle">'
    "Distribution of police records by incident type"
    "</div>"
)

if not by_type.empty:
    type_column = find_dimension_column(by_type)
    count_column = find_count_column(by_type)
    chart_data = by_type.copy()

    if count_column:
        chart_data[count_column] = pd.to_numeric(
            chart_data[count_column], errors="coerce"
        ).fillna(0)

        chart_data = chart_data.sort_values(count_column, ascending=False)

        fig = px.bar(
            chart_data.head(15),
            x=count_column,
            y=type_column,
            orientation="h",
            text=count_column,
        )

        fig.update_layout(
            template="plotly_dark",
            height=450,
            xaxis_title="Records",
            yaxis_title="Incident Type",
            margin=dict(l=10, r=10, t=30, b=10),
        )

        fig.update_traces(textposition="outside")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Could not detect a count column in by_type data.")
else:
    st.info("No incident type data available yet.")


# ============================================================
# TWO COLUMN ANALYTICS
# ============================================================

left, right = st.columns(2)

with left:
    render_html('<div class="section-title">📍 Geographic Intelligence</div>')

    if not by_location.empty:
        location_column = find_dimension_column(by_location)
        count_column = find_count_column(by_location)
        location_data = by_location.copy()

        if count_column:
            location_data[count_column] = pd.to_numeric(
                location_data[count_column], errors="coerce"
            ).fillna(0)

            location_data = location_data.sort_values(
                count_column, ascending=False
            )

            fig = px.bar(
                location_data.head(10),
                x=count_column,
                y=location_column,
                orientation="h",
                text=count_column,
            )

            fig.update_layout(
                template="plotly_dark",
                height=420,
                xaxis_title="Records",
                yaxis_title="Location",
                margin=dict(l=10, r=10, t=20, b=10),
            )

            fig.update_traces(textposition="outside")

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Could not detect a count column in by_location data.")
    else:
        st.info("No location data available yet.")

with right:
    render_html('<div class="section-title">⏱ Temporal Intelligence</div>')

    if not by_hour.empty:
        hour_column = find_dimension_column(by_hour)
        count_column = find_count_column(by_hour)
        hour_data = by_hour.copy()

        if count_column:
            hour_data[count_column] = pd.to_numeric(
                hour_data[count_column], errors="coerce"
            ).fillna(0)

            try:
                hour_data = hour_data.sort_values(hour_column)
            except Exception:
                pass

            fig = px.line(
                hour_data,
                x=hour_column,
                y=count_column,
                markers=True,
            )

            fig.update_layout(
                template="plotly_dark",
                height=420,
                xaxis_title="Hour",
                yaxis_title="Records",
                margin=dict(l=10, r=10, t=20, b=10),
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Could not detect a count column in by_hour data.")
    else:
        st.info("No hourly data available yet.")


# ============================================================
# MONTHLY TREND
# ============================================================

render_html('<div class="section-title">📈 Monthly Activity Trend</div>')

if not by_month.empty:
    count_column = find_count_column(by_month)
    month_data = by_month.copy()

    if count_column:
        month_data[count_column] = pd.to_numeric(
            month_data[count_column], errors="coerce"
        ).fillna(0)

        # by_month has separate year/month columns rather than one
        # combined dimension — build a proper chronological label.
        if "year" in month_data.columns and "month" in month_data.columns:
            month_data["period"] = (
                month_data["year"].astype(int).astype(str)
                + "-"
                + month_data["month"].astype(int).astype(str).str.zfill(2)
            )
            month_data = month_data.sort_values(["year", "month"])
            month_column = "period"
        else:
            month_column = find_dimension_column(by_month)

        fig = px.area(month_data, x=month_column, y=count_column)

        fig.update_layout(
            template="plotly_dark",
            height=400,
            xaxis_title="Month",
            yaxis_title="Records",
            margin=dict(l=10, r=10, t=20, b=10),
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Could not detect a count column in by_month data.")
else:
    st.info("No monthly trend data available yet.")


# ============================================================
# SUBTYPE TABLE
# ============================================================

with st.expander("🔎 View Detailed Incident Subtypes"):
    if not by_subtype.empty:
        st.dataframe(by_subtype, use_container_width=True, hide_index=True)
    else:
        st.info("No subtype data available yet.")


# ============================================================
# PIPELINE INFORMATION
# ============================================================

render_html('<div class="section-title">⚙️ Data Pipeline</div>')

pipeline_col1, pipeline_col2, pipeline_col3 = st.columns(3)

with pipeline_col1:
    st.info(
        "📥 **Ingestion**\n\n"
        "Incremental API ingestion with checkpointing."
    )

with pipeline_col2:
    st.info(
        "⚡ **Processing**\n\n"
        "PySpark cleaning, validation, deduplication, "
        "windowing and aggregation."
    )

with pipeline_col3:
    st.info(
        "⏰ **Orchestration**\n\n"
        "Apache Airflow executes the pipeline hourly."
    )


# ============================================================
# FOOTER
# ============================================================

render_html(
    """
    <div style="text-align:center; color:#475569; padding-top:35px; font-size:12px;">
        Cambridge Police Data Engineering Platform
        • PySpark • Apache Airflow • Streamlit
    </div>
    """
)
