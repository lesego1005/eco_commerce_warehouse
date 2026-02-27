import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- Pull credentials from environment variables securely ---
db_user = os.getenv("ECO_DB_USER", "postgres")
db_password = os.getenv("ECO_DB_PASSWORD", "") # No hardcoded fallback!
db_host = os.getenv("ECO_DB_HOST", "127.0.0.1")
db_port = os.getenv("ECO_DB_PORT", "6432")
db_name = os.getenv("ECO_DB_NAME", "eco_warehouse")

# Database Connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

st.set_page_config(page_title="Pipeline Monitor", layout="wide")
st.title("🚀 Eco-Warehouse Pipeline Monitor")

# Load Data from our Phase 8 View
try:
    df = pd.read_sql("SELECT * FROM v_pipeline_health", engine)
except Exception as e:
    st.error(f"Database Error: {e}")
    df = pd.DataFrame()

if df.empty:
    st.warning("No pipeline data found. Please run ingest.sh to generate logs.")
else:
    # Top Level Metrics with Null Safety
    col1, col2, col3 = st.columns(3)
    
    latest_status = df['ingestion_status'].iloc[0]
    # Use 0 if the value is None (happens during active 'RUNNING' status)
    files_moved = df['files_moved'].iloc[0] if pd.notna(df['files_moved'].iloc[0]) else 0
    total_rows = df['total_rows'].iloc[0] if pd.notna(df['total_rows'].iloc[0]) else 0

    col1.metric("Latest Status", latest_status)
    col2.metric("Files Processed", int(files_moved))
    col3.metric("Total Rows", int(total_rows))

    # Charts - Filter out incomplete runs for cleaner visuals
    chart_df = df[df['ingestion_status'] != 'RUNNING'].copy()
    
    if not chart_df.empty:
        st.subheader("Data Volume Trends")
        st.line_chart(chart_df.set_index('start_time')['total_rows'])

        st.subheader("Data Quality Issues (Nulls/Duplicates)")
        st.bar_chart(chart_df.set_index('start_time')[['null_counts', 'duplicate_counts']])
    else:
        st.info("Charts will populate once the first run completes.")

    # Detailed Log Table
    st.subheader("Recent Run Details")
    st.dataframe(df)