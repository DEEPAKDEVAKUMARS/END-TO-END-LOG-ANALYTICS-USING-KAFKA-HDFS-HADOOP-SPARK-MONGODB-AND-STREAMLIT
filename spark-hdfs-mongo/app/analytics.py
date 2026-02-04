import time
import pandas as pd
from datetime import datetime, timezone, date
from pymongo import MongoClient
import streamlit as st

MONGO_URI = "mongodb://127.0.0.1:27017"
DB_NAME = "bdproj"

@st.cache_data(ttl=5)
def load_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Pull the three aggregate collections you already populate
    hits = list(db.hits_by_hour.find({}, {"_id": 0}))
    top = list(db.top_urls.find({}, {"_id": 0}))
    status = list(db.status_counts.find({}, {"_id": 0}))

    # Normalize to DataFrames
    df_hits = pd.DataFrame(hits) if hits else pd.DataFrame(columns=["date","hour","hits","errors"])
    df_top = pd.DataFrame(top) if top else pd.DataFrame(columns=["date","url","hits"])
    df_status = pd.DataFrame(status) if status else pd.DataFrame(columns=["date","status","count"])

    # Convert dates
    for df in (df_hits, df_top, df_status):
        if "date" in df.columns and not df.empty:
            df["date"] = pd.to_datetime(df["date"])

    return df_hits, df_top, df_status

st.set_page_config(page_title="Log Analytics", layout="wide")
st.title("📊 Traffic Analytics — Spark ➜ MongoDB ➜ Streamlit")

auto = st.sidebar.checkbox("Auto-refresh every 5s", value=True)
if auto:
     st.query_params = {"_": str(time.time())}
df_hits, df_top, df_status = load_data()

# --- KPI row ---
col1, col2, col3 = st.columns(3)
total_hits = int(df_hits["hits"].sum()) if not df_hits.empty else 0
total_errors = int(df_hits["errors"].sum()) if not df_hits.empty else 0
unique_days = df_hits["date"].dt.date.nunique() if not df_hits.empty else 0

col1.metric("Total hits (all time)", total_hits)
col2.metric("Total 5xx errors (all time)", total_errors)
col3.metric("Days covered", unique_days)

st.markdown("---")

# --- Hits by hour (latest day) ---
if not df_hits.empty:
    latest_day = df_hits["date"].max().normalize()
    day_hits = df_hits[df_hits["date"] == latest_day].sort_values("hour")
    left, right = st.columns(2)

    with left:
        st.subheader("Hits by Hour (latest day)")
        st.dataframe(day_hits[["date","hour","hits","errors"]])
        st.bar_chart(day_hits.set_index("hour")[["hits"]])

    with right:
        st.subheader("Errors by Hour (latest day)")
        st.bar_chart(day_hits.set_index("hour")[["errors"]])
else:
    st.info("No hits data yet.")

# --- Status code distribution (latest day) ---
if not df_status.empty:
    latest_day = df_status["date"].max().normalize()
    day_status = df_status[df_status["date"] == latest_day].sort_values("status")
    st.subheader("HTTP Status Distribution (latest day)")
    st.dataframe(day_status[["date","status","count"]])
    st.bar_chart(day_status.set_index("status")[["count"]])
else:
    st.info("No status data yet.")

# --- Top URLs (latest day) ---
if not df_top.empty:
    latest_day = df_top["date"].max().normalize()
    day_top = df_top[df_top["date"] == latest_day].sort_values("hits", ascending=False).head(20)
    st.subheader("Top URLs (latest day)")
    st.dataframe(day_top[["date","url","hits"]])
    st.bar_chart(day_top.set_index("url")[["hits"]])
else:
    st.info("No top URL data yet.")

# Auto refresh
if auto:
    st.caption("Auto-refreshing every 5s…")
    time.sleep(5)
    st.rerun()
