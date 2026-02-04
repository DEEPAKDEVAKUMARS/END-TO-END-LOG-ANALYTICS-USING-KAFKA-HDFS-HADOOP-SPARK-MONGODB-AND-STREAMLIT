import time
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import streamlit as st

MONGO_URI = "mongodb://127.0.0.1:27017"
DB_NAME = "bdproj"
COLL_NAME = "alerts"

st.set_page_config(page_title="Live 5xx Alerts", layout="wide")
st.title("🚨 Live 5xx Alerts")
st.caption("Spark Structured Streaming ➜ MongoDB ➜ Streamlit")

@st.cache_resource
def get_mongo_coll():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]

def load_alerts(limit=200):
    coll = get_mongo_coll()
    docs = list(
        coll.find({}, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
    )
    if not docs:
        return pd.DataFrame(columns=["created_at","date","hour","errors","hits","epoch_id"])
    df = pd.DataFrame(docs)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])
    return df

# simple auto-refresh toggle (loops every N seconds)
auto = st.sidebar.checkbox("Auto-refresh every 5s", value=True)

df = load_alerts()

# KPIs
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Total alerts (shown)", len(df))
with c2:
    last_10m = df[df["created_at"] >= datetime.utcnow() - timedelta(minutes=10)]
    st.metric("Alerts in last 10 min", len(last_10m))
with c3:
    latest = df["created_at"].max() if not df.empty else None
    st.metric("Last alert (UTC)", latest.strftime("%Y-%m-%d %H:%M:%S") if latest is not None else "—")

if df.empty:
    st.info("No alerts yet. Keep the Spark streaming job running and push logs into HDFS.")
else:
    # small trend by (date,hour)
    trend = (df.groupby(["date","hour"], as_index=False)
               .agg(errors=("errors","sum"), hits=("hits","sum"))
               .sort_values(["date","hour"]))
    st.subheader("Errors by hour")
    idx = trend["date"].astype(str) + " " + trend["hour"].astype(str)
    st.line_chart(trend.set_index(idx)[["errors"]])

    st.subheader("Most recent alerts")
    st.dataframe(df[["created_at","date","hour","errors","hits","epoch_id"]], use_container_width=True)

# if auto-refresh is on, sleep then rerun
if auto:
    time.sleep(5)
    st.rerun()
