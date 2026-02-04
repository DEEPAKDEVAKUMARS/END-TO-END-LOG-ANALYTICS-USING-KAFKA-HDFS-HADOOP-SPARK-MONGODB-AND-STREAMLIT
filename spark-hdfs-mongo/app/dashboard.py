import pandas as pd
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Log Analytics (Spark + HDFS + MongoDB)", layout="wide")

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["bdproj"]

def load_df(name: str):
    rows = list(db[name].find({}, {"_id": 0}))
    return pd.DataFrame(rows)

st.title("Log Analytics — Spark ➜ HDFS ➜ MongoDB")

c1, c2 = st.columns(2)
with c1:
    st.subheader("Hits by Hour")
    df_hour = load_df("hits_by_hour")
    if not df_hour.empty:
        st.dataframe(df_hour)
        st.bar_chart(df_hour.sort_values(["date", "hour"]), x="hour", y="hits")
    else:
        st.info("No data in hits_by_hour")

with c2:
    st.subheader("Top URLs")
    df_urls = load_df("top_urls")
    if not df_urls.empty:
        st.dataframe(df_urls)
        top10 = df_urls.sort_values("hits", ascending=False).head(10)
        st.bar_chart(top10, x="url", y="hits")
    else:
        st.info("No data in top_urls")

st.subheader("Status Counts")
df_status = load_df("status_counts")
if not df_status.empty:
    st.dataframe(df_status)
    st.bar_chart(df_status.sort_values(["date", "status"]), x="status", y="count")
else:
    st.info("No data in status_counts")
