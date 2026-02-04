#!/bin/bash
set -euo pipefail
echo "=== STOPPING PIPELINE ==="

# 1) Stop app processes by name (Spark driver, Streamlit, producer)
pkill -f "spark-submit .*stream_logs_kafka.py" 2>/dev/null || true
pkill -f "streamlit run app/alerts.py"         2>/dev/null || true
pkill -f "streamlit run app/analytics.py"      2>/dev/null || true
pkill -f "produce_kafka.sh"                    2>/dev/null || true
pkill -f "python .*streamlit"                  2>/dev/null || true
pkill -f "streamlit"                           2>/dev/null || true

# 2) Free UI ports 8501/8502 aggressively
for p in 8501 8502; do
  fuser -k ${p}/tcp 2>/dev/null || true
  lsof -ti :$p 2>/dev/null | xargs -r kill || true
  sleep 1
  lsof -ti :$p 2>/dev/null | xargs -r kill -9 || true
done

# 3) Stop YARN/HDFS if running (safe if not)
if jps | grep -q ResourceManager; then stop-yarn.sh || true; fi
if jps | grep -Eq 'NameNode|DataNode|SecondaryNameNode'; then stop-dfs.sh || true; fi

# 4) Clean Hadoop PID files
rm -f /tmp/hadoop-*-namenode.pid \
      /tmp/hadoop-*-datanode.pid \
      /tmp/hadoop-*-secondarynamenode.pid \
      /tmp/hadoop-*-resourcemanager.pid \
      /tmp/hadoop-*-nodemanager.pid 2>/dev/null || true

echo "✅ All components stopped."
