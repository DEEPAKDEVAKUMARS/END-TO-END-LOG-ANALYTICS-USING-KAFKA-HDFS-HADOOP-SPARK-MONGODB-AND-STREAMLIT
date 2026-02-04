#!/bin/bash
set -euo pipefail

PROJECT_DIR="$HOME/spark-hdfs-mongo"
LOGDIR="$PROJECT_DIR/logs"
PIDDIR="$PROJECT_DIR/pids"
TOPIC="web_logs"
BROKER="localhost:9092"
KAFKA_HOME="$HOME/kafka"

mkdir -p "$LOGDIR" "$PIDDIR"

echo "=== STARTING BIG DATA PIPELINE ==="

# ---------- 0. Python env ----------
if [ -z "${VIRTUAL_ENV:-}" ]; then
  . "$PROJECT_DIR/.venv/bin/activate"
fi

# ---------- 1. HDFS / YARN ----------
echo "→ Starting HDFS..."
start-dfs.sh >/dev/null 2>&1 || true
sleep 2
echo "→ Starting YARN..."
start-yarn.sh >/dev/null 2>&1 || true
sleep 2

# Verify RM & NM
echo "→ Verifying Hadoop processes..."
jps | grep -E 'NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager' || {
  echo "ERROR: Hadoop/YARN not up"; exit 1; }

# ---------- 2. Kafka (KRaft) ----------
echo "→ Starting Kafka (KRaft config)…"
# ensure our kraft config exists (from your earlier setup)
KRAFT_CONF="$KAFKA_HOME/config/kraft/server.properties"
if ! grep -q "process.roles=" "$KRAFT_CONF"; then
  echo "ERROR: Missing KRaft config at $KRAFT_CONF"; exit 1;
fi

# Kill any old broker, then start clean
pkill -f 'kafka\.Kafka' 2>/dev/null || true
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KRAFT_CONF" \
  > /tmp/kafka.log 2>&1 & echo $! > "$PIDDIR/kafka.pid"
sleep 6

# Ensure 9092 is listening
if ! ss -ltn 2>/dev/null | grep -q ':9092'; then
  echo "ERROR: Kafka not listening on 9092; see /tmp/kafka.log"; exit 1;
fi

# Ensure topic
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BROKER" \
  --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

# ---------- 3. Spark streaming job ----------
echo "→ Locating spark-submit..."
if command -v spark-submit >/dev/null 2>&1; then
  SPARK_BIN="$(command -v spark-submit)"
else
  for d in /usr/local/spark /opt/spark "$HOME"/spark-*; do
    [ -x "$d/bin/spark-submit" ] && SPARK_BIN="$d/bin/spark-submit" && break
  done
fi
[ -z "${SPARK_BIN:-}" ] && { echo "ERROR: spark-submit not found"; exit 1; }
echo "  Using: $SPARK_BIN"

echo "→ Launching Spark streaming job (Kafka → Mongo)…"
nohup "$SPARK_BIN" \
  --master yarn \
  --deploy-mode client \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
  "$PROJECT_DIR/src/stream_logs_kafka.py" \
  > "$LOGDIR/spark_stream.log" 2>&1 & echo $! > "$PIDDIR/spark.pid"
sleep 5
echo "  Spark job started (log: $LOGDIR/spark_stream.log)"

# ---------- 4. Producer (LIVE timestamps) ----------
echo "→ Starting Kafka log producer (live timestamps)…"
nohup "$PROJECT_DIR/produce_kafka.sh" > "$LOGDIR/producer.log" 2>&1 & echo $! > "$PIDDIR/producer.pid"
sleep 2

# ---------- 5. Streamlit ----------
echo "→ Starting Streamlit dashboards…"
nohup streamlit run "$PROJECT_DIR/app/alerts.py"   --server.port 8501 --browser.gatherUsageStats false \
  > "$LOGDIR/alerts.ui.log" 2>&1 & echo $! > "$PIDDIR/alerts.pid"
nohup streamlit run "$PROJECT_DIR/app/analytics.py" --server.port 8502 --browser.gatherUsageStats false \
  > "$LOGDIR/analytics.ui.log" 2>&1 & echo $! > "$PIDDIR/analytics.pid"

echo "=== PIPELINE READY ==="
echo "✅ Spark streaming job → MongoDB"
echo "✅ Kafka producer active"
echo "✅ Dashboards:"
echo "   → http://localhost:8501 (Alerts)"
echo "   → http://localhost:8502 (Analytics)"
echo "Logs → $LOGDIR"
