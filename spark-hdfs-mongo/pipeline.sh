#!/usr/bin/env bash
set -euo pipefail

# ===== Config =====
ROOT_DIR="${ROOT_DIR:-$PWD}"
VENV_DIR="$ROOT_DIR/.venv"
LOG_DIR="$ROOT_DIR/logs"
PID_DIR="$ROOT_DIR/pids"
KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka}"
KAFKA_VER="${KAFKA_VER:-3.7.0}"
KAFKA_TGZ="kafka_2.13-${KAFKA_VER}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VER}/${KAFKA_TGZ}"
TOPIC="${TOPIC:-web_logs}"

# Streamlit ports
ALERTS_PORT=8501
ANALYTICS_PORT=8502

# Spark temp (avoid /tmp on WSL)
SPARK_LOCAL="${SPARK_LOCAL:-$HOME/spark-local}"
SPARK_TMP_DRIVER="${SPARK_TMP_DRIVER:-$HOME/spark-tmp/driver}"
SPARK_TMP_EXEC="${SPARK_TMP_EXEC:-$HOME/spark-tmp/executor}"

# Mongo
export MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"

# ===== Helpers =====
msg() { echo -e "\033[1;36m$*\033[0m"; }
ok()  { echo -e "\033[1;32m$*\033[0m"; }
warn(){ echo -e "\033[1;33m$*\033[0m"; }
err() { echo -e "\033[1;31m$*\033[0m"; }

ensure_dirs() {
  mkdir -p "$LOG_DIR" "$PID_DIR" "$SPARK_LOCAL" "$SPARK_TMP_DRIVER" "$SPARK_TMP_EXEC"
}

ensure_python() {
  if ! command -v python3 >/dev/null 2>&1; then
    err "python3 not found"; exit 1
  fi
  if [ ! -d "$VENV_DIR" ]; then
    msg "→ Creating venv"
    python3 -m venv "$VENV_DIR"
  fi
  # shellcheck disable=SC1090
  . "$VENV_DIR/bin/activate"
  python -m pip install --upgrade pip >/dev/null
  if [ -f "$ROOT_DIR/requirements.txt" ]; then
    msg "→ Installing Python deps (requirements.txt)"
    pip install -r "$ROOT_DIR/requirements.txt" >>"$LOG_DIR/pip.log" 2>&1 || {
      warn "requirements.txt failed, installing minimal set"
      pip install streamlit pymongo pandas >>"$LOG_DIR/pip.log" 2>&1
    }
  else
    msg "→ Installing Python deps (minimal)"
    pip install streamlit pymongo pandas >>"$LOG_DIR/pip.log" 2>&1
  fi
}

ensure_kafka() {
  if [ ! -x "$KAFKA_DIR/bin/kafka-server-start.sh" ]; then
    msg "→ Downloading Kafka $KAFKA_VER"
    rm -rf "$KAFKA_DIR" /tmp/$KAFKA_TGZ 2>/dev/null || true
    curl -L -o /tmp/$KAFKA_TGZ "$KAFKA_URL"
    mkdir -p "$KAFKA_DIR"
    tar -xzf /tmp/$KAFKA_TGZ -C "$KAFKA_DIR" --strip-components=1
    rm -f /tmp/$KAFKA_TGZ
  fi

  # minimal server.properties tweak: bind & advertise localhost
  if ! grep -q '^listeners=PLAINTEXT://127.0.0.1:9092' "$KAFKA_DIR/config/server.properties"; then
    msg "→ Configuring Kafka listeners"
    sed -i 's|^#\?listeners=.*||g' "$KAFKA_DIR/config/server.properties"
    sed -i 's|^#\?advertised.listeners=.*||g' "$KAFKA_DIR/config/server.properties"
    sed -i 's|^#\?listener.security.protocol.map=.*||g' "$KAFKA_DIR/config/server.properties"
    sed -i 's|^#\?inter.broker.listener.name=.*||g' "$KAFKA_DIR/config/server.properties"
    cat >> "$KAFKA_DIR/config/server.properties" <<'CFG'

# === added by pipeline.sh ===
listeners=PLAINTEXT://127.0.0.1:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
CFG
  fi

  # start broker if not listening
  if ! ss -ltn '( sport = :9092 )' | tail -n +2 >/dev/null 2>&1; then
    msg "→ Starting Kafka"
    nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties" \
      > "$LOG_DIR/kafka.log" 2>&1 & echo $! > "$PID_DIR/kafka.pid"
    sleep 2
  fi

  # ensure topic
  msg "→ Ensuring topic '$TOPIC'"
  "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
}

detect_spark() {
  for d in /usr/local/spark /opt/spark "$HOME"/spark-* "$HOME"/tools/spark; do
    [ -x "$d/bin/spark-submit" ] && export SPARK_HOME="$d" && export PATH="$SPARK_HOME/bin:$PATH"
  done
  if ! command -v spark-submit >/dev/null 2>&1; then
    err "spark-submit not found. Please install Apache Spark and retry."
    exit 1
  fi
}

start_streamlit() {
  # Alerts
  nohup streamlit run "$ROOT_DIR/app/alerts.py" --server.port $ALERTS_PORT --browser.gatherUsageStats false \
    > "$LOG_DIR/alerts.ui.log" 2>&1 & echo $! > "$PID_DIR/alerts.pid"
  # Analytics
  nohup streamlit run "$ROOT_DIR/app/analytics.py" --server.port $ANALYTICS_PORT --browser.gatherUsageStats false \
    > "$LOG_DIR/analytics.ui.log" 2>&1 & echo $! > "$PID_DIR/analytics.pid"
}

start_producer() {
  # Replay with live timestamps (uses your existing script if present)
  if [ -x "$ROOT_DIR/produce_kafka.sh" ]; then
    nohup "$ROOT_DIR/produce_kafka.sh" > "$LOG_DIR/producer.log" 2>&1 & echo $! > "$PID_DIR/producer.pid"
  else
    # fallback one-liner
    nohup bash -lc "while true; do cat '$ROOT_DIR/sample_logs/access.log' | '$KAFKA_DIR/bin/kafka-console-producer.sh' --bootstrap-server localhost:9092 --topic '$TOPIC'; sleep 2; done" \
      > "$LOG_DIR/producer.log" 2>&1 & echo $! > "$PID_DIR/producer.pid"
  fi
}

start_spark() {
  detect_spark
  msg "→ Launching Spark streaming job (Kafka → Mongo)"
  # Try HDFS path first, fall back to local checkpoint if HDFS unreachable
  HDFS_OK=0
  hdfs dfs -mkdir -p /checkpoints/stream_logs_kafka >/dev/null 2>&1 && HDFS_OK=1 || true
  if [ $HDFS_OK -eq 1 ]; then
    CHK="hdfs://localhost:9000/checkpoints/stream_logs_kafka"
  else
    warn "HDFS not reachable; using local checkpoint"
    CHK="$ROOT_DIR/.chk/stream_logs_kafka"
    mkdir -p "$CHK"
  fi

  nohup spark-submit \
    --master yarn \
    --deploy-mode client \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.local.dir="$SPARK_LOCAL" \
    --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=$SPARK_TMP_DRIVER" \
    --conf spark.executor.extraJavaOptions="-Djava.io.tmpdir=$SPARK_TMP_EXEC" \
    "$ROOT_DIR/src/stream_logs_kafka.py" \
    > "$LOG_DIR/spark_stream.log" 2>&1 & echo $! > "$PID_DIR/spark.pid"
}

stop_pids() {
  for f in "$PID_DIR"/*.pid; do
    [ -f "$f" ] || continue
    pid=$(cat "$f" 2>/dev/null || true)
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null || true
    rm -f "$f"
  done

  # extra safety
  pkill -f "streamlit run app/alerts.py" 2>/dev/null || true
  pkill -f "streamlit run app/analytics.py" 2>/dev/null || true
  pkill -f "spark-submit .*stream_logs_kafka.py" 2>/dev/null || true
  pkill -f "kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $TOPIC" 2>/dev/null || true
}

status_line() {
  printf "%-18s  %s\n" "$1" "$2"
}

cmd_start() {
  msg "=== START ==="
  ensure_dirs
  ensure_python

  # YARN/HDFS quick checks (non-fatal)
  yarn node -list >/dev/null 2>&1 || warn "YARN check failed (will try anyway)"
  hdfs dfsadmin -report >/dev/null 2>&1 || warn "HDFS check failed (will try anyway)"

  ensure_kafka
  start_spark
  start_streamlit
  start_producer

  ok "Pipeline up!"
  echo "Dashboards:"
  echo "  Alerts   → http://localhost:${ALERTS_PORT}"
  echo "  Analytics→ http://localhost:${ANALYTICS_PORT}"
  echo "Logs → $LOG_DIR"
}

cmd_stop() {
  msg "=== STOP ==="
  stop_pids
  ok "All user-space components stopped (Spark job, producer, Streamlit)."
  warn "HDFS/YARN and MongoDB were not stopped by this script."
}

cmd_status() {
  msg "=== STATUS ==="
  ss -ltn '( sport = :9092 or sport = :8032 or sport = :27017 or sport = :8501 or sport = :8502 )' || true
  status_line "Kafka" "$(jps | grep -q Kafka && echo RUNNING || echo down)"
  status_line "Spark job" "$( [ -f "$PID_DIR/spark.pid" ] && ps -p "$(cat $PID_DIR/spark.pid 2>/dev/null)" >/dev/null 2>&1 && echo RUNNING || echo down )"
  status_line "Producer" "$( [ -f "$PID_DIR/producer.pid" ] && ps -p "$(cat $PID_DIR/producer.pid 2>/dev/null)" >/dev/null 2>&1 && echo RUNNING || echo down )"
  status_line "Alerts UI" "$(ss -ltn '( sport = :8501 )' | tail -n +2 >/dev/null 2>&1 && echo LISTEN || echo down)"
  status_line "Analytics UI" "$(ss -ltn '( sport = :8502 )' | tail -n +2 >/dev/null 2>&1 && echo LISTEN || echo down)"
}

cmd_logs() {
  msg "=== LOGS (tail -f) ==="
  echo "Ctrl+C to exit"
  tail -f "$LOG_DIR"/{spark_stream.log,producer.log,alerts.ui.log,analytics.ui.log} 2>/dev/null || true
}

usage() {
  cat <<USAGE
Usage: $0 {start|stop|status|logs}

start   – setup venv, ensure Kafka + topic, run Spark, Streamlit, producer
stop    – stop Spark job, producer, Streamlit (does NOT stop HDFS/YARN/Mongo)
status  – show quick port/process status
logs    – tail Spark/producer/Streamlit logs
USAGE
}

case "${1:-}" in
  start)  cmd_start ;;
  stop)   cmd_stop ;;
  status) cmd_status ;;
  logs)   cmd_logs ;;
  *)      usage; exit 1 ;;
esac
