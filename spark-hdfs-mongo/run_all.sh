#!/usr/bin/env bash
set -euo pipefail
cd ~/spark-hdfs-mongo
mkdir -p logs pids

# --- Python env ---
if [ ! -d ".venv" ]; then python3 -m venv .venv; fi
. .venv/bin/activate
pip -q install --upgrade pip
pip -q install streamlit pymongo

# --- App env ---
export MONGO_URI="mongodb://127.0.0.1:27017"

# --- Spark/YARN env ---
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export SPARK_LOCAL_IP=127.0.0.1

# Make sure spark-submit is on PATH
for d in /usr/local/spark /opt/spark "$HOME/spark" "$HOME"/spark-3.3.4-bin-hadoop3 $(ls -d "$HOME"/spark-* 2>/dev/null | head -n1); do
  if [ -x "$d/bin/spark-submit" ]; then export SPARK_HOME="$d"; export PATH="$SPARK_HOME/bin:$PATH"; break; fi
done
command -v spark-submit >/dev/null || { echo "spark-submit not found. Export SPARK_HOME then re-run."; exit 1; }

# --- Kafka (install if missing) ---
if [ ! -x "$HOME/kafka/bin/kafka-server-start.sh" ]; then
  echo "[Kafka] Installing 3.7.0..."
  KVER=3.7.0
  rm -rf "$HOME/kafka"
  curl -L -o /tmp/kafka.tgz https://downloads.apache.org/kafka/${KVER}/kafka_2.13-${KVER}.tgz
  mkdir -p "$HOME/kafka" && tar -xzf /tmp/kafka.tgz -C "$HOME/kafka" --strip-components=1
  cat > "$HOME/kafka/config/kraft/server.properties" <<KF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$HOME/kafka/data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
KF
  "$HOME/kafka/bin/kafka-storage.sh" format -t "$("$HOME/kafka/bin/kafka-storage.sh" random-uuid)" -c "$HOME/kafka/config/kraft/server.properties"
fi

# Start Kafka if not up
if ! nc -z localhost 9092 2>/dev/null; then
  echo "[Kafka] Starting..."
  nohup "$HOME/kafka/bin/kafka-server-start.sh" "$HOME/kafka/config/kraft/server.properties" > logs/kafka.log 2>&1 &
  echo $! > pids/kafka.pid
  for i in {1..20}; do nc -z localhost 9092 && break || sleep 0.5; done
fi

# Ensure topic
"$HOME/kafka/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --topic web_logs --if-not-exists >/dev/null 2>&1 || true

# --- Start Spark streaming job (Kafka -> Mongo) on YARN ---
echo "[Spark] Submitting streaming job..."
nohup spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 \
  --conf spark.yarn.am.memory=512m \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  src/stream_logs_kafka.py > logs/spark_stream.log 2>&1 & echo $! > pids/spark_stream.pid

# --- Start producer to feed sample logs into Kafka (in loop) ---
echo "[Producer] Replaying sample_logs/access.log to Kafka..."
nohup bash -c 'while true; do cat sample_logs/access.log | "$HOME"/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic web_logs; sleep 2; done' > logs/producer.log 2>&1 &
echo $! > pids/producer.pid

# --- Start Streamlit dashboards ---
echo "[Streamlit] Starting dashboards..."
nohup streamlit run app/alerts.py   --server.port 8501 --server.headless true > logs/alerts.log 2>&1 &  echo $! > pids/alerts.pid
nohup streamlit run app/analytics.py --server.port 8502 --server.headless true > logs/analytics.log 2>&1 & echo $! > pids/analytics.pid

echo
echo "=== All components launched ==="
echo "Alerts UI     -> http://localhost:8501"
echo "Analytics UI  -> http://localhost:8502"
echo
echo "Logs:      $(pwd)/logs"
echo "PIDs:      $(pwd)/pids"
