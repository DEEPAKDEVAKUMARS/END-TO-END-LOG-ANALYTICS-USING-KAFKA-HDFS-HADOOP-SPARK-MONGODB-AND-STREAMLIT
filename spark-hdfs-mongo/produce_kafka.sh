#!/bin/bash
set -euo pipefail
LOGFILE="$HOME/spark-hdfs-mongo/sample_logs/access.log"
TOPIC="web_logs"
KAFKA_BIN="$HOME/kafka/bin/kafka-console-producer.sh"
BROKER="localhost:9092"

echo "♻️  Replaying $LOGFILE to $TOPIC with LIVE timestamps (Ctrl+C to stop)"
while true; do
  while IFS= read -r line; do
    now=$(date -u +"%d/%b/%Y:%H:%M:%S +0000")
    # replace the bracketed timestamp with NOW (UTC)
    echo "$line" | sed -E "s|\[[^]]+\]|[${now}]|"
  done < "$LOGFILE" | "$KAFKA_BIN" --bootstrap-server "$BROKER" --topic "$TOPIC"
  echo "→ Completed one cycle — restarting in 5s..."
  sleep 5
done
