#!/bin/bash
echo "=== STATUS ==="
echo "[Ports]"
for p in 8032 8088 9000 9870 9092 8501 8502; do
  ss -ltn 2>/dev/null | awk -vp=":$p" '$4 ~ p {print "  ", p, "LISTEN"}'
done
echo "[JPS]"
jps | awk '{print "  " $2}'
echo "[Producer]"
ps aux | grep -v grep | grep -q produce_kafka.sh && echo "  running" || echo "  not running"
echo "Logs: ~/spark-hdfs-mongo/logs"
