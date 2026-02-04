#!/usr/bin/env bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
  --conf spark.mongodb.write.connection.uri="mongodb://localhost:27017" \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  src/parse_logs.py
