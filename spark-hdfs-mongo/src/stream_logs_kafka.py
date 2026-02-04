from pyspark.sql import SparkSession, functions as F

LOG_RE = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s?([^"]*)\s?(\S+)?"\s+(\d{3})\s+(\S+)\s+"[^"]*"\s+"([^"]*)"'
MONGO_URI = "mongodb://127.0.0.1:27017"
DB = "bdproj"
ERROR_THRESHOLD = 1  # alerts fire when errors >= this per batch

def parse_from_text_col(df, col="text"):
    df = (df
          .withColumn("ip",     F.regexp_extract(col, LOG_RE, 1))
          .withColumn("ts_str", F.regexp_extract(col, LOG_RE, 2))
          .withColumn("method", F.regexp_extract(col, LOG_RE, 3))
          .withColumn("url",    F.regexp_extract(col, LOG_RE, 4))
          .withColumn("status", F.regexp_extract(col, LOG_RE, 6).cast("int"))
          .withColumn("bytes",  F.when(F.regexp_extract(col, LOG_RE, 7) == "-", 0)
                                   .otherwise(F.regexp_extract(col, LOG_RE, 7).cast("long")))
          .withColumn("agent",  F.regexp_extract(col, LOG_RE, 8)))
    df = (df
          .withColumn("ts", F.to_timestamp("ts_str", "dd/MMM/yyyy:HH:mm:ss Z"))
          .withColumn("date", F.to_date("ts"))
          .withColumn("hour", F.hour("ts"))
          .drop("ts_str"))
    return df

def write_mongo(frame, coll, mode="append"):
    (frame.write
        .format("mongo")
        .mode(mode)
        .option("uri", f"{MONGO_URI}/{DB}.{coll}")
        .save())

def process_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    # --- EVIDENCE: sample rows from each micro-batch ---
    print(f"=== Batch {epoch_id} sample ===")
    batch_df.select("ip","ts","method","url","status","bytes","agent") \
            .show(5, truncate=False)

    hits_by_hour = (batch_df.groupBy("date","hour")
                    .agg(F.count("*").alias("hits"),
                         F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("errors"))
                    .orderBy("date","hour"))

    top_urls = (batch_df.groupBy("date","url")
                .agg(F.count("*").alias("hits"))
                .orderBy(F.desc("hits"))
                .limit(1000))

    status_counts = (batch_df.groupBy("date","status")
                     .agg(F.count("*").alias("count"))
                     .orderBy("date","status"))

    write_mongo(hits_by_hour, "hits_by_hour")
    write_mongo(top_urls, "top_urls")
    write_mongo(status_counts, "status_counts")

    alerts = (hits_by_hour
              .where(F.col("errors") >= ERROR_THRESHOLD)
              .withColumn("epoch_id", F.lit(int(epoch_id)))
              .withColumn("created_at", F.current_timestamp()))
    if alerts.count() > 0:
        write_mongo(alerts, "alerts")
        print("ALERTS:")
        alerts.show(truncate=False)

def main():
    spark = (SparkSession.builder
             .appName("stream_log_analytics_kafka_to_mongo")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka = (spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("subscribe", "web_logs")
             .option("startingOffsets", "latest")
             .load())

    lines = kafka.select(F.col("value").cast("string").alias("text"))
    parsed = parse_from_text_col(lines, "text")

    # --- EVIDENCE: schema + physical plan printed once at startup ---
    parsed.printSchema()
    parsed.explain(True)
    print("Schema and plan printed above. Waiting for micro-batches...")

    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .outputMode("append")
             .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/stream_logs_kafka")
             .start())

    print("Kafka streaming query started. Waiting for messages…")
    query.awaitTermination()

if __name__ == "__main__":
    main()
