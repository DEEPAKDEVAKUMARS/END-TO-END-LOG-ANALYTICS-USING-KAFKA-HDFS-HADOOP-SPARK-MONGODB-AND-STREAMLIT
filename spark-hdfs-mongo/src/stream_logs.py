from pyspark.sql import SparkSession, functions as F

# same regex you used before (Apache/Nginx common log with quotes)
LOG_RE = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s?([^"]*)\s?(\S+)?"\s+(\d{3})\s+(\S+)\s+"[^"]*"\s+"([^"]*)"'

MONGO_URI = "mongodb://127.0.0.1:27017"
DB = "bdproj"
ERROR_THRESHOLD = 1  # raise alert if >= this many 5xx in a (date,hour) batch

def parse(df):
    df = (df.select(F.col("value").alias("line"))
             .withColumn("ip",     F.regexp_extract("line", LOG_RE, 1))
             .withColumn("ts_str", F.regexp_extract("line", LOG_RE, 2))
             .withColumn("method", F.regexp_extract("line", LOG_RE, 3))
             .withColumn("url",    F.regexp_extract("line", LOG_RE, 4))
             .withColumn("status", F.regexp_extract("line", LOG_RE, 6).cast("int"))
             .withColumn("bytes",  F.when(F.regexp_extract("line", LOG_RE, 7) == "-", 0)
                                      .otherwise(F.regexp_extract("line", LOG_RE, 7).cast("long")))
             .withColumn("agent",  F.regexp_extract("line", LOG_RE, 8))
             .drop("line"))
    df = (df
          .withColumn("ts", F.to_timestamp("ts_str", "dd/MMM/yyyy:HH:mm:ss Z"))
          .withColumn("date", F.to_date("ts"))
          .withColumn("hour", F.hour("ts"))
          .drop("ts_str"))
    return df

def write_mongo(frame, coll, mode="append"):
    (frame.write
        .format("mongo")  # using mongo-spark-connector 3.x short name
        .mode(mode)
        .option("uri", f"{MONGO_URI}/{DB}.{coll}")
        .save())

def process_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        return

    # === Aggregations on this micro-batch ===
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

    # write each table to MongoDB (append streaming results)
    write_mongo(hits_by_hour, "hits_by_hour", mode="append")
    write_mongo(top_urls, "top_urls", mode="append")
    write_mongo(status_counts, "status_counts", mode="append")

    # === Simple alerting: if errors >= threshold in any (date,hour) ===
    alerts = hits_by_hour.where(F.col("errors") >= ERROR_THRESHOLD) \
                         .withColumn("epoch_id", F.lit(int(epoch_id))) \
                         .withColumn("created_at", F.current_timestamp())
    if alerts.count() > 0:
        write_mongo(alerts, "alerts", mode="append")
        # also print to driver logs
        print("ALERTS:")
        alerts.show(truncate=False)

def main():
    spark = (SparkSession.builder
             .appName("stream_log_analytics_hdfs_to_mongo")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # stream new files arriving under /logs/ (HDFS)
    raw = (spark.readStream
           .format("text")
           .option("path", "hdfs:///logs/")           # monitor whole /logs tree
           .option("recursiveFileLookup", "true")     # require Spark 3.2+
           .option("maxFilesPerTrigger", 1)           # simulate steady stream
           .load())

    parsed = parse(raw)

    # use foreachBatch to write to Mongo and do alerts
    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .outputMode("append")
             .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/stream_logs")   # HDFS checkpoint
             .start())

    print("Streaming query started. Waiting for files…")
    query.awaitTermination()

if __name__ == "__main__":
    main()
