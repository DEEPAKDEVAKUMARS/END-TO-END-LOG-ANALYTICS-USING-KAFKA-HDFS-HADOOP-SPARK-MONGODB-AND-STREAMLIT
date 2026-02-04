from pyspark.sql import SparkSession, functions as F

# Regex groups:
# 1: ip, 2: time, 3: method, 4: url, 5: proto, 6: status, 7: bytes, 8: agent
LOG_RE = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s?([^"]*)\s?(\S+)?"\s+(\d{3})\s+(\S+)\s+"[^"]*"\s+"([^"]*)"'

def main():
    spark = (SparkSession.builder
             .appName("log_analytics_hdfs_to_mongo")
             .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Read raw logs (recursive through /logs/)
    lines = (spark.read
             .option("recursiveFileLookup", "true")
             .text("hdfs:///logs/")
             .select(F.col("value").alias("line")))

    # Parse with regexp_extract (no Python UDFs)
    df = (lines
          .withColumn("ip",     F.regexp_extract("line", LOG_RE, 1))
          .withColumn("ts_str", F.regexp_extract("line", LOG_RE, 2))
          .withColumn("method", F.regexp_extract("line", LOG_RE, 3))
          .withColumn("url",    F.regexp_extract("line", LOG_RE, 4))
          .withColumn("status", F.regexp_extract("line", LOG_RE, 6).cast("int"))
          .withColumn("bytes",  F.when(F.regexp_extract("line", LOG_RE, 7) == "-", 0)
                                   .otherwise(F.regexp_extract("line", LOG_RE, 7).cast("long")))
          .withColumn("agent",  F.regexp_extract("line", LOG_RE, 8))
          .drop("line"))

    # Timestamp & derived columns
    df = (df
          .withColumn("ts", F.to_timestamp("ts_str", "dd/MMM/yyyy:HH:mm:ss Z"))
          .withColumn("date", F.to_date("ts"))
          .withColumn("hour", F.hour("ts"))
          .drop("ts_str"))

    # === Spark evidence ===
    print("\n=== Spark printSchema() ==="); df.printSchema()
    print("\n=== Spark explain(True) ==="); df.explain(True)
    print("\n=== Spark preview (df.show(10)) ==="); df.show(10, truncate=False)

    # Aggregations
    hits_by_hour = (df.groupBy("date","hour")
                      .agg(F.count("*").alias("hits"),
                           F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("errors"))
                      .orderBy("date","hour"))

    top_urls = (df.groupBy("date","url")
                  .agg(F.count("*").alias("hits"))
                  .orderBy(F.desc("hits"))
                  .limit(1000))

    status_counts = (df.groupBy("date","status")
                       .agg(F.count("*").alias("count"))
                       .orderBy("date","status"))

    # Show previews
    print("\n=== hits_by_hour.show(10) ===");   hits_by_hour.show(10, truncate=False)
    print("\n=== top_urls.show(10) ===");       top_urls.show(10, truncate=False)
    print("\n=== status_counts.show(10) ===");  status_counts.show(10, truncate=False)

    # Write to MongoDB
    def save_mongo(frame, coll):
        (frame.write
         .format("mongo")                           # ← use 'mongo' short name
         .mode("overwrite")
         .option("uri", f"mongodb://localhost:27017/bdproj.{coll}")  # ← full URI
         .save())

    save_mongo(hits_by_hour, "hits_by_hour")
    save_mongo(top_urls, "top_urls")
    save_mongo(status_counts, "status_counts")

    spark.stop()

if __name__ == "__main__":
    main()
