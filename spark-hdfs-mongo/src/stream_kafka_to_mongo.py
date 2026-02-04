from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, current_timestamp
from pyspark.sql.types import StringType

# Create Spark session
spark = SparkSession.builder.appName("KafkaLogsToMongo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# === Read from Kafka (structured streaming) ===
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "latest")
    .load()
)

# Parse to text -> tokenize -> count
lines = df_raw.select(col("value").cast(StringType()).alias("line"))
tokens = (
    lines.select(explode(split(col("line"), r"\s+")).alias("token"))
    .where((col("token").isNotNull()) & (col("token") != ""))
)
counts = tokens.groupBy("token").count().withColumn("ingested_at", current_timestamp())

# === Sir evidence ===
print("=== printSchema ===")
counts.printSchema()
print("\n=== explain(True) ===")
counts.explain(True)

# Show preview each micro-batch
def show_batch(df, epoch_id):
    print("\n=== preview (show 10) ===")
    df.orderBy(col("count").desc()).show(10, truncate=False)

# Write to MongoDB using PyMongo from the driver
from pymongo import MongoClient

def write_to_mongo(df, epoch_id):
    rows = [r.asDict() for r in df.collect()]
    if not rows:
        return
    client = MongoClient("mongodb://localhost:27017")
    col = client["bdproj"]["agg_results"]
    for r in rows:
        col.update_one(
            {"token": r["token"]},
            {"$inc": {"count": int(r["count"])}, "$set": {"last_ingested_at": r["ingested_at"]}},
            upsert=True,
        )
    client.close()

query = (
    counts.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, eid: (show_batch(df, eid), write_to_mongo(df, eid)))
    .option("checkpointLocation", "hdfs:///checkpoints/kafka_logs")
    .start()
)

query.awaitTermination()
