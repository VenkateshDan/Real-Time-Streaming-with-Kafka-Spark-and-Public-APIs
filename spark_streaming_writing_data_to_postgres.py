from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgresJSON") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.jars", "/home/jovyan/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Get available topics
batch_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribePattern", ".*") \
    .option("startingOffsets", "earliest") \
    .load()

available_topics = batch_df.select("topic").distinct().rdd.flatMap(lambda x: x).collect()
print(f"Available topics: {available_topics}")

if not available_topics:
    print("No Kafka topics found. Exiting.")
    spark.stop()
    exit()

# 3. Stream messages as JSON strings
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", ",".join(available_topics)) \
    .option("startingOffsets", "latest") \
    .load() \
    .select(
        col("topic").alias("source"),
        col("value").cast("string").alias("json_data")
    )

# 4. PostgreSQL connection config
jdbc_url = "jdbc:postgresql://postgres:5432/apidata"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# # 5. Function to write batch to PostgreSQL
# def write_to_postgres(df, epoch_id):
#     if df.isEmpty():
#         return

#     df.write \
#         .jdbc(
#             url=jdbc_url,
#             table="raw_api_data",
#             mode="append",
#             properties=db_properties
#         )

def write_to_postgres(df, epoch_id):
    if df.isEmpty():
        return

    # Print each row to console
    print(f"\n=== Writing Batch (Epoch {epoch_id}) to PostgreSQL ===")
    for row in df.collect():
        print(f"Source: {row['source']} | JSON: {row['json_data']}")

    # Write to PostgreSQL
    df.write \
        .jdbc(
            url=jdbc_url,
            table="raw_api_data",
            mode="append",
            properties=db_properties
        )

# 6. Start streaming query
query = stream_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka_json_pg_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

# 7. Keep streaming
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping stream...")
    query.stop()
    spark.stop()
