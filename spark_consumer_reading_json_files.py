from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from IPython.display import clear_output
import time

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaJsonFileReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# 2. Get available topics (static batch)
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

# 3. Define the streaming read
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", ",".join(available_topics)) \
    .option("startingOffsets", "latest") \
    .load() \
    .select(
        col("topic"),
        col("value").cast("string").alias("json_string")
    )

# 4. Define foreachBatch function
def foreach_batch_function(df, epoch_id):
    clear_output(wait=True)
    topics = df.select("topic").distinct().collect()
    
    for topic_row in topics:
        topic = topic_row["topic"]
        print(f"\n=== Messages in Topic: {topic} ===")
        topic_msgs = df.filter(col("topic") == topic).select("json_string").collect()
        for row in topic_msgs:
            print(row["json_string"])

# 5. Start the streaming query
query = stream_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka_json_reader_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

# 6. Keep the stream alive
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping stream...")
    query.stop()
    spark.stop()
