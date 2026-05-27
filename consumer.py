import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session with Kafka connector
spark = SparkSession.builder \
    .appName("CurrencyExchangePipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema definition for incoming Kafka messages
schema = StructType([
    StructField("base_currency", StringType()),
    StructField("target_currency", StringType()),
    StructField("exchange_rate", DoubleType()),
    StructField("timestamp", StringType())
])

# Read raw stream from Kafka topic
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "exchange-rates") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize bytes to string and parse JSON into structured columns
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Persist raw records to Parquet (data lake layer)
raw_query = parsed_stream \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.path.expanduser("~/currency-pipeline/data-lake/raw")) \
    .option("checkpointLocation", os.path.expanduser("~/currency-pipeline/checkpoints/raw")) \
    .trigger(processingTime="10 seconds") \
    .start()

# Compute average exchange rate per currency and output to console
agg_query = parsed_stream \
    .groupBy("target_currency") \
    .agg(avg("exchange_rate").alias("avg_exchange_rate")) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("PySpark consumer running. Reading from Kafka...\n")

# Maintain both streaming queries concurrently
spark.streams.awaitAnyTermination()
