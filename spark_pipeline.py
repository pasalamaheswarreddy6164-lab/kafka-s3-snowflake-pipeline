# Kafka → Spark → S3 Streaming Pipeline (Final Fixed)

import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col

# -------------------------------
# Load ENV
# -------------------------------
load_dotenv()

aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

# -------------------------------
# Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("KafkaToS3Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Read Kafka Stream (FIXED)
# -------------------------------
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "customers") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# -------------------------------
# Convert Kafka value
# -------------------------------
value_df = df.selectExpr("CAST(value AS STRING)")

# -------------------------------
# Schema
# -------------------------------
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("amount", IntegerType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType())

# -------------------------------
# Parse JSON
# -------------------------------
json_df = value_df.select(from_json(col("value"), schema).alias("data"))
final_df = json_df.select("data.*")

# -------------------------------
# Configure S3
# -------------------------------
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

# -------------------------------
# Write Stream
# -------------------------------
query = final_df.writeStream \
    .format("csv") \
    .option("path", "s3a://data-lake-maheswar1/raw/") \
    .option("checkpointLocation", "s3a://data-lake-maheswar1/checkpoint/") \
    .option("header", "true") \
    .option("failOnDataLoss", "false") \
    .trigger(processingTime="1 minute") \
    .start()

# -------------------------------
# Keep Alive
# -------------------------------
query.awaitTermination()