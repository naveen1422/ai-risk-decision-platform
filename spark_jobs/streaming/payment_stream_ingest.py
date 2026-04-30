from core.spark_session import spark
from pyspark.sql.functions import col, from_json, current_date, lit
from pyspark.sql.types import StructType, StringType, DoubleType, DecimalType

# -----------------------------------------
# Step 1: Define Kafka JSON schema
# -----------------------------------------
kafka_schema = StructType() \
    .add("payment_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("status", StringType())

# -----------------------------------------
# Step 2: Read stream from Kafka
# -----------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "payment_events") \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------------------
# Step 3: Convert Kafka value to JSON
# -----------------------------------------
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), kafka_schema).alias("data")
).select("data.*")

# -----------------------------------------
# Step 4: Map to RAW table schema
# -----------------------------------------
final_df = parsed_df.select(
    col("payment_id"),
    lit(None).cast("string").alias("loan_id"),
    col("customer_id"),
    col("amount").cast(DecimalType(18, 2)).alias("payment_amount"),
    lit(None).cast("string").alias("payment_date"),
    lit(None).cast("string").alias("payment_channel"),
    col("status").alias("payment_status"),
    lit("KAFKA_STREAM").alias("source_system"),
    current_date().alias("ingestion_date")
)

# -----------------------------------------
# Step 5: Write to Delta table
# -----------------------------------------
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/payment_stream") \
    .toTable("payment_db.payment_transactions_raw")

# -----------------------------------------
# Step 6: Keep stream running
# -----------------------------------------
query.awaitTermination()