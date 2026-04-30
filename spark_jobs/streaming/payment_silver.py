from core.spark_session import spark
from pyspark.sql.functions import col, when, lit

# -----------------------------------------
# Step 1: Read from Bronze (streaming)
# -----------------------------------------
df = spark.readStream \
    .format("delta") \
    .table("payment_db.payment_transactions_raw")

# -----------------------------------------
# Step 2: Apply validation rules
# -----------------------------------------

valid_condition = (
    col("payment_id").isNotNull() &
    col("customer_id").isNotNull() &
    (col("payment_amount") > 0) &
    col("payment_status").isin("SUCCESS", "FAILED", "PENDING")
)

# -----------------------------------------
# Step 3: Split data
# -----------------------------------------

valid_df = df.filter(valid_condition)

invalid_df = df.filter(~valid_condition) \
    .withColumn(
        "error_reason",
        when(col("payment_id").isNull(), "Missing payment_id")
        .when(col("customer_id").isNull(), "Missing customer_id")
        .when(col("payment_amount") <= 0, "Invalid amount")
        .otherwise("Invalid status")
    )

# -----------------------------------------
# Step 4: Write CLEAN data
# -----------------------------------------

clean_query = valid_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/payment_clean") \
    .toTable("payment_db.payment_transactions_clean")

# -----------------------------------------
# Step 5: Write QUARANTINE data
# -----------------------------------------

quarantine_query = invalid_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/payment_quarantine") \
    .toTable("payment_db.payment_transactions_quarantine")

# -----------------------------------------
# Step 6: Keep running
# -----------------------------------------

clean_query.awaitTermination()
quarantine_query.awaitTermination()