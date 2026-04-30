from core.config import CUSTOMER_DB_PATH
from core.spark_session import spark
from pyspark.sql.functions import col, current_timestamp, when

df = spark.table("customer_db.customer_master_raw")

valid_df = df.filter(
    col("customer_id").isNotNull()
)

invalid_df = df.filter(
    col("customer_id").isNull()
)

invalid_df = invalid_df.withColumn(
    "rejection_reason",
    when(col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
)

invalid_df = invalid_df.withColumn(
    "rejection_timestamp",
    current_timestamp()
)

valid_df.write.format("delta") \
    .mode("append") \
    .option("path",f"{CUSTOMER_DB_PATH}/customer_master_clean") \
    .saveAsTable("customer_db.customer_master_clean")

invalid_df.write.format("delta") \
    .mode("append") \
    .option(
        "path", f"{CUSTOMER_DB_PATH}/customer_master_quarantine") \
    .saveAsTable("customer_db.customer_master_quarantine")

print("Customer Silver and Quarantine complete")

spark.stop()