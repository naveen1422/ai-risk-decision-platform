from core.spark_session import spark
from core.config import ACCOUNT_DB_PATH
from pyspark.sql.functions import col, lit

# Read bronze
df = spark.table("account_db.account_master_raw")

# Validation rules
valid_df = df.filter(
    (col("account_id").isNotNull()) &
    (col("customer_id").isNotNull())
)

invalid_df = df.filter(
    (col("account_id").isNull()) |
    (col("customer_id").isNull())
).withColumn("rejection_reason", lit("ACCOUNT_ID_OR_CUSTOMER_ID_NULL"))

# Write clean
valid_df.write.format("delta") \
.mode("append") \
.option("path", f"{ACCOUNT_DB_PATH}/account_master_clean") \
.saveAsTable("account_db.account_master_clean")

# Write quarantine
invalid_df.write.format("delta") \
.mode("append") \
.option("path", f"{ACCOUNT_DB_PATH}/account_master_quarantine") \
.saveAsTable("account_db.account_master_quarantine")

spark.stop()