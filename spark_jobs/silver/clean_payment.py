from core.spark_session import spark
from core.config import PAYMENT_DB_PATH
from pyspark.sql.functions import col, lit

df = spark.table("payment_db.payment_transactions_raw")

valid_df = df.filter(
    (col("payment_id").isNotNull()) &
    (col("payment_amount") > 0)
)

invalid_df = df.filter(
    (col("payment_id").isNull()) |
    (col("payment_amount") <= 0)
).withColumn("rejection_reason", lit("INVALID_PAYMENT"))

valid_df.write.format("delta") \
.mode("append") \
.option("path", f"{PAYMENT_DB_PATH}/payment_transactions_clean") \
.saveAsTable("payment_db.payment_transactions_clean")

invalid_df.write.format("delta") \
.mode("append") \
.option("path", f"{PAYMENT_DB_PATH}/payment_transactions_quarantine") \
.saveAsTable("payment_db.payment_transactions_quarantine")

spark.stop()