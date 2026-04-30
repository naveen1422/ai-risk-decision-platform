from core.spark_session import spark
from core.config import TRANSACTION_DB_PATH
from pyspark.sql.functions import col, lit

df = spark.table("transaction_db.transaction_raw")

valid_df = df.filter(
    (col("transaction_id").isNotNull()) &
    (col("transaction_amount") > 0)
)

invalid_df = df.filter(
    (col("transaction_id").isNull()) |
    (col("transaction_amount") <= 0)
).withColumn("rejection_reason", lit("INVALID_TRANSACTION"))

valid_df.write.format("delta") \
.mode("append") \
.option("path", f"{TRANSACTION_DB_PATH}/transaction_clean") \
.saveAsTable("transaction_db.transaction_clean")

invalid_df.write.format("delta") \
.mode("append") \
.option("path", f"{TRANSACTION_DB_PATH}/transaction_quarantine") \
.saveAsTable("transaction_db.transaction_quarantine")

spark.stop()