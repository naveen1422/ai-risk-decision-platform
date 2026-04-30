from core.spark_session import spark
from core.config import LOAN_DB_PATH
from pyspark.sql.functions import col, lit

df = spark.table("loan_db.loan_master_raw")

valid_df = df.filter(
    (col("loan_id").isNotNull()) &
    (col("loan_amount") > 0)
)

invalid_df = df.filter(
    (col("loan_id").isNull()) |
    (col("loan_amount") <= 0)
).withColumn("rejection_reason", lit("INVALID_LOAN_AMOUNT_OR_ID"))

valid_df.write.format("delta") \
.mode("append") \
.option("path", f"{LOAN_DB_PATH}/loan_master_clean") \
.saveAsTable("loan_db.loan_master_clean")

invalid_df.write.format("delta") \
.mode("append") \
.option("path", f"{LOAN_DB_PATH}/loan_master_quarantine") \
.saveAsTable("loan_db.loan_master_quarantine")

spark.stop()