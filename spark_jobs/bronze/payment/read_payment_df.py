from core.spark_session import spark

df = spark.table("payment_db.payment_transactions_raw")
df.show(5)

spark.stop()
