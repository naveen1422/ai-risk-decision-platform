from core.spark_session import spark

spark.sql("""
DESCRIBE EXTENDED payment_db.payment_transactions_raw
""").show(truncate=False)

spark.stop()
