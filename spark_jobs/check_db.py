from core.spark_session import spark

spark.sql("""
DESCRIBE DATABASE EXTENDED customer_db
""").show(truncate=False)

spark.stop()