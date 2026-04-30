from core.spark_session import spark
spark.sql("CREATE DATABASE IF NOT EXISTS payment_db")
print("Payment Databases created successfully.")
spark.stop()