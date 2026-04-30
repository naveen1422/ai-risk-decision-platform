from core.spark_session import spark
spark.sql("CREATE DATABASE IF NOT EXISTS customer_db")
print("Customer Databases created successfully.")
spark.stop()