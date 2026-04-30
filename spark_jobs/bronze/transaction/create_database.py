from core.spark_session import spark
spark.sql("CREATE DATABASE IF NOT EXISTS transaction_db")
print("Transaction Databases created successfully.")
spark.stop()