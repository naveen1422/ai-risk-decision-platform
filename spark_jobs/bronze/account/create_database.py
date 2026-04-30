from core.spark_session import spark
spark.sql("CREATE DATABASE IF NOT EXISTS account_db")
print("Account Databases created successfully.")
spark.stop()