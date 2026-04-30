from core.spark_session import spark
spark.sql("CREATE DATABASE IF NOT EXISTS loan_db")
print("Loan Databases created successfully.")
spark.stop()