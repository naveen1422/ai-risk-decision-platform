from core.spark_session import spark

spark.sql("""
CREATE DATABASE IF NOT EXISTS customer_db
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/customer_db'
""")

spark.sql("""
CREATE DATABASE IF NOT EXISTS loan_db
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/loan_db'
""")

spark.sql("""
CREATE DATABASE IF NOT EXISTS transaction_db
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/transaction_db'
""")

print("Databases created successfully")

spark.stop()
