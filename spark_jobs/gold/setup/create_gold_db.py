from core.spark_session import spark

spark.sql("""
CREATE DATABASE IF NOT EXISTS risk_vault_db
LOCATION 'file:///home/naveen1422/projects/defaulter-platform/data/risk_vault_db'
""")

print("✅ Gold DB created")

spark.stop()