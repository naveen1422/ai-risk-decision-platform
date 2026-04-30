from core.spark_session import spark
spark.sql("""
SELECT * FROM risk_vault_db.customer_features
LIMIT 10
""").show()