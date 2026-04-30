from core.spark_session import spark
spark.sql("""
SELECT * FROM payment_db.payment_transactions_quarantine
ORDER BY ingestion_date DESC
""").show()