from core.spark_session import spark
import time

spark.sql(
    "SELECT COUNT(*) FROM payment_db.payment_transactions_raw"
).show()

spark.sql(
    "SELECT * FROM payment_db.payment_transactions_raw LIMIT 5"
).show()

print("Sleeping so you can inspect Spark UI...")
time.sleep(300)   # 5 minutes

spark.stop()
