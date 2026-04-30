from core.spark_session import spark
from pyspark.sql.functions import current_timestamp

rules = [
    ("R1", "High Failed Payments",
     "failed_payment_count >= 3",
     "CUSTOMER",
     "failed_payment_count",
     3, ">=",
     30, 1,
     "DAILY", "30D",
     "Y"),

    ("R2", "Fraud Transactions",
     "fraud_transaction_count > 0",
     "CUSTOMER",
     "fraud_transaction_count",
     0, ">",
     50, 1,
     "REALTIME", "7D",
     "Y"),

    ("R3", "Loan Default",
     "defaulted_loans > 0",
     "CUSTOMER",
     "defaulted_loans",
     0, ">",
     40, 2,
     "DAILY", "30D",
     "Y"),

    ("R4", "Low Payment Activity",
     "total_payment_amount < 1000",
     "CUSTOMER",
     "total_payment_amount",
     1000, "<",
     10, 3,
     "DAILY", "30D",
     "Y")
]

columns = [
    "rule_id", "rule_name", "rule_description",
    "focus_level", "rule_expression",
    "threshold_value", "comparison_operator",
    "risk_score", "priority",
    "frequency", "monitoring_period",
    "is_active"
]

df = spark.createDataFrame(rules, columns) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("updated_at", current_timestamp())

df.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .insertInto("risk_vault_db.rule_master")
print("✅ Rules inserted")

spark.stop()