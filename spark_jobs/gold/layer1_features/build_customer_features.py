from core.spark_session import spark
from pyspark.sql.functions import *

# -----------------------------
# 1. READ SILVER TABLES
# -----------------------------
customer_df = spark.table("customer_db.customer_master_clean")
account_df = spark.table("account_db.account_master_clean")
loan_df = spark.table("loan_db.loan_master_clean")
payment_df = spark.table("payment_db.payment_transactions_clean")
transaction_df = spark.table("transaction_db.transaction_clean")

# -----------------------------
# 2. DERIVE CATEGORY (TEMP IN GOLD)
# -----------------------------
transaction_df = transaction_df.withColumn(
    "merchant_category",
    when(col("merchant").rlike("(?i)supermarket"), "Supermarket")
    .when(col("merchant").rlike("(?i)school"), "Education")
    .when(col("merchant").rlike("(?i)service"), "Service")
    .otherwise("Others")
)

# -----------------------------
# 3. PAYMENT FEATURES
# -----------------------------
payment_features = payment_df.groupBy("customer_id").agg(
    count("*").alias("total_payments"),
    sum("payment_amount").alias("total_payment_amount"),
    sum(when(col("payment_status") == "FAILED", 1).otherwise(0)).alias("failed_payment_count")
)

# -----------------------------
# 4. TRANSACTION FEATURES
# -----------------------------
transaction_features = transaction_df.groupBy("customer_id").agg(
    count("*").alias("total_transactions"),
    avg("transaction_amount").alias("avg_transaction_amount"),
    sum(when(col("fraud_flag") == "Y", 1).otherwise(0)).alias("fraud_transaction_count")
)

# -----------------------------
# 5. LOAN FEATURES
# -----------------------------
loan_features = loan_df.groupBy("customer_id").agg(
    count("*").alias("total_loans"),
    sum(when(col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loans")
)

# -----------------------------
# 6. ACCOUNT FEATURES
# -----------------------------
account_features = account_df.groupBy("customer_id").agg(
    count("*").alias("total_accounts")
)

# -----------------------------
# 7. JOIN ALL
# -----------------------------
gold_df = customer_df.select("customer_id") \
    .join(payment_features, "customer_id", "left") \
    .join(transaction_features, "customer_id", "left") \
    .join(loan_features, "customer_id", "left") \
    .join(account_features, "customer_id", "left")

# -----------------------------
# 8. HANDLE NULLS
# -----------------------------
gold_df = gold_df.fillna(0)

# -----------------------------
# 9. DERIVED METRICS
# -----------------------------
gold_df = gold_df.withColumn(
    "payment_failure_rate",
    when(col("total_payments") > 0,
         col("failed_payment_count") / col("total_payments"))
    .otherwise(0)
)

# -----------------------------
# 10. METADATA
# -----------------------------
gold_df = gold_df.withColumn("last_updated", current_timestamp())

# -----------------------------
# 11. WRITE TO GOLD
# -----------------------------
gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("risk_vault_db.customer_features")

print("✅ Gold Layer 1 built successfully")

spark.stop()