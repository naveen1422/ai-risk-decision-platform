from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS risk_vault_db.customer_risk_profile (
    customer_id STRING,
    risk_score INT,
    risk_bucket STRING,
    risk_flags ARRAY<STRING>,
    last_updated TIMESTAMP
)
USING DELTA
""")

print("✅ customer_risk_profile table created")

spark.stop()