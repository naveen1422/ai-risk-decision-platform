from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS risk_vault_db.customer_insights (
    customer_id STRING,
    risk_score INT,
    risk_bucket STRING,
    risk_flags ARRAY<STRING>,

    behaviour_summary STRING,

    top_entities ARRAY<STRUCT<
        type: STRING,
        name: STRING,
        frequency: INT
    >>,

    top_counterparties ARRAY<STRING>,

    recommended_actions ARRAY<STRING>,

    last_updated TIMESTAMP
)
USING DELTA
""")

print("✅ customer_insights table created")

spark.stop()