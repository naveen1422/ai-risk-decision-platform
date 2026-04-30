from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS risk_vault_db.rule_master (
    rule_id STRING,
    rule_name STRING,
    rule_description STRING,

    focus_level STRING,          -- CUSTOMER / ACCOUNT / TRANSACTION
    rule_expression STRING,      -- dynamic expression

    threshold_value DOUBLE,
    comparison_operator STRING,  -- >, <, =

    risk_score INT,
    priority INT,

    frequency STRING,            -- REALTIME / DAILY
    monitoring_period STRING,    -- 7D / 30D

    is_active STRING,

    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

print("✅ Rule config table created")

spark.stop()