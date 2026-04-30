from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS transaction_db.transaction_raw (

    transaction_id STRING,
    account_id STRING,
    customer_id STRING,
    transaction_amount DECIMAL(18,2),
    transaction_type STRING,
    transaction_date STRING,
    merchant STRING,
    location STRING,
    fraud_flag STRING,
    source_system STRING,
    ingestion_date DATE

)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/transaction_db/transaction_raw'
""")

print("Transaction raw table created")

spark.stop()
