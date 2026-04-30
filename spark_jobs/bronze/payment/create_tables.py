from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS payment_db.payment_transactions_raw (
    payment_id STRING,
    loan_id STRING,
    customer_id STRING,
    payment_amount DECIMAL(18,2),
    payment_date STRING,
    payment_channel STRING,
    payment_status STRING,
    source_system STRING,
    ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION '/home/naveen1422/projects/defaulter-platform/data/payment_db/payment_transactions_raw'
""")

print("Payment raw Delta table created")
