from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS account_db.account_master_raw (

    account_id STRING,
    customer_id STRING,
    account_number STRING,
    account_type STRING,
    account_status STRING,
    account_open_date STRING,
    balance DECIMAL(18,2),
    currency STRING,
    branch_code STRING,
    source_system STRING,

    ingestion_date DATE

)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/account_db/account_master_raw'

""")

print("✅ account_master_raw created")

spark.stop()
