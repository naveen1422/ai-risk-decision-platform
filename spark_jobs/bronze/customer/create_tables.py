from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS customer_db.customer_master_raw (

    customer_id STRING,
    customer_name STRING,
    date_of_birth STRING,
    phone_number STRING,
    email STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    kyc_status STRING,
    source_system STRING,
    ingestion_date DATE

)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/customer_db/customer_master_raw'
""")

print("Customer raw table created")

spark.stop()
