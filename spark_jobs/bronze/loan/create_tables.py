from core.spark_session import spark

spark.sql("""
CREATE TABLE IF NOT EXISTS loan_db.loan_master_raw (

    loan_id STRING,
    customer_id STRING,
    loan_amount DECIMAL(18,2),
    interest_rate DECIMAL(5,2),
    loan_type STRING,
    loan_status STRING,
    disbursement_date STRING,
    loan_term_months INT,
    source_system STRING,
    ingestion_date DATE

)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION 'file:/home/naveen1422/projects/defaulter-platform/data/loan_db/loan_master_raw'
""")

print("Loan raw table created")

spark.stop()
