from core.spark_session import spark
from pyspark.sql.functions import current_date
from pyspark.sql.types import DecimalType
import random

transaction_types = ["DEBIT", "CREDIT", None]
fraud_flags = ["Y", "N", None]
source_systems = ["ATM", "POS", "UPI", None]

def random_amount():
    return random.choice([
        round(random.uniform(100,50000),2),
        -100.00,
        None
    ])

records = []

for _ in range(2000):

    record = (
        f"TXN_{random.randint(1,500)}",
        f"ACC_{random.randint(1,200)}",
        f"CUST_{random.randint(1,300)}",
        random_amount(),
        random.choice(transaction_types),
        random.choice(["2024-01-01", None, "INVALID"]),
        random.choice(["Amazon", "Flipkart", None]),
        random.choice(["Mumbai", "Delhi", None]),
        random.choice(fraud_flags),
        random.choice(source_systems)
    )

    records.append(record)

columns = [
    "transaction_id",
    "account_id",
    "customer_id",
    "transaction_amount",
    "transaction_type",
    "transaction_date",
    "merchant",
    "location",
    "fraud_flag",
    "source_system"
]

df = spark.createDataFrame(records, columns)

df = df.withColumn("ingestion_date", current_date())

df = df.withColumn("transaction_amount",
                   df["transaction_amount"].cast(DecimalType(18,2)))

df.write.format("delta") \
    .mode("append") \
    .saveAsTable("transaction_db.transaction_raw")

print("Transaction dirty data loaded")

spark.stop()
