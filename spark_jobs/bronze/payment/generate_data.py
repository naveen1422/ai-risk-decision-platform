# -------------------------------------------------
# Dirty Payment Data Generator (RAW / Bronze Layer)
# -------------------------------------------------

from core.spark_session import spark
from pyspark.sql.functions import current_date
from pyspark.sql.types import DecimalType
import random
from datetime import datetime, timedelta

# -------------------------------------------------
# Reference values (intentionally dirty)
# -------------------------------------------------
payment_channels = ["UPI", "NEFT", "IMPS", "CASH", "CARD", None, "upi", "NETBANK"]
payment_statuses = ["SUCCESS", "FAILED", "PENDING", None, "success", "UNKNOWN"]
source_systems = ["TATA_CORE", "ICICI_API", "HDFC_BATCH", None]

# -------------------------------------------------
# Helper functions
# -------------------------------------------------
def random_date_string():
    """
    Generates mixed-format and invalid date strings
    to simulate real-world dirty source data.
    """
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "INVALID_DATE",
        None
    ]

    fmt = random.choice(formats)

    if fmt is None or fmt == "INVALID_DATE":
        return fmt

    return (datetime.now() - timedelta(days=random.randint(0, 30))).strftime(fmt)


def random_amount():
    """
    Generates valid, negative, zero, and null amounts
    to simulate payment inconsistencies.
    """
    return random.choice([
        round(random.uniform(100, 50000), 2),  # valid
        -500.00,                               # invalid
        0.00,                                  # edge case
        None                                   # missing
    ])

# -------------------------------------------------
# Generate dirty payment records
# -------------------------------------------------
records = []

for _ in range(1000):
    record = (
        f"PAY_{random.randint(1, 200)}",   # duplicates possible
        f"LN_{random.randint(1, 150)}",
        f"CUST_{random.randint(1, 300)}",
        random_amount(),
        random_date_string(),
        random.choice(payment_channels),
        random.choice(payment_statuses),
        random.choice(source_systems)
    )
    records.append(record)

columns = [
    "payment_id",
    "loan_id",
    "customer_id",
    "payment_amount",
    "payment_date",
    "payment_channel",
    "payment_status",
    "source_system"
]

df = spark.createDataFrame(records, columns)

# -------------------------------------------------
# Add system-generated ingestion column
# -------------------------------------------------
df = df.withColumn("ingestion_date", current_date())

# -------------------------------------------------
# IMPORTANT: Explicitly cast amount to match Delta
# -------------------------------------------------
df = df.withColumn(
    "payment_amount",
    df["payment_amount"].cast(DecimalType(18, 2))
)

# -------------------------------------------------
# Write to RAW Delta table
# -------------------------------------------------
spark.sql("USE payment_db")
df.write.format("delta") \
    .mode("append") \
    .saveAsTable("payment_db.payment_transactions_raw")

print("✅ Dirty payment data successfully loaded into RAW table")

# -------------------------------------------------
# Stop Spark (batch job best practice)
# -------------------------------------------------
spark.stop()
