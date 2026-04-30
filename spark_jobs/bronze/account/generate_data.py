# -------------------------------------------------
# Dirty Account Data Generator (Bronze Layer)
# -------------------------------------------------

from core.spark_session import spark
from pyspark.sql.functions import current_date
from pyspark.sql.types import DecimalType
import random
from datetime import datetime, timedelta

# -------------------------------------------------
# Dirty reference values
# -------------------------------------------------

account_types = [
    "SAVINGS",
    "CURRENT",
    "LOAN",
    None,
    "savings",
    "UNKNOWN"
]

account_statuses = [
    "ACTIVE",
    "CLOSED",
    "FROZEN",
    None,
    "active"
]

currencies = [
    "INR",
    "USD",
    None,
    "inr"
]

source_systems = [
    "TATA_CORE",
    "ICICI_API",
    "HDFC_BATCH",
    None
]

branch_codes = [
    "BR001",
    "BR002",
    "BR003",
    None
]

# -------------------------------------------------
# Helper functions
# -------------------------------------------------

def random_balance():

    return random.choice([
        round(random.uniform(0, 500000), 2),
        -1000.00,
        None
    ])


def random_date():

    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "INVALID_DATE",
        None
    ]

    fmt = random.choice(formats)

    if fmt is None or fmt == "INVALID_DATE":
        return fmt

    return (
        datetime.now() -
        timedelta(days=random.randint(0, 3650))
    ).strftime(fmt)


# -------------------------------------------------
# Generate records
# -------------------------------------------------

records = []

for _ in range(1000):

    record = (

        f"ACC_{random.randint(1,500)}",
        f"CUST_{random.randint(1,300)}",

        str(random.randint(1000000000, 9999999999)),

        random.choice(account_types),
        random.choice(account_statuses),

        random_date(),

        random_balance(),

        random.choice(currencies),
        random.choice(branch_codes),
        random.choice(source_systems)

    )

    records.append(record)

columns = [

    "account_id",
    "customer_id",
    "account_number",
    "account_type",
    "account_status",
    "account_open_date",
    "balance",
    "currency",
    "branch_code",
    "source_system"

]

df = spark.createDataFrame(records, columns)

# add ingestion date
df = df.withColumn("ingestion_date", current_date())

# cast balance properly
df = df.withColumn(
    "balance",
    df["balance"].cast(DecimalType(18,2))
)

# -------------------------------------------------
# Write to Bronze table
# -------------------------------------------------

df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("account_db.account_master_raw")

print("✅ Dirty account data generated")

spark.stop()
