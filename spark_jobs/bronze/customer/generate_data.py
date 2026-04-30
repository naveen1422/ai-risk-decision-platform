# -----------------------------------------------
# Dirty Customer Data Generator (Bronze Layer)
# -----------------------------------------------

from core.spark_session import spark
from pyspark.sql.functions import current_date
import random
from datetime import datetime, timedelta

# Dirty reference values
kyc_statuses = ["VERIFIED", "PENDING", "FAILED", None, "verified", "UNKNOWN"]
source_systems = ["TATA_CORE", "ICICI_API", "HDFC_BATCH", None]

cities = ["Mumbai", "Delhi", "Hyderabad", "Chennai", None, "mumbai"]
states = ["MH", "DL", "TS", "TN", None]
countries = ["India", None]

# Dirty helpers
def random_dob():
    formats = ["%Y-%m-%d", "%d-%m-%Y", "INVALID", None]
    fmt = random.choice(formats)

    if fmt is None or fmt == "INVALID":
        return fmt

    return (datetime.now() - timedelta(days=random.randint(7000, 20000))).strftime(fmt)

def random_phone():
    return random.choice([
        str(random.randint(6000000000, 9999999999)),
        None,
        "INVALID_PHONE"
    ])

# Generate records
records = []

for _ in range(1000):
    record = (
        f"CUST_{random.randint(1,300)}",  # duplicates
        random.choice(["Ramesh", "Suresh", "Anita", None]),
        random_dob(),
        random_phone(),
        random.choice(["test@email.com", None, "invalid_email"]),
        random.choice(["Street 1", None]),
        random.choice(cities),
        random.choice(states),
        random.choice(countries),
        random.choice(kyc_statuses),
        random.choice(source_systems)
    )

    records.append(record)

columns = [
    "customer_id",
    "customer_name",
    "date_of_birth",
    "phone_number",
    "email",
    "address",
    "city",
    "state",
    "country",
    "kyc_status",
    "source_system"
]

df = spark.createDataFrame(records, columns)

df = df.withColumn("ingestion_date", current_date())

df.write.format("delta") \
    .mode("append") \
    .saveAsTable("customer_db.customer_master_raw")

print("Customer dirty data loaded")

spark.stop()
