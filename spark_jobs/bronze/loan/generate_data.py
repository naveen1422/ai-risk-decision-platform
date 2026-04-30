from core.spark_session import spark
from pyspark.sql.functions import current_date, col
from pyspark.sql.types import DecimalType, IntegerType
import random

loan_types = ["PERSONAL", "HOME", "AUTO", None]
loan_statuses = ["ACTIVE", "CLOSED", "DEFAULTED", None]
source_systems = ["TATA_CORE", "ICICI_API", None]

def random_amount():
    return random.choice([
        round(random.uniform(10000, 1000000),2),
        -5000.00,
        None
    ])

records = []

for _ in range(1000):

    record = (
        f"LN_{random.randint(1,200)}",
        f"CUST_{random.randint(1,300)}",
        random_amount(),
        random.choice([7.5, 8.5, None]),
        random.choice(loan_types),
        random.choice(loan_statuses),
        random.choice(["2024-01-01", None, "INVALID"]),
        random.choice([12, 24, 36, None]),
        random.choice(source_systems)
    )

    records.append(record)

columns = [
    "loan_id",
    "customer_id",
    "loan_amount",
    "interest_rate",
    "loan_type",
    "loan_status",
    "disbursement_date",
    "loan_term_months",
    "source_system"
]

df = spark.createDataFrame(records, columns)

df = df.withColumn("ingestion_date", current_date())

# Fix 1
df = df.withColumn(
    "loan_amount",
    col("loan_amount").cast(DecimalType(18,2))
)

# Fix 2
df = df.withColumn(
    "interest_rate",
    col("interest_rate").cast(DecimalType(5,2))
)

# Fix 3 (NEW FIX)
df = df.withColumn(
    "loan_term_months",
    col("loan_term_months").cast(IntegerType())
)

df.write.format("delta") \
    .mode("append") \
    .saveAsTable("loan_db.loan_master_raw")

print("Loan dirty data loaded successfully")

spark.stop()
