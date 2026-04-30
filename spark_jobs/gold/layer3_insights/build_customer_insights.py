from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, current_timestamp, when

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("RiskInsights") \
    .getOrCreate()

# -----------------------------
# PATHS
# -----------------------------
BASE_PATH = "/opt/project/data/risk_vault_db"

INPUT_PATH = f"{BASE_PATH}/customer_risk_profile"
OUTPUT_PATH = f"{BASE_PATH}/customer_risk_insights"

# -----------------------------
# LOAD DATA
# -----------------------------
df = spark.read.parquet(INPUT_PATH)

# -----------------------------
# CALCULATE METRICS
# -----------------------------
total_customers = df.count()

agg_df = df.agg(
    count(when(col("risk_bucket") == "HIGH", True)).alias("high_risk_count"),
    count(when(col("risk_bucket") == "MEDIUM", True)).alias("medium_risk_count"),
    count(when(col("risk_bucket") == "LOW", True)).alias("low_risk_count"),
    avg("risk_score").alias("avg_risk_score")
)

# add total + percentage
final_df = agg_df.withColumn("total_customers", col("high_risk_count") + col("medium_risk_count") + col("low_risk_count")) \
    .withColumn("high_risk_percentage", (col("high_risk_count") / col("total_customers")) * 100) \
    .withColumn("generated_at", current_timestamp())

# -----------------------------
# WRITE OUTPUT
# -----------------------------
import shutil
import os

if os.path.exists(OUTPUT_PATH):
    shutil.rmtree(OUTPUT_PATH)

final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ Gold Layer 3 (Insights) built successfully")

spark.stop()