from pyspark.sql.functions import col, expr, when, array, current_timestamp
from pyspark.sql import SparkSession
import shutil
import os

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("RiskJob") \
    .getOrCreate()

# -----------------------------
# LOCAL PATHS
# -----------------------------
BASE_PATH = "/opt/project/data/risk_vault_db"

FEATURES_PATH = f"{BASE_PATH}/customer_features"
RULES_PATH = f"{BASE_PATH}/rule_master"
OUTPUT_PATH = f"{BASE_PATH}/customer_risk_profile"

# -----------------------------
# 1. LOAD DATA
# -----------------------------
features_df = spark.read.parquet(FEATURES_PATH)

rules_df = spark.read.parquet(RULES_PATH) \
    .filter(col("is_active") == "Y") \
    .orderBy(col("priority"))

rules = rules_df.collect()

# -----------------------------
# 2. BUILD SAFE RISK SCORE
# -----------------------------
risk_expr = None

for rule in rules:
    condition = rule["rule_expression"]

    if condition is None:
        continue

    condition = condition.strip()

    # Skip invalid conditions
    if condition == "" or condition.lower() == "none":
        continue

    if all(op not in condition for op in [">", "<", "=", "!"]):
        print(f"⚠️ Skipping invalid rule: {condition}")
        continue

    case_stmt = f"CASE WHEN {condition} THEN {rule['risk_score']} ELSE 0 END"

    if risk_expr is None:
        risk_expr = case_stmt
    else:
        risk_expr = f"{risk_expr} + {case_stmt}"

# fallback if no valid rules
if risk_expr is None:
    risk_expr = "0"

risk_df = features_df.withColumn("risk_score", expr(risk_expr))

# -----------------------------
# 3. BUILD SAFE FLAGS
# -----------------------------
flag_exprs = []

for rule in rules:
    condition = rule["rule_expression"]

    if condition is None:
        continue

    condition = condition.strip()

    if condition == "" or condition.lower() == "none":
        continue

    if all(op not in condition for op in [">", "<", "=", "!"]):
        continue

    flag_exprs.append(
        when(expr(condition), rule["rule_name"])
    )

risk_df = risk_df.withColumn("risk_flags", array(*flag_exprs))

# remove nulls
risk_df = risk_df.withColumn(
    "risk_flags",
    expr("filter(risk_flags, x -> x is not null)")
)

# -----------------------------
# 4. RISK BUCKET
# -----------------------------
risk_df = risk_df.withColumn(
    "risk_bucket",
    when(col("risk_score") >= 70, "HIGH")
    .when(col("risk_score") >= 30, "MEDIUM")
    .otherwise("LOW")
)

# -----------------------------
# 5. FINAL OUTPUT
# -----------------------------
final_df = risk_df.select(
    "customer_id",
    "risk_score",
    "risk_bucket",
    "risk_flags"
).withColumn("last_updated", current_timestamp())

# -----------------------------
# 6. CLEAN OUTPUT PATH (CRITICAL FIX)
# -----------------------------
if os.path.exists(OUTPUT_PATH):
    print(f"🧹 Cleaning existing path: {OUTPUT_PATH}")
    shutil.rmtree(OUTPUT_PATH)

# -----------------------------
# 7. WRITE OUTPUT
# -----------------------------
final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ Gold Layer 2 built successfully (FINAL STABLE VERSION)")

spark.stop()