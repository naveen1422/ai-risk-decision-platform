import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SPARK_EVENTS_DIR = f"file://{PROJECT_ROOT}/../spark-events"
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "spark-warehouse")


builder = (
    SparkSession.builder
    .appName("Defaulter-Platform")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .config(
    "spark.jars.packages",
    "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    # --- Hive Metastore (CRITICAL) ---
    .enableHiveSupport()

    # --- Spark UI ---
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")

    # --- Event logging ---
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", SPARK_EVENTS_DIR)

    # --- Performance defaults ---
    .config("spark.sql.shuffle.partitions", "8")

    # --- Delta Lake ---
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
