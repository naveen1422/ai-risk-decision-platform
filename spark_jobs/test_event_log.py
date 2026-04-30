import sys
import os

# Add project root to PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from core.spark_session import spark

df = spark.range(0, 20_000_000)
df.groupBy(df.id % 10).count().show()

spark.stop()