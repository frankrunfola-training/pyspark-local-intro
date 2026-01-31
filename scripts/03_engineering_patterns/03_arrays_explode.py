import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_03_arrays_explode.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_03_arrays_explode
# -----------------------------------------------------------------------
# Description:
#   Arrays + explode: build array columns, explode to rows, and re-aggregate.
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("07_03_arrays_explode")
spark.sparkContext.setLogLevel("ERROR")

# Pretend this is a product events dataset (arrays are common)
rows = [
    (101, "NY", "view,cart,checkout"),
    (102, "CA", "view,view,cart"),
    (103, "NY", "view"),
    (104, "TX", "view,cart"),
]
df = spark.createDataFrame(rows, ["customer_id", "state", "actions_csv"])

with_arrays = df.withColumn("actions", F.split(F.col("actions_csv"), ","))

print("\n--- array column ---")
with_arrays.show(truncate=False)

exploded = with_arrays.select("customer_id", "state", F.explode("actions").alias("action"))
print("\n--- exploded ---")
exploded.show(truncate=False)

# Count actions per state (simple but realistic)
counts = (
    exploded.groupBy("state", "action")
    .count()
    .orderBy("state", "action")
)

print("\n--- counts per state/action ---")
counts.show(truncate=False)

spark.stop()
