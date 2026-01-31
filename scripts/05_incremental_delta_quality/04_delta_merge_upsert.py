import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/09_incremental_delta_quality/09_04_delta_merge_upsert.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_incremental_delta_quality.09_04_delta_merge_upsert
# -----------------------------------------------------------------------
# Description:
#   Delta MERGE (upsert) pattern for Silver tables (graceful if Delta isn't installed).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("09_04_delta_merge_upsert")
spark.sparkContext.setLogLevel("ERROR")

try:
    from delta.tables import DeltaTable
except Exception:
    print("\nDelta not available. Install delta-spark and configure SparkSession for Delta.")
    spark.stop()
    raise SystemExit(0)

# Simulate an existing Silver table
silver_path = "data/out/09_silver_customers_delta"

base = spark.createDataFrame(
    [(1, "Alice", "NY"), (2, "Bob", "CA"), (3, "Carla", "TX")],
    ["customer_id", "name", "state"]
)

(
    base.write.mode("overwrite")
    .format("delta")
    .save(silver_path)
)

# New increment: customer 2 changed state, customer 4 is new
increment = spark.createDataFrame(
    [(2, "Bob", "WA"), (4, "Dev", "FL")],
    ["customer_id", "name", "state"]
)

silver = DeltaTable.forPath(spark, silver_path)

(
    silver.alias("t")
    .merge(increment.alias("s"), "t.customer_id = s.customer_id")
    .whenMatchedUpdate(set={
        "name": "s.name",
        "state": "s.state",
    })
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "state": "s.state",
    })
    .execute()
)

print("\n--- after merge ---")
spark.read.format("delta").load(silver_path).orderBy("customer_id").show(truncate=False)

spark.stop()
