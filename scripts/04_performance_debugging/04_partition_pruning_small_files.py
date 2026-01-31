import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/08_performance_debugging/08_04_partition_pruning_small_files.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.08_performance_debugging.08_04_partition_pruning_small_files
# -----------------------------------------------------------------------
# Description:
#   Partition pruning + the small-files problem (demo-friendly version).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("08_04_partition_pruning_small_files")
spark.sparkContext.setLogLevel("ERROR")

customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
).select("customer_id", "state")

out_path = "data/out/08_customers_partitioned"

# Write partitioned by state (good) - but too many tiny files (bad) if you write with high partitions.
(
    customers.repartition(50)  # intentionally too many for local demo
    .write.mode("overwrite")
    .partitionBy("state")
    .parquet(out_path)
)

# Fix: coalesce to reduce files before write
out_path_fixed = "data/out/08_customers_partitioned_fixed"
(
    customers.coalesce(2)  # fewer files
    .write.mode("overwrite")
    .partitionBy("state")
    .parquet(out_path_fixed)
)

print(f"\nWrote (too many files) -> {out_path}")
print(f"Wrote (fewer files)     -> {out_path_fixed}")

# Partition pruning: filtering on state should prune partitions
ny = spark.read.parquet(out_path_fixed).filter(F.col("state") == "NY")
print("\n--- explain (PartitionFilters) ---")
ny.explain(True)

spark.stop()
