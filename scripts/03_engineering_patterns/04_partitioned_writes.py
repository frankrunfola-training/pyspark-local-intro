import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_04_partitioned_writes.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_04_partitioned_writes
# -----------------------------------------------------------------------
# Description:
#   Partitioned writes: write parquet partitioned by a column, then demonstrate partition pruning with filters.
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("07_04_partitioned_writes")
spark.sparkContext.setLogLevel("ERROR")

customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
).select("customer_id", "first_name", "last_name", "state")

out_path = "data/out/07_customers_by_state_parquet"

# ----------------------------------------
# Write partitioned (state=NY/CA/...)
# ----------------------------------------
(
    customers.write.mode("overwrite")
    .partitionBy("state")
    .parquet(out_path)
)

print(f"\nWrote partitioned parquet to: {out_path}")

# ----------------------------------------
# Partition pruning demo:
# filtering on the partition column should read fewer files
# ----------------------------------------
ny = spark.read.parquet(out_path).filter(F.col("state") == "NY")
print("\n--- NY customers (partition pruned) ---")
ny.show(5, truncate=False)

print("\n--- explain (look for PartitionFilters) ---")
ny.explain(True)

spark.stop()
