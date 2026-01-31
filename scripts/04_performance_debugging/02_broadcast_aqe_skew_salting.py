import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/08_performance_debugging/08_02_broadcast_aqe_skew_salting.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.08_performance_debugging.08_02_broadcast_aqe_skew_salting
# -----------------------------------------------------------------------
# Description:
#   Broadcast, AQE, skew + salting (conceptual + small runnable example).
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_utils import get_spark

spark = get_spark("08_02_broadcast_aqe_skew_salting")
spark.sparkContext.setLogLevel("ERROR")

# Turn on AQE (Adaptive Query Execution) for local learning
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Skewed fact table: many rows share the same key (customer_id=1)
fact_rows = [(1, float(i)) for i in range(1000)] + [(2, 10.0), (3, 20.0), (4, 30.0)]
fact = spark.createDataFrame(fact_rows, ["customer_id", "amount"])

# Small dimension table -> broadcast candidate
dim = spark.createDataFrame([(1, "NY"), (2, "CA"), (3, "TX"), (4, "FL")], ["customer_id", "state"])

joined = fact.join(F.broadcast(dim), on="customer_id", how="left")
print("\n--- explain (BroadcastHashJoin often shows up) ---")
joined.explain(True)

# Salting: spread the hot key across multiple partitions
SALT_BUCKETS = 10
salted_fact = fact.withColumn(
    "salt",
    F.when(F.col("customer_id") == 1, (F.rand() * SALT_BUCKETS).cast("int")).otherwise(F.lit(0))
)

salted_dim = (
    dim.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))
    .filter(F.col("customer_id") == 1)
).unionByName(
    dim.filter(F.col("customer_id") != 1).withColumn("salt", F.lit(0))
)

salted_join = salted_fact.join(salted_dim, on=["customer_id", "salt"], how="left")
print("\n--- salted join explain ---")
salted_join.explain(True)

print("\nCounts:")
print("joined:", joined.count(), "| salted_join:", salted_join.count())

spark.stop()
