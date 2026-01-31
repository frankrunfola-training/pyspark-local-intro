import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_05_broadcast_cache_explain_rdd.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_05_broadcast_cache_explain_rdd
# -----------------------------------------------------------------------
# Description:
#   Broadcast joins, caching, explain plans, plus tiny RDD fundamentals (because interviews still ask).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("07_05_broadcast_cache_explain_rdd")
spark.sparkContext.setLogLevel("ERROR")

customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
).select("customer_id", "state")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
).select("transaction_id", "customer_id", "amount")

# ----------------------------------------
# 1) Broadcast join hint (dimension table is "small")
# ----------------------------------------
joined = txns.join(F.broadcast(customers), on="customer_id", how="left")

print("\n--- explain (look for BroadcastHashJoin if Spark chooses it) ---")
joined.explain(True)

# ----------------------------------------
# 2) Cache to avoid recompute across multiple actions
# ----------------------------------------
joined_cached = joined.cache()
t1 = joined_cached.count()  # triggers compute + cache fill
t2 = joined_cached.filter(F.col("amount") > 100).count()  # should be faster locally

print(f"\nCached joined count: {t1} | amount>100 count: {t2}")

# ----------------------------------------
# 3) Tiny RDD fundamentals (map/filter/reduce)
# ----------------------------------------
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
evens = rdd.filter(lambda x: x % 2 == 0).map(lambda x: x * 10).collect()
total = rdd.reduce(lambda a, b: a + b)

print(f"\nRDD evens * 10: {evens}")
print(f"RDD sum: {total}")

spark.stop()
