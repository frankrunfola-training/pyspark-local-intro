import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/08_performance_debugging/08_05_debugging_playbook.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.08_performance_debugging.08_05_debugging_playbook
# -----------------------------------------------------------------------
# Description:
#   A practical debugging playbook: sanity checks, counting nulls, explain, and quick perf timing.
#########################################################################

import time
from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("08_05_debugging_playbook")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

print("\n--- schema ---")
txns.printSchema()

print("\n--- sample ---")
txns.show(5, truncate=False)

# 1) cheap sanity counts (catch surprises early)
row_count = txns.count()
null_amount = txns.filter(F.col("amount").isNull()).count()
non_positive_amount = txns.filter(F.col("amount") <= 0).count()

print(f"\nrows={row_count} | null_amount={null_amount} | amount<=0={non_positive_amount}")

# 2) narrow projection before heavy ops (good habit)
narrow = txns.select("customer_id", "amount").filter(F.col("amount").isNotNull())

# 3) measure: time your actions (local)
t0 = time.time()
agg = narrow.groupBy("customer_id").agg(F.sum("amount").alias("total_amount"))
agg.count()
t1 = time.time()
print(f"\nAgg action seconds: {t1 - t0:0.3f}")

# 4) explain is your best friend
print("\n--- explain ---")
agg.explain(True)

# 5) partitions + skew hints
print("\nPartitions:", narrow.rdd.getNumPartitions())
print("Top customers by txn count (skew check):")
narrow.groupBy("customer_id").count().orderBy(F.desc("count")).show(10, truncate=False)

spark.stop()
