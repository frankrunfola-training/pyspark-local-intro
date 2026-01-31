import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/09_incremental_delta_quality/09_05_incremental_gold_kpis.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_incremental_delta_quality.09_05_incremental_gold_kpis
# -----------------------------------------------------------------------
# Description:
#   Incremental Gold aggregates: update KPIs by processing only new batches (simple pattern).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("09_05_incremental_gold_kpis")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
).select("customer_id", "amount")

# Simulate a 'new batch' by sampling (in real life, you'd filter by watermark)
new_batch = txns.orderBy(F.rand()).limit(200)

gold_path = "data/out/09_gold_customer_kpis"

# If Gold exists, load it; else start empty
try:
    gold_existing = spark.read.parquet(gold_path)
    gold_exists = True
except Exception:
    gold_existing = spark.createDataFrame([], "customer_id int, txn_cnt long, total_amount double")
    gold_exists = False

new_kpis = new_batch.groupBy("customer_id").agg(
    F.count("*").alias("txn_cnt"),
    F.sum("amount").alias("total_amount"),
)

# Update pattern:
# - join old + new on key
# - add counts and sums
updated = (
    gold_existing.alias("g")
    .join(new_kpis.alias("n"), on="customer_id", how="full")
    .select(
        F.coalesce(F.col("g.customer_id"), F.col("n.customer_id")).alias("customer_id"),
        (F.coalesce(F.col("g.txn_cnt"), F.lit(0)) + F.coalesce(F.col("n.txn_cnt"), F.lit(0))).alias("txn_cnt"),
        (F.coalesce(F.col("g.total_amount"), F.lit(0.0)) + F.coalesce(F.col("n.total_amount"), F.lit(0.0))).alias("total_amount"),
    )
)

updated.write.mode("overwrite").parquet(gold_path)

print(f"\nGold existed: {gold_exists}")
print("\n--- updated gold KPIs sample ---")
updated.orderBy(F.desc("total_amount")).show(10, truncate=False)

spark.stop()
