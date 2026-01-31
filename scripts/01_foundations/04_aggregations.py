import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : scripts/05_foundations/04_aggregations.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.05_foundations.04_aggregations
# -----------------------------------------------------------------------
# Description:
#   Aggregations + window functions to produce customer spend KPIs.
#
# Dependencies:
#   - Expects cleaned transactions written by `02_cleaning.py` at:
#       data/out/txns_clean
###################################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.spark_utils import get_spark

spark = get_spark("04_aggregations")
spark.sparkContext.setLogLevel("ERROR")  # keep local console output readable

# -----------------------------
# Read cleaned transactions (Silver-like)
# -----------------------------
txns_clean = spark.read.parquet("data/out/txns_clean")

# -----------------------------
# Basic sanity check: confirm we loaded data
# (Count is an action and triggers a Spark job.)
# -----------------------------
txns_clean_count = txns_clean.count()
print("\n--- Sanity check ---")
print("txns_clean rows:", txns_clean_count)

###################################################################################
# 1) Customer-level KPIs (groupBy + aggregate)
###################################################################################
kpis = (
    txns_clean
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_spend"),
        F.max("amount").alias("max_spend")
    )
)

# -----------------------------
# KPI sanity check: confirm we produced one row per customer_id
# -----------------------------
kpis_count = kpis.count()
print("\n--- KPI sanity check ---")
print("kpis rows (unique customers w/ txns):", kpis_count)

print("\n--- Customer KPIs (sorted by total spend) ---")
kpis.orderBy(F.desc("total_spend")).show(truncate=False)

###################################################################################
# 2) Rank customers by spend (window function)
###################################################################################
w = Window.orderBy(F.desc("total_spend"))
ranked = kpis.withColumn("spend_rank", F.dense_rank().over(w))

print("\n--- Ranked KPIs ---")
ranked.orderBy(F.asc("spend_rank")).show(truncate=False)

# -----------------------------
# Quick “human readable” output: Top 5 customers
# -----------------------------
print("\n--- Top 5 customers by total spend ---")
(
    ranked
    .select("customer_id", "total_spend", "txn_count", "spend_rank")
    .orderBy(F.asc("spend_rank"))
    .limit(5)
    .show(truncate=False)
)

# -----------------------------
# Write output (Gold-like analytics)
# NOTE: Spark writes a folder of part files; that's normal.
# -----------------------------
ranked.write.mode("overwrite").parquet("data/out/customer_kpis")

spark.stop()
