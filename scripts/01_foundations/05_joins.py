###################################################################################
# File   : scripts/05_foundations/05_joins.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.01_foundations.05_joins
# -----------------------------------------------------------------------
# Description:
#   Join patterns used in real data engineering:
#   - fact table (transactions) + dimension table (customers)
#   - dimension table + KPI table (aggregations)
# -----------------------------------------------------------------------
# Dependencies:
#   - Expects outputs written by prior steps:
#       02_cleaning.py   -> data/out/customers_clean, data/out/txns_clean
#       03_aggregations.py -> data/out/customer_kpis
###################################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("05_joins")
spark.sparkContext.setLogLevel("ERROR")  # keep local console output readable

# -----------------------------------------------------------------------
# Read cleaned datasets (Silver-like)
# Why:
# - joining on cleaned data reduces nulls, bad keys, and confusing results
# -----------------------------------------------------------------------
customers_clean = spark.read.parquet("data/out/customers_clean")
txns_clean = spark.read.parquet("data/out/txns_clean")

# Read KPI output (Gold-like)
kpis = spark.read.parquet("data/out/customer_kpis")

# -----------------------------------------------------------------------
# Sanity checks (counts)
# Count triggers Spark jobs — intentionally used here for quick validation.
# -----------------------------------------------------------------------
customers_count = customers_clean.count()
txns_count = txns_clean.count()
kpis_count = kpis.count()

print("\n--- Sanity check ---")
print("customers_clean rows:", customers_count)
print("txns_clean rows:", txns_count)
print("customer_kpis rows:", kpis_count)

###################################################################################
# 1) Join transactions (fact) to customers (dimension)
# Goal:
# - enrich each transaction with customer attributes (first_name, state, etc.)
#
# Join type: LEFT
# - keep every transaction even if customer info is missing (good for debugging)
###################################################################################
enriched_txns = (
    txns_clean.join(customers_clean, on="customer_id", how="left")
)

# Optional: check for unmatched joins (transactions with missing customer fields)
unmatched_txns = enriched_txns.where(F.col("first_name").isNull()).count()

print("\n--- Join quality check ---")
print("transactions with no matching customer:", unmatched_txns)

print("\n--- Enriched transactions (sample) ---")
(
    enriched_txns
    .select("txn_id", "customer_id", "first_name", "state", "amount", "merchant")
    .show(10, truncate=False)
)

###################################################################################
# 2) Join KPIs to customers (one row per customer)
# Goal:
# - create a “customer analytics” table: customer attributes + aggregated metrics
#
# Join type: LEFT
# - keep all customers even if they have no transactions (KPIs will be null)
# - then fill KPI nulls to make the table easier to use downstream
###################################################################################
customer_analytics = (
    customers_clean
    .join(kpis, on="customer_id", how="left")
    .fillna({"txn_count": 0, "total_spend": 0.0, "avg_spend": 0.0})
)

# KPI sanity check: customers without transactions should have txn_count == 0
no_txn_customers = customer_analytics.where(F.col("txn_count") == 0).count()

print("\n--- Customer analytics checks ---")
print("customers with txn_count == 0:", no_txn_customers)

print("\n--- Top 10 customers by total_spend ---")
(
    customer_analytics
    .select("customer_id", "first_name", "state", "txn_count", "total_spend", "avg_spend", "spend_rank")
    .orderBy(F.desc("total_spend"))
    .show(10, truncate=False)
)

# -----------------------------
# Write output (Gold-like analytics)
# NOTE: Spark writes a folder of part files; that's normal.
# -----------------------------
customer_analytics.write.mode("overwrite").parquet("data/out/customer_analytics")

spark.stop()
