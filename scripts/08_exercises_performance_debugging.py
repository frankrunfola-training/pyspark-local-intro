import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : 08_exercises_performance_debugging.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.08_exercises_performance_debugging
# -----------------------------------------------------------------------
# Description:
#   Performance + debugging drills (Spark DE interview prep).
#   Focus: explain plans, shuffles, broadcast, AQE, skew, salting,
#   caching/persist, partition pruning, small files problem, checkpoints.
#
#   Exercises 43–55. Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
from src.spark_utils import get_spark

spark = get_spark("08_exercises_performance_debugging")

# Base reads (same data as your other scripts)
customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

#########################################################################
# EXERCISE 43 (Medium)
#------------------------------------------------------------------------
# Goal: Set Spark configs for learning:
# - spark.sql.shuffle.partitions = 8
# - spark.sql.adaptive.enabled = true
# Then print both values to confirm
#########################################################################
# TODO

#########################################################################
# EXERCISE 44 (Medium)
#------------------------------------------------------------------------
# Goal: Create a baseline join df: txns joined to customers on customer_id
# Return columns: txn_id, customer_id, state, amount, merchant
# Then call explain("formatted") on it
#########################################################################
join_baseline_df = (
    # TODO
)
# join_baseline_df.explain("formatted")

#########################################################################
# EXERCISE 45 (Hard)
#------------------------------------------------------------------------
# Goal: Force a broadcast join (dimension join pattern)
# Broadcast customers and join to txns on customer_id
# Return same columns as Exercise 44
# Then explain("formatted") to verify broadcast is used
#########################################################################
join_broadcast_df = (
    # TODO
)
# join_broadcast_df.explain("formatted")

#########################################################################
# EXERCISE 46 (Hard)
#------------------------------------------------------------------------
# Goal: Compare "count()" runtime (roughly) between baseline and broadcast join
# - time baseline_df.count()
# - time broadcast_df.count()
# Print both runtimes
#########################################################################
# TODO

#########################################################################
# EXERCISE 47 (Hard)
#------------------------------------------------------------------------
# Goal: Create a skewed dataset (synthetic) to simulate real-world skew
# Build skew_txns with columns: customer_id, amount
# - 90% of rows should be customer_id = 1
# - 10% spread across customer_id 2..100
# Target ~100k rows total (local-friendly)
#########################################################################
skew_txns = (
    # TODO
)
# print("skew_txns rows:", skew_txns.count())
# skew_txns.groupBy("customer_id").count().orderBy(F.desc("count")).show(10, False)

#########################################################################
# EXERCISE 48 (Very Hard)
#------------------------------------------------------------------------
# Goal: Join skew_txns to customers on customer_id and explain the plan
# Return columns: customer_id, state, amount
#########################################################################
skew_join_df = (
    # TODO
)
# skew_join_df.explain("formatted")

#########################################################################
# EXERCISE 49 (Very Hard)
#------------------------------------------------------------------------
# Goal: Skew mitigation with salting (classic technique)
# Create salted join:
# - Add a salt column to skew_txns with values 0..9 (10 buckets)
# - Expand customers rows for customer_id=1 into 10 salted copies (0..9)
# - Join on (customer_id, salt) so the heavy key spreads across partitions
# Return columns: customer_id, state, amount
#########################################################################
salted_skew_join_df = (
    # TODO
)
# salted_skew_join_df.explain("formatted")

#########################################################################
# EXERCISE 50 (Very Hard)
#------------------------------------------------------------------------
# Goal: Cache/persist correctness drill
# Build spend_per_customer from txns: customer_id, total_spend
# Persist it (MEMORY_AND_DISK), then run two actions:
# - count()
# - orderBy(desc(total_spend)).show(10, False)
#########################################################################
spend_per_customer = (
    # TODO
)
# spend_per_customer.persist(StorageLevel.MEMORY_AND_DISK)
# print("count:", spend_per_customer.count())
# spend_per_customer.orderBy(F.desc("total_spend")).show(10, False)

#########################################################################
# EXERCISE 51 (Hard)
#------------------------------------------------------------------------
# Goal: Partition pruning drill
# Write txns to parquet partitioned by merchant:
#   data/out/txns_by_merchant_parquet
# Then read it back and filter merchant == "GroceryTown"
# Explain the filtered plan (formatted) to see pruning
#########################################################################
# TODO: write partitioned parquet
txns_by_merchant = (
    # TODO: read parquet back
)

txns_grocery_only = (
    # TODO: filter merchant == "GroceryTown"
)
# txns_grocery_only.explain("formatted")
# txns_grocery_only.show(20, False)

#########################################################################
# EXERCISE 52 (Very Hard)
#------------------------------------------------------------------------
# Goal: Small files problem drill (simulate)
# Write txns out with too many partitions to:
#   data/out/txns_small_files_parquet
# Then "fix" it by coalescing to a small number (e.g., 2) and write to:
#   data/out/txns_fixed_files_parquet
#########################################################################
# TODO

#########################################################################
# EXERCISE 53 (Hard)
#------------------------------------------------------------------------
# Goal: Repartition vs coalesce behavior
# Create:
# - txns_rep_8 = txns.repartition(8, "customer_id")
# - txns_coal_2 = txns_rep_8.coalesce(2)
# Print number of partit
