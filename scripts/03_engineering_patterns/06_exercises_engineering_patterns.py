import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_exercises_engineering_patterns.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_exercises_engineering_patterns
# -----------------------------------------------------------------------
# Description:
#   Data Engineering-focused PySpark practice beyond basic filters/joins/KPIs.
#   Covers: schema enforcement, JSON parsing, arrays/explode, partitioned writes,
#   broadcast joins, caching, explain plans, and a small RDD fundamentals section.
#
#   12 exercises total (31–42). Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
from src.spark_utils import get_spark

spark = get_spark("07_exercises_engineering_patterns")

# Base reads (like your other files)
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
# EXERCISE 31 (Medium)
#------------------------------------------------------------------------
# Goal: Read customers.csv again BUT using an explicit schema (NO inferSchema)
# Return DataFrame: customers_typed
#########################################################################
customers_schema = T.StructType([
    # TODO: define fields + types that match customers.csv
])

customers_typed = (
    # TODO: read customers.csv using customers_schema
)
# customers_typed.printSchema()
# customers_typed.show(truncate=False)

#########################################################################
# EXERCISE 32 (Medium)
#------------------------------------------------------------------------
# Goal: Read transactions.csv again BUT using an explicit schema (NO inferSchema)
# Return DataFrame: txns_typed
#########################################################################
txns_schema = T.StructType([
    # TODO: define fields + types that match transactions.csv
])

txns_typed = (
    # TODO: read transactions.csv using txns_schema
)
# txns_typed.printSchema()
# txns_typed.show(truncate=False)

#########################################################################
# EXERCISE 33 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Create a "clean types" version of txns with:
# - txn_ts converted to timestamp (create txn_ts_ts)
# - txn_date converted to date (create txn_date)
# Return columns: txn_id, customer_id, txn_ts_ts, txn_date, amount, merchant
#########################################################################
txns_time = (
    # TODO
)
# txns_time.show(truncate=False)

#########################################################################
# EXERCISE 34 (Hard)
#------------------------------------------------------------------------
# Goal: Semi-structured JSON parsing
# Create a small DataFrame with columns: customer_id, profile_json
# profile_json contains JSON like: {"tier":"gold","email_opt_in":true,"tags":["a","b"]}
# Parse JSON into a struct column: profile
# Return columns: customer_id, profile.tier, profile.email_opt_in, explode(profile.tags) as tag
#########################################################################
json_rows = [
    # feel free to keep these as-is
    (1, '{"tier":"gold","email_opt_in":true,"tags":["vip","promo"]}'),
    (2, '{"tier":"silver","email_opt_in":false,"tags":["promo"]}'),
    (3, '{"tier":"bronze","email_opt_in":true,"tags":[]}'),
]

profiles_raw = spark.createDataFrame(json_rows, ["customer_id", "profile_json"])

profile_schema = T.StructType([
    # TODO: tier (string), email_opt_in (boolean), tags (array<string>)
])

profiles_parsed = (
    # TODO
)
# profiles_parsed.show(truncate=False)

#########################################################################
# EXERCISE 35 (Hard)
#------------------------------------------------------------------------
# Goal: Arrays + explode use case (no JSON)
# Create a DataFrame: customer_id, interests (array<string>)
# Explode interests to one row per interest
# Return columns: customer_id, interest
#########################################################################
interest_rows = [
    (1, ["sports", "music"]),
    (2, ["music"]),
    (3, []),
    (4, ["gaming", "sports", "finance"]),
]
interests_df = spark.createDataFrame(interest_rows, ["customer_id", "interests"])

customer_interests = (
    # TODO
)
# customer_interests.show(truncate=False)

#########################################################################
# EXERCISE 36 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: Broadcast join (dimension table join pattern)
# Create a small merchant_dim DataFrame mapping merchant -> category
# Join txns to merchant_dim and return:
# txn_id, customer_id, amount, merchant, category
# Use a broadcast join hint for merchant_dim
#########################################################################
merchant_dim_rows = [
    ("GroceryTown", "grocery"),
    ("ElectroMart", "electronics"),
    ("CoffeeBox", "food_bev"),
]
merchant_dim = spark.createDataFrame(merchant_dim_rows, ["merchant", "category"])

txns_with_category = (
    # TODO
)
# txns_with_category.show(truncate=False)

#########################################################################
# EXERCISE 37 (Very Hard)
#------------------------------------------------------------------------
# Goal: Partitioned write pattern
# Write customers to Parquet partitioned by state:
#   data/out/customers_by_state_parquet
# Then read it back into: customers_by_state_read
# Show count and schema
#########################################################################
# TODO: write partitioned parquet
customers_by_state_read = (
    # TODO: read parquet back
)
# print("customers_by_state_read:", customers_by_state_read.count())
# customers_by_state_read.printSchema()

#########################################################################
# EXERCISE 38 (Very Hard)
#------------------------------------------------------------------------
# Goal: Repartition/coalesce pattern
# - Repartition txns by customer_id into 4 partitions
# - Then coalesce to 1 partition (simulate "final output")
# Return: txns_repartitioned, txns_single_partition
#########################################################################
txns_repartitioned = (
    # TODO
)

txns_single_partition = (
    # TODO
)
# print("repartitioned partitions:", txns_repartitioned.rdd.getNumPartitions())
# print("single partitions:", txns_single_partition.rdd.getNumPartitions())

#########################################################################
# EXERCISE 39 (Very Hard)
#------------------------------------------------------------------------
# Goal: Cache/persist pattern
# - Build a heavy-ish aggregation: spend per customer
# - Persist it (MEMORY_AND_DISK)
# - Trigger an action twice (count then show) to prove reuse
# Return: spend_per_customer_cached
#########################################################################
spend_per_customer_cached = (
    # TODO
)
# print("count:", spend_per_customer_cached.count())
# spend_per_customer_cached.show(truncate=False)

#########################################################################
# EXERCISE 40 (Very Hard -> Brutal)
#------------------------------------------------------------------------
# Goal: Explain plan literacy
# Build a join between txns and customers, then:
# - call explain("formatted") (or explain(True))
# - return the joined DataFrame: join_plan_df
#########################################################################
join_plan_df = (
    # TODO
)
# join_plan_df.explain("formatted")

#########################################################################
# EXERCISE 41 (RDD - Medium)
#------------------------------------------------------------------------
# Goal: Basic RDD transforms (just fundamentals)
# From txns, create an RDD of (merchant, amount)
# Then compute total spend per merchant using reduceByKey
# Convert back to a DataFrame with columns: merchant, total_spend
#########################################################################
merchant_spend_df = (
    # TODO
)
# merchant_spend_df.orderBy(F.desc("total_spend")).show(truncate=False)

#########################################################################
# EXERCISE 42 (RDD - Hard)
#------------------------------------------------------------------------
# Goal: Partition awareness
# - Take txns.rdd and print the number of partitions
# - Repartition the RDD to 6 partitions
# - Compute count per partition (hint: mapPartitions)
# Return: rdd_partition_counts_df with columns: partition_id, row_count
#########################################################################
rdd_partition_counts_df = (
    # TODO
)
# rdd_partition_counts_df.orderBy("partition_id").show(truncate=False)

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
