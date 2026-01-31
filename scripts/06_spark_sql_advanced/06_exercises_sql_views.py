import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_exercises_sql_views.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_exercises_sql_views
# ---------------------------------------------------------------------------------
# Description:
#   Spark SQL exercises (CTEs + windows + dedupe + cohort-style logic).
#   Mirrors real SQL-heavy DE interview tasks using temp views + spark.sql().
#
#   Exercises 71–80. Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("10_exercises_sql_views")

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

# Create temp views for SQL
customers.createOrReplaceTempView("customers")
txns.createOrReplaceTempView("txns")

#########################################################################
# EXERCISE 71 (Medium)
#------------------------------------------------------------------------
# Goal: SQL filter
# Customers who signed up on/after 2025-03-01
# Return columns: customer_id, signup_date
#########################################################################
ex71_df = (
    # TODO: spark.sql("...")
)
# ex71_df.show(truncate=False)

#########################################################################
# EXERCISE 72 (Medium)
#------------------------------------------------------------------------
# Goal: SQL CASE bucket
# Bucket txns.amount into: small (<20), medium (<100), large (else)
# Return: txn_id, amount, amount_bucket
#########################################################################
ex72_df = (
    # TODO
)
# ex72_df.show(truncate=False)

#########################################################################
# EXERCISE 73 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: SQL aggregate KPIs by customer
# Return: customer_id, txn_count, total_spend (2 decimals), avg_spend (2 decimals)
#########################################################################
ex73_df = (
    # TODO
)
# ex73_df.show(truncate=False)

#########################################################################
# EXERCISE 74 (Hard)
#------------------------------------------------------------------------
# Goal: SQL window function
# For each customer_id, rank txns by amount desc
# Return: customer_id, txn_id, amount, amount_rank
#########################################################################
ex74_df = (
    # TODO
)
# ex74_df.orderBy("customer_id", "amount_rank").show(truncate=False)

#########################################################################
# EXERCISE 75 (Hard)
#------------------------------------------------------------------------
# Goal: SQL top-N per group
# For each customer_id, keep TOP 2 transactions by amount
# Return: customer_id, txn_id, amount, merchant
#########################################################################
ex75_df = (
    # TODO
)
# ex75_df.orderBy("customer_id", F.desc("amount")).show(truncate=False)

#########################################################################
# EXERCISE 76 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: SQL anti-join pattern
# Customers with NO transactions
# Return: customer_id, first_name, last_name
#########################################################################
ex76_df = (
    # TODO
)
# ex76_df.show(truncate=False)

#########################################################################
# EXERCISE 77 (Very Hard)
#------------------------------------------------------------------------
# Goal: SQL dedupe
# Deduplicate customers by customer_id keeping latest signup_date
# Return: full customer row
#########################################################################
ex77_df = (
    # TODO
)
# ex77_df.show(truncate=False)

#########################################################################
# EXERCISE 78 (Very Hard)
#------------------------------------------------------------------------
# Goal: SQL daily aggregation
# Create txn_date from txn_ts and compute:
# - daily_txn_count
# - daily_total_spend (2 decimals)
# Return: txn_date, daily_txn_count, daily_total_spend
#########################################################################
ex78_df = (
    # TODO
)
# ex78_df.orderBy("txn_date").show(truncate=False)

#########################################################################
# EXERCISE 79 (Brutal)
#------------------------------------------------------------------------
# Goal: SQL cohort-style spend in first 30 days after signup
# - Parse signup_date
# - Parse txn_ts -> txn_date
# - Join customers to txns
# - Keep txns between signup_date and signup_date + 30 days (inclusive)
# - Group by signup_month (yyyy-MM)
# Return: signup_month, cohort_size (distinct customers), cohort_total_spend (2 decimals)
#########################################################################
ex79_df = (
    # TODO
)
# ex79_df.orderBy("signup_month").show(truncate=False)

#########################################################################
# EXERCISE 80 (Brutal)
#------------------------------------------------------------------------
# Goal: SQL data quality counts
# Create a report with counts for txns:
# - null_customer_id_count
# - non_positive_amount_count (amount <= 0)
# - invalid_customer_fk_count (customer_id not in customers)
# Return: one row with these three columns
#########################################################################
ex80_df = (
    # TODO
)
# ex80_df.show(truncate=False)

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
