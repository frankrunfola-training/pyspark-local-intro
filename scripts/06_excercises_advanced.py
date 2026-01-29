import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : 06_exercises_advanced.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.06_exercises_advanced
# -----------------------------------------------------------------------
# Description:
#   Advanced exercises (Windows + Dedupe + Anti-Join + Pivot + Cohorts).
#   10 exercises total (21–30). Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.spark_utils import get_spark

spark = get_spark("06_exercises_advanced")

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
# EXERCISE 21 (Medium)
#------------------------------------------------------------------------
# Goal: Deduplicate customers on customer_id
# Keep the "latest" record by signup_date (most recent)
# Return full deduped customers DataFrame
#########################################################################
customers_deduped = (
    # TODO
)
# customers_deduped.show(truncate=False)

#########################################################################
# EXERCISE 22 (Medium)
#------------------------------------------------------------------------
# Goal: Create txn_date from txn_ts and compute daily_total_spend
# Return columns: txn_date, daily_total_spend (rounded to 2 decimals)
#########################################################################
daily_spend = (
    # TODO
)
# daily_spend.orderBy("txn_date").show(truncate=False)

#########################################################################
# EXERCISE 23 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Top 3 merchants by total spend (descending)
# Return columns: merchant, total_spend (rounded to 2 decimals)
#########################################################################
top_merchants = (
    # TODO
)
# top_merchants.show(truncate=False)

#########################################################################
# EXERCISE 24 (Hard)
#------------------------------------------------------------------------
# Goal: For each customer_id, rank transactions by amount DESC
# Add column: amount_rank (1 = largest)
# Return columns: customer_id, txn_id, amount, amount_rank
#########################################################################
txns_ranked = (
    # TODO
)
# txns_ranked.orderBy("customer_id", "amount_rank").show(truncate=False)

#########################################################################
# EXERCISE 25 (Hard)
#------------------------------------------------------------------------
# Goal: For each customer_id, keep only the TOP 2 transactions by amount
# Return columns: customer_id, txn_id, amount, merchant
#########################################################################
txns_top2_per_customer = (
    # TODO
)
# txns_top2_per_customer.orderBy("customer_id", F.desc("amount")).show(truncate=False)

#########################################################################
# EXERCISE 26 (Hard)
#------------------------------------------------------------------------
# Goal: Customers with NO transactions (anti-join)
# Return columns: customer_id, first_name, last_name
#########################################################################
customers_no_txns = (
    # TODO
)
# customers_no_txns.show(truncate=False)

#########################################################################
# EXERCISE 27 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: "Suspicious activity" - customer has >= 3 txns in the SAME DAY
# Create txn_date from txn_ts
# Return columns: customer_id, txn_date, txn_count
#########################################################################
suspicious_daily_activity = (
    # TODO
)
# suspicious_daily_activity.orderBy(F.desc("txn_count")).show(truncate=False)

#########################################################################
# EXERCISE 28 (Very Hard)
#------------------------------------------------------------------------
# Goal: Merchant bucket using rules:
# - "Grocery" for merchant == "GroceryTown"
# - "Electronics" for merchant == "ElectroMart"
# - "Other" otherwise
# Return columns: txn_id, customer_id, merchant, merchant_bucket, amount
#########################################################################
txns_with_merchant_bucket = (
    # TODO
)
# txns_with_merchant_bucket.show(truncate=False)

#########################################################################
# EXERCISE 29 (Very Hard)
#------------------------------------------------------------------------
# Goal: Pivot spend by merchant (wide table)
# For each customer_id, create columns for each merchant with total spend
# Return one row per customer_id, missing values filled with 0.0
#########################################################################
customer_spend_pivot = (
    # TODO
)
# customer_spend_pivot.show(truncate=False)

#########################################################################
# EXERCISE 30 (Very Hard -> Brutal)
#------------------------------------------------------------------------
# Goal: Cohort spend in first 30 days after signup
# Steps:
# - Convert customers.signup_date to date
# - Convert txns.txn_ts to date (txn_date)
# - Join txns to customers on customer_id
# - Keep only txns where txn_date is between signup_date and signup_date + 30 days (inclusive)
# - Group by signup_month (yyyy-MM) and compute:
#   * cohort_size (distinct customers)
#   * cohort_total_spend (rounded to 2 decimals)
# Return columns: signup_month, cohort_size, cohort_total_spend
#########################################################################
cohort_30d_spend = (
    # TODO
)
# cohort_30d_spend.orderBy("signup_month").show(truncate=False)

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
