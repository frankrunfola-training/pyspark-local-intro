import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/06_exercises/05_excercises.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.06_exercises.05_excercises
# -----------------------------------------------------------------------
# Description:
#   Combined exercises (Select/Filter + Cleaning/Quarantine + KPIs + Joins).
#   20 exercises total. Start easy, get progressively more difficult.
#   Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("05_excercises")

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
# EXERCISE 01 (Easy)
#------------------------------------------------------------------------
# Goal: Select specific columns from customers: customer_id, first_name, state
#########################################################################
ex01_df = (
    # TODO
    customers.select("customer_id", "first_name", "state")
)
#ex01_df.show(truncate=False)

#########################################################################
# EXERCISE 02 (Easy)
#------------------------------------------------------------------------
# Goal: Filter customers where state == "CA"
# Return columns: customer_id, first_name, state
#########################################################################
ex02_df = (
    # TODO
    customers.filter(F.col("state")=="CA")
)
#ex02_df.show(truncate=False)

#########################################################################
# EXERCISE 03 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where amount > 50
# Return columns: txn_id, customer_id, amount
#########################################################################
ex03_df = (
    # TODO
    txns
    .filter(F.col("amount")>50)
    .select("txn_id","customer_id","amount")
)
#ex03_df.show(truncate=False)

#########################################################################
# EXERCISE 04 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant == "ElectroMart"
# Return columns: txn_id, customer_id, merchant, amount
#########################################################################
ex04_df = (
    # TODO
    txns
    .filter(F.col("merchant")=="ElectroMart")
    .select("txn_id","customer_id","merchant","amount")
)
#ex04_df.show(truncate=False)

#########################################################################
# EXERCISE 05 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where customer_id is in {1,2,3}
# Return columns: txn_id, customer_id, txn_ts, amount
#########################################################################
ex05_df = (
    # TODO
    txns
    .filter(F.col("customer_id").isin(1,2,3))
    .select("txn_id","customer_id","txn_ts","amount")
)
#ex05_df.show(truncate=False)

#########################################################################
# EXERCISE 06 (Easy)
# Goal: Filter customers where first_name is missing OR blank
# Return columns: customer_id, first_name, last_name
#########################################################################
ex06_df = (
    # TODO
    customers
    .filter((F.col("first_name").isNull()) | (F.trim(F.col("first_name"))==""))
    .select("customer_id","first_name","last_name")
)
#ex06_df.show(truncate=False)

#########################################################################
# EXERCISE 07 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where amount is between 10 and 100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
ex07_df = (
    # TODO
    txns
    .filter(F.col("amount").between(10,100))
    .select("txn_id","customer_id","amount","merchant")
)
#ex07_df.show(truncate=False)

#########################################################################
# EXERCISE 08 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant != "GroceryTown" AND amount 10->100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
ex08_df = (
    # TODO  
    txns
    .filter((F.col("merchant")!="GroceryTown") & (F.col("amount").between(10,100)))
    .select("txn_id","customer_id","amount","merchant")
)
#ex08_df.show(truncate=False)

#########################################################################
# EXERCISE 09 (Medium)
#------------------------------------------------------------------------
# Goal: Customers who signed up on or after 2025-03-01
# Return columns: customer_id, signup_date
#########################################################################
ex09_df = (
    customers
    .withColumn("signup_date", F.to_date("signup_date")) # Convert signup_date to a proper DATE type
    .filter(F.col("signup_date") >= F.lit("2025-03-01").cast("date"))
    .select("customer_id", "signup_date")
)
#ex09_df.show(truncate=False)

#########################################################################
# EXERCISE 10 (Medium)
#------------------------------------------------------------------------
# Goal: Create a new column on txns: amount_bucket with values:
# - "small" if amount < 20
# - "medium" if amount < 100
# - "large" otherwise
# Return columns: txn_id, amount, amount_bucket
#########################################################################
ex10_df = (
    # TODO
)
# ex10_df.show(truncate=False)

#########################################################################
# EXERCISE 11 (Medium)
#------------------------------------------------------------------------
# Goal: Standardize customers:
# - trim first_name and last_name
# - uppercase state
# Return full standardized customers DataFrame
#########################################################################
customers_std = (
    # TODO
)
# customers_std.show(truncate=False)

#########################################################################
# EXERCISE 12 (Medium)
#------------------------------------------------------------------------
# Goal: Create customers_quarantine where first_name is missing OR blank
# And customers_clean as the remaining records
#########################################################################
customers_quarantine = (
    # TODO
)

customers_clean = (
    # TODO
)
# print("customers_clean:", customers_clean.count(), "customers_quarantine:", customers_quarantine.count())

#########################################################################
# EXERCISE 13 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Create txns_quarantine where:
# - amount <= 0 OR customer_id is null
# And txns_clean as the remaining records
#########################################################################
txns_quarantine = (
    # TODO
)

txns_clean = (
    # TODO
)
# print("txns_clean:", txns_clean.count(), "txns_quarantine:", txns_quarantine.count())

#########################################################################
# EXERCISE 14 (Hard)
#------------------------------------------------------------------------
# Goal: Add a quarantine_reason column to txns_quarantine with values:
# - "non_positive_amount" when amount <= 0
# - "missing_customer_id" when customer_id is null
# (If both happen, choose one consistent rule)
#########################################################################
txns_quarantine_reasoned = (
    # TODO
)
# txns_quarantine_reasoned.show(truncate=False)

#########################################################################
# EXERCISE 15 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where customer_id does NOT exist in customers_clean
# Produce:
# - txns_bad_fk
# - txns_good_fk
#########################################################################
txns_bad_fk = (
    # TODO
)

txns_good_fk = (
    # TODO
)
# print("txns_good_fk:", txns_good_fk.count(), "txns_bad_fk:", txns_bad_fk.count())

#########################################################################
# EXERCISE 16 (Hard)
#------------------------------------------------------------------------
# Goal: Join txns_good_fk to customers_clean (left join) and return:
# txn_id, customer_id, first_name, state, amount, merchant
#########################################################################
enriched_txns = (
    # TODO
)
# enriched_txns.show(truncate=False)

#########################################################################
# EXERCISE 17 (Hard)
#------------------------------------------------------------------------
# Goal: Build customer KPIs from txns_good_fk:
# - txn_count
# - total_spend (rounded to 2 decimals)
# - avg_spend (rounded to 2 decimals)
# Return one row per customer_id
#########################################################################
customer_kpis = (
    # TODO
)
# customer_kpis.show(truncate=False)

#########################################################################
# EXERCISE 18 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: Add last_txn_ts (max txn_ts) to customer_kpis
# Return columns: customer_id, txn_count, total_spend, avg_spend, last_txn_ts
#########################################################################
customer_kpis_with_last = (
    # TODO
)
# customer_kpis_with_last.show(truncate=False)

#########################################################################
# EXERCISE 19 (Very Hard)
#------------------------------------------------------------------------
# Goal: Create customer_analytics by joining customers_clean to customer_kpis_with_last.
# Fill null KPI values with:
# - txn_count = 0
# - total_spend = 0.0
# - avg_spend = 0.0
#########################################################################
customer_analytics = (
    # TODO
)
# customer_analytics.orderBy(F.desc("total_spend")).show(truncate=False)

#########################################################################
# EXERCISE 20 (Very Hard)
#------------------------------------------------------------------------
# Goal: Write outputs (overwrite mode) to:
# - data/out/customers_clean
# - data/out/customers_quarantine
# - data/out/txns_good_fk
# - data/out/txns_bad_fk
# - data/out/customer_analytics
#########################################################################
# TODO

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
