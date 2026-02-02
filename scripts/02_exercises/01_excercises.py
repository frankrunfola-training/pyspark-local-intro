#########################################################################
# File   : scripts/06_exercises/05_excercises.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.02_exercises.01_excercises
# -----------------------------------------------------------------------
# Description:
#   Combined exercises (Select/Filter + Cleaning/Quarantine + KPIs + Joins).
#   22 exercises total. Start easy, get progressively more difficult.
#   Fill in TODOs. No hints.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

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
# Goal: Select specific columns from customers, transaction
#########################################################################
ex01_df = (
    # TODO
    customers.select("customer_id", "first_name","last_name", "state","signup_date")
)
ex01_df.show(n=3,truncate=False)

ex01_df = (
    # TODO
    txns.select("txn_id", "customer_id","txn_ts", "amount","merchant")
)
ex01_df.show(n=3,truncate=False)

#########################################################################
# EXERCISE 02 (Easy)
#------------------------------------------------------------------------
# Goal: Filter customers where state == "CA"
# Return columns: customer_id, first_name, state
#########################################################################
# TODO
'''
ex02_df = (
    customers.filter(F.col("state")=="CA")
)
ex02_df.show(truncate=False)'''

#########################################################################
# EXERCISE 03 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where amount > 50
# Return columns: txn_id, customer_id, amount
#########################################################################
# TODO
'''
ex03_df = (
    txns
    .filter(F.col("amount")>50)
    .select("txn_id","customer_id","amount")
)
ex03_df.show(truncate=False)'''

#########################################################################
# EXERCISE 04 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant == "ElectroMart"
# Return columns: txn_id, customer_id, merchant, amount
#########################################################################
# TODO
'''
ex04_df = (
    txns
    .filter(F.col("merchant")=="ElectroMart")
    .select("txn_id","customer_id","merchant","amount")
)
ex04_df.show(truncate=False)'''

#########################################################################
# EXERCISE 05 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where customer_id is in {1,2,3}
# Return columns: txn_id, customer_id, txn_ts, amount
#########################################################################
# TODO
'''
ex05_df = (
    txns
    .filter(F.col("customer_id").isin(1,2,3))
    .select("txn_id","customer_id","txn_ts","amount")
)
ex05_df.show(truncate=False)'''

#########################################################################
# EXERCISE 06 (Easy)
# Goal: Filter customers where first_name is missing OR blank
# Return columns: customer_id, first_name, last_name
#########################################################################
# TODO
'''
ex06_df = (
    customers
    .filter((F.col("first_name").isNull()) | (F.trim(F.col("first_name"))==""))
    .select("customer_id","first_name","last_name")
)
ex06_df.show(truncate=False)'''

#########################################################################
# EXERCISE 07 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where amount is between 10 and 100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
# TODO
'''
ex07_df = (
    txns
    .filter(F.col("amount").between(10,100))
    .select("txn_id","customer_id","amount","merchant")
)
ex07_df.show(truncate=False)'''

#########################################################################
# EXERCISE 08 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant != "GroceryTown" AND amount 10->100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
# TODO
'''
ex08_df = (
    txns
    .filter((F.col("merchant")!="GroceryTown") & (F.col("amount").between(10,100)))
    .select("txn_id","customer_id","amount","merchant")
)
ex08_df.show(truncate=False)'''

#########################################################################
# EXERCISE 09 (Medium)
#------------------------------------------------------------------------
# Goal: Customers who signed up on or after 2025-03-01
# Return columns: customer_id, signup_date
#########################################################################
# TODO
'''
ex09_df = (
    customers
    .withColumn("signup_date", F.to_date("signup_date")) # Convert signup_date to a proper DATE type
    .filter(F.col("signup_date") >= F.lit("2025-03-01").cast("date"))
    .select("customer_id", "signup_date")
)
ex09_df.show(truncate=False)'''

#########################################################################
# EXERCISE 10 (Medium)
#------------------------------------------------------------------------
# Goal: Create a new column on txns: amount_bucket with values:
# - "small" if amount < 20
# - "medium" if amount < 100
# - "large" otherwise
# Return columns: txn_id, amount, amount_bucket
#########################################################################
# TODO
'''
ex10_df = (
    txns
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 20,  F.lit("small"))
         .when(F.col("amount") < 100, F.lit("medium"))
         .otherwise(F.lit("large"))
    )
)
ex10_df.show(truncate=False)'''

#########################################################################
# EXERCISE 11 (Medium)
#------------------------------------------------------------------------
# Goal: Standardize customers:
#   - trim first_name and last_name
#   - uppercase state
# Return full standardized customers DataFrame
#########################################################################
# TODO
'''
customers_std = (
    customers
    .withColumn("first_name",F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("state"     ,F.upper(F.trim(F.col("state"))))
)
customers_std.show(truncate=False)'''

#########################################################################
# EXERCISE 12 (Medium)
#------------------------------------------------------------------------
# Goal: Create customers_quarantine where first_name is missing OR blank
# And customers_clean as the remaining records
#########################################################################
# TODO
'''
# Column that evaluates to True/False per row.
is_bad_first = F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")
cust_quarantine = customers.filter(is_bad_first)
cust_clean      = customers.filter(~is_bad_first)
#print("cust_clean:", cust_clean.count(), "cust_quarantine:", cust_quarantine.count())'''

#########################################################################
# EXERCISE 13 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Create txns_quarantine where:
#  - amount <= 0 OR customer_id is null
#  - And txns_clean as the remaining records
#########################################################################\
# TODO
'''
txns = txns.withColumn("amount", F.col("amount").cast("double"))  #first ensure amount is double
badData = (F.col("amount")<=0) | (F.col("customer_id").isNull())  #non-positive amt, or bad custId
txns_quarantine = txns.filter(badData)
txns_clean      = txns.filter(~badData)
print("txns_clean:", txns_clean.count(), "txns_quarantine:", txns_quarantine.count())
txns_quarantine.show(truncate=False)'''

#########################################################################
# EXERCISE 14 (Hard)
#------------------------------------------------------------------------
# Goal: Add a quarantine_reason column to txns_quarantine with values:
#   - "non_positive_amount" when amount <= 0
#   - "missing_customer_id" when customer_id is null
# (If both happen, choose one consistent rule)
#########################################################################
# TODO
'''
txns_quarantine_reasoned = (
    txns
    .withColumn(
        "quarantine_reason",
         F.when(F.col("customer_id").isNull(),F.lit("missing_customer_id"))
          .when(F.col("amount")<=0,F.lit("non_positive_amount"))
          .otherwise(F.lit(None)))
)
txns_quarantine_reasoned.show(truncate=False)'''

#########################################################################
# EXERCISE 15 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where customer_id does NOT exist in customers_clean
# Produce:
# - txns_bad_fk
# - txns_good_fk
#########################################################################
# TODO
'''
# txns with customer_id not found in cust_clean (bad FK)
txns_bad_fk = txns.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_anti"  #“rows in left that do not match” (exactly quarantine)
)
# txns with valid customer_id (good FK)
txns_good_fk = txns.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_semi"  #“rows in left that do match” (exactly clean)
)

print("txns_good_fk:", txns_good_fk.count(), "txns_bad_fk:", txns_bad_fk.count())'''


#########################################################################
# EXERCISE 16 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine orphan customers where customer_id has NO matching
#       transactions in txns_clean
## Produce:
# - customers_orphan   (customers with zero transactions)
# - customers_active   (customers with 1+ transactions)

# Rules:
# - Join key: customer_id
# - Treat null/blank customer_id in customers_clean as orphan automatically
# - Keep all original customer columns in both outputs
#########################################################################
# TODO

customers_orphan = (
)

customers_active = (
)

print("customers_active:", customers_active.count(), "customers_orphan:", customers_orphan.count())


#########################################################################
# EXERCISE 17 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where (state, txn_type) does NOT exist
#       in an allowed reference table (allowed_pairs)
#
# Inputs:
# - txns_clean: txn_id, customer_id, state, txn_type, amount
# - allowed_pairs: state, txn_type
#
# Produce:
# - txns_bad_ref    (invalid (state, txn_type) OR missing keys)
# - txns_good_ref   (remaining records)
#
# Rules:
# - Normalize for matching:
#   - state: trim + uppercase
#   - txn_type: trim + lowercase
# - If state OR txn_type is null/blank -> quarantine
# - Add quarantine_reason to txns_bad_ref with one of:
#   - "missing_state"
#   - "missing_txn_type"
#   - "invalid_state_txn_type"
#########################################################################
# TODO
"""
txns_bad_ref = (
)

txns_good_ref = (
)

print("txns_good_ref:", txns_good_ref.count(), "txns_bad_ref:", txns_bad_ref.count())
"""


#########################################################################
# EXERCISE 18 (Hard)
#------------------------------------------------------------------------
# Goal: Join txns_good_fk to customers_clean (left join) and return:
# txn_id, customer_id, first_name, state, amount, merchant
#########################################################################
# TODO
'''
enriched_txns = (
)
enriched_txns.show(truncate=False)'''

#########################################################################
# EXERCISE 19 (Hard)
#------------------------------------------------------------------------
# Goal: Build customer KPIs from txns_good_fk:
# - txn_count
# - total_spend (rounded to 2 decimals)
# - avg_spend (rounded to 2 decimals)
# Return one row per customer_id
#########################################################################
# TODO
'''
customer_kpis = (

)
customer_kpis.show(truncate=False)'''

#########################################################################
# EXERCISE 20 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: Add last_txn_ts (max txn_ts) to customer_kpis
# Return columns: customer_id, txn_count, total_spend, avg_spend, last_txn_ts
#########################################################################
# TODO
'''
customer_kpis_with_last = (

)
customer_kpis_with_last.show(truncate=False)'''

#########################################################################
# EXERCISE 21 (Very Hard)
#------------------------------------------------------------------------
# Goal: Create customer_analytics by joining customers_clean to customer_kpis_with_last.
# Fill null KPI values with:
# - txn_count = 0
# - total_spend = 0.0
# - avg_spend = 0.0
#########################################################################
# TODO
'''
customer_analytics = (

)
customer_analytics.orderBy(F.desc("total_spend")).show(truncate=False)'''

#########################################################################
# EXERCISE 22 (Very Hard)
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