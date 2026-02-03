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
#   24 exercises total. Start easy, get progressively more difficult.
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
# TODO
ex01_df = (customers.select("customer_id", "first_name","last_name", "state","signup_date"))
ex01_df.show(n=3,truncate=False)

ex01_df = (txns.select("txn_id", "customer_id","txn_ts", "amount","merchant"))
ex01_df.show(n=3,truncate=False)

#########################################################################
# EXERCISE 02 (Easy)
#------------------------------------------------------------------------
# Goal: Filter customers where state == "CA"
# Return columns: customer_id, first_name, state
#########################################################################
# TODO
'''
ex02_df = (customers.filter(F.col("state")=="CA"))
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
ex09_df.show(truncate=False)
'''

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
ex10_df.show(truncate=False)
'''

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
customers_std.show(truncate=False)
'''

#########################################################################
# EXERCISE 12 (Medium)
#------------------------------------------------------------------------
# Goal: Create customers_quarantine where first_name is missing OR blank
# And customers_clean as the remaining records
#########################################################################
# TODO
# Column that evaluates to True/False per row.
is_bad_first = F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")
cust_quarantine = customers.filter(is_bad_first)
cust_clean      = customers.filter(~is_bad_first)
print("cust_clean:", cust_clean.count())
print("cust_quarantine:", cust_quarantine.count())
print("")

#########################################################################
# EXERCISE 13 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Create txns_quarantine where:
#  - amount <= 0 OR customer_id is null
#  - And txns_clean as the remaining records
#########################################################################\
# TODO
txns = txns.withColumn("amount", F.col("amount").cast("double"))  #first ensure amount is double
badData = (F.col("amount")<=0) | (F.col("customer_id").isNull())  #non-positive amt, or bad custId
txns_quarantine = txns.filter(badData)
txns_clean      = txns.filter(~badData)
print("txns_clean:", txns_clean.count())
print("txns_quarantine:", txns_quarantine.count())
print("")
#txns_quarantine.show(truncate=False)

#########################################################################
# EXERCISE 14 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where merchant does NOT exist in an allowed
#       merchant reference table (allowed_merchants)
#
# Produce:
#   - txns_bad_merchant
#   - txns_good_merchant
#
# Notes:
# - This is the same “reference integrity” idea as FK checks, but easier:
#   it’s a single-column lookup instead of a full customer join.
#########################################################################
# TODO
'''
# Normalize merchant for matching (trim + lowercase)
txns_norm = (
    txns_clean
    # TODO: create a normalized merchant column
)

allowed_norm = (
    allowed_merchants
    # TODO: normalize merchant the same way + drop duplicates
)

# txns with merchant NOT found in allowed_merchants (quarantine)
txns_bad_merchant = (
    # TODO: join with left_anti using the normalized merchant key
)

# txns with merchant found in allowed_merchants (clean)
txns_good_merchant = (
    # TODO: join with left_semi using the normalized merchant key
)

print(f"txns_good_merchant: ${txns_good_merchant.count()}")
print(f"txns_bad_merchant: ${txns_bad_merchant.count()}")
print("")
'''


#########################################################################
# EXERCISE 15 (Hard)
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
txns_quarantine_reasoned.show(truncate=False)
'''

#########################################################################
# EXERCISE 16 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where customer_id does NOT exist in customers_clean
# Produce:
#   - txns_bad_fk
#   - txns_good_fk
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

print(f"txns_good_fk: ${txns_good_fk.count()}")
print(f"txns_bad_fk: ${txns_bad_fk.count()}")
print("")'''


#########################################################################
# EXERCISE 17 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine orphan customers where customer_id has NO matching
#       transactions in txns_clean
#------------------------------------------------------------------------
## Produce:
# - customers_orphan   (customers with zero transactions)
# - customers_active   (customers with 1+ transactions)
#------------------------------------------------------------------------
# Rules:
# - Join key: customer_id
# - Treat null/blank customer_id in customers_clean as orphan automatically
# - Keep all original customer columns in both outputs
#########################################################################
# TODO
'''
badCustId = (  #Flag bad customer_id values (null or blank after trimming)
    F.col("customer_id").isNull()
    | (F.trim(F.col("customer_id").cast("string")) == "")
)

# Split customers by customer_id presence
customers_bad_id = customers.filter(badCustId)
customers_valid_id = customers.filter(~badCustId)

# Distinct customer_ids that appear in txns_clean (smaller join table)
txns_keys = txns_clean.select("customer_id").dropDuplicates()

# Orphan customers (valid id) = no matching transaction (left_anti = "not found")
customers_no_txns = customers_valid_id.join(txns_keys, on="customer_id", how="left_anti")

# Active customers (at least one matching transaction) (inner = "found")
customers_active = customers_valid_id.join(txns_keys, on="customer_id", how="inner")

# Final orphan set (bad-id customers + valid-id customers with no transactions)
customers_orphan = customers_bad_id.unionByName(customers_no_txns)

# Quick check: print counts for each group
print(f"customers_active: ${customers_active.count()}")
print(f"customers_orphan: ${customers_orphan.count()}")
print("")'''

#########################################################################
# EXERCISE 18 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine orphan transactions 
#   Where txns_clean.customer_id has NO matching customer in cust_clean
#------------------------------------------------------------------------
## Produce:
#   - txns_orphan_fk   (transactions with missing/invalid customer reference)
#   - txns_valid_fk    (transactions with a valid customer reference)
#------------------------------------------------------------------------
# Rules:
#   - Join key: customer_id
#   - Treat null/blank customer_id in txns_clean as orphan automatically
#   - Keep all original transaction columns in both outputs
#########################################################################
# TODO
'''
bad_cust_id = (  # Condition: txns_clean customer_id is missing/blank
    F.col("customer_id").isNull()
    | (F.trim(F.col("customer_id").cast("string")) == "")
)

# Split txns by customer_id presence
txns_bad_id = txns_clean.filter(bad_cust_id)
txns_with_id = txns_clean.filter(~bad_cust_id)

# Small reference table of valid customer_ids (dedup for faster joins)
cust_keys = cust_clean.select("customer_id").dropDuplicates()

# Orphan FK txns: customer_id NOT found in cust_clean
txns_missing_customer = txns_with_id.join(cust_keys, on="customer_id", how="left_anti")

# Valid FK txns (customer_id found in cust_clean)
txns_valid_fk = txns_with_id.join(cust_keys, on="customer_id", how="inner")

# Final orphan set  (missing/blank id) + (id present but not in customers)
txns_orphan_fk = txns_bad_id.unionByName(txns_missing_customer)

print(f"txns_valid_fk: ${txns_valid_fk.count()}")
print(f"txns_orphan_fk: ${txns_orphan_fk.count()}")
print("")'''


#########################################################################
# EXERCISE 19 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where (state, txn_type) does NOT exist
#       in an allowed reference table (allowed_pairs)
#------------------------------------------------------------------------
# Inputs:
#   - txns_clean: txn_id, customer_id, state, txn_type, amount
#   - allowed_pairs: state, txn_type
#------------------------------------------------------------------------
# Produce:
#   - txns_bad_ref    (invalid (state, txn_type) OR missing keys)
#   - txns_good_ref   (remaining records)
#------------------------------------------------------------------------
# Rules:
#   - Normalize for matching:
#      - state: trim + uppercase
#      - txn_type: trim + lowercase
#   - If state OR txn_type is null/blank -> quarantine
#   - Add quarantine_reason to txns_bad_ref with one of:
#      - "missing_state"
#      - "missing_txn_type"
#      - "invalid_state_txn_type"
#########################################################################
# TODO
# 1) Normalize both txns_clean and allowed_pairs for matching
'''
# Reference table: allowed (state, txn_type) combinations
allowed_pairs = spark.createDataFrame(
    [
        ("NY", "deposit"),
        ("NY", "withdrawal"),
        ("CA", "deposit"),
        ("CA", "withdrawal"),
        ("TX", "deposit"),
        ("FL", "withdrawal"),
    ],
    ["state", "txn_type"]
)

txns_norm = (
    txns_clean
    .withColumn("state_norm", F.upper(F.trim(F.col("state"))))
    .withColumn("txn_type_norm", F.lower(F.trim(F.col("txn_type"))))
)

allowed_norm = (
    allowed_pairs
    .withColumn("state_norm", F.upper(F.trim(F.col("state"))))
    .withColumn("txn_type_norm", F.lower(F.trim(F.col("txn_type"))))
    .select("state_norm", "txn_type_norm")
    .dropDuplicates()
)

# 2) Missing-key checks (Column expressions)
state_missing = txns_norm["state_norm"].isNull() | (txns_norm["state_norm"] == "")
type_missing  = txns_norm["txn_type_norm"].isNull() | (txns_norm["txn_type_norm"] == "")

txns_missing_state = txns_norm.filter(state_missing).withColumn("quarantine_reason", F.lit("missing_state"))
txns_missing_type  = txns_norm.filter(~state_missing & type_missing).withColumn("quarantine_reason", F.lit("missing_txn_type"))

# 3) Pair validity check (only where both keys are present)
txns_keys_present = txns_norm.filter(~state_missing & ~type_missing)

txns_invalid_pair = (
    txns_keys_present
    .join(allowed_norm, on=["state_norm", "txn_type_norm"], how="left_anti")
    .withColumn("quarantine_reason", F.lit("invalid_state_txn_type"))
)

txns_good_ref = txns_keys_present.join(allowed_norm, on=["state_norm", "txn_type_norm"], how="inner")

# 4) Combine all quarantined rows
txns_bad_ref = txns_missing_state.unionByName(txns_missing_type).unionByName(txns_invalid_pair)

print("txns_bad_ref:", txns_bad_ref.count())
print("txns_good_ref:", txns_good_ref.count())
print("")
'''


#########################################################################
# EXERCISE 20 (Hard)
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
# EXERCISE 21 (Hard)
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
# EXERCISE 22 (Hard -> Very Hard)
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
# EXERCISE 23 (Very Hard)
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
customer_analytics.orderBy(F.desc("total_spend")).show(truncate=False)
'''

#########################################################################
# EXERCISE 24 (Very Hard)
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