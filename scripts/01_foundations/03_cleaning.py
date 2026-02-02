###################################################################################
# File   : scripts/05_foundations/03_cleaning.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.01_foundations.03_cleaning
# -----------------------------------------------------------------------
# Description:
#   Cleaning + basic data quality rules.
#   Output pattern:
#     - Clean datasets: data that passes minimal quality checks
#     - Quarantine datasets: data that fails checks (kept for audit/debug)
###################################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("03_cleaning")
spark.sparkContext.setLogLevel("ERROR")  # reduce noisy Spark logs for local runs

# -----------------------------
# Read raw inputs (Bronze-like)
# -----------------------------
# NOTE: inferSchema is fine for training; in real pipelines you'd often define schemas explicitly.
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

###################################################################################
# 1) Standardize and trim text (basic normalization)
# Why:
#   - prevents false mismatches (e.g., " ca " vs "CA")
#   - keeps downstream joins/filters consistent
###################################################################################
customers_std = (
    customers
    .withColumn("first_name", F.trim(F.col("first_name")))  # remove leading/trailing spaces
    .withColumn("last_name", F.trim(F.col("last_name")))    # remove leading/trailing spaces
    .withColumn("state", F.upper(F.trim(F.col("state"))))   # normalize to uppercase state codes
)

###################################################################################
# 2) Customer quarantine rules (basic but realistic)
# Why quarantine:
#    - you don't silently drop bad data
#    - you can measure quality and debug upstream issues
## Rule:
#   - missing first_name (null or empty)
###################################################################################
bad_rows_cust = F.col("first_name").isNull() | F.trim((F.col("first_name") == ""))
cust_quarantine = customers_std.filter(bad_rows_cust)

# Clean customers are the remainder
# NOTE: subtract works for small training data. At scale, you often prefer joins (anti-join).
cust_clean = customers_std.filter(~bad_rows_cust)

###################################################################################
# 3) Transaction quarantine rules (basic but realistic)
# Rules:
#   - amount <= 0 (non-positive payment)
#   - customer_id missing (can't link to a customer)
###################################################################################
bad_rows_txn =  F.col("amount") <= 0 | (F.col("customer_id").isNull())
txn_quarantine = txns.filter(
   
)

# Clean transactions are the remainder
txn_clean = txns.subtract(txn_quarantine)

# -----------------------------
# Simple run summary (counts)
# Why:
#   - makes runs auditable
#   - easy sanity check during development
# -----------------------------
print("\n--- Customers clean/quarantine counts ---")
print("clean:", cust_clean.count(), "quarantine:", cust_quarantine.count())

print("\n--- Transactions clean/quarantine counts ---")
print("clean:", txn_clean.count(), "quarantine:", txn_quarantine.count())

# -----------------------------
# Write outputs (Silver-like)
# NOTE: Spark writes a folder with part files; that's normal.
# -----------------------------
cust_clean.write.mode("overwrite").parquet("data/out/customers_clean")
cust_quarantine.write.mode("overwrite").parquet("data/out/customers_quarantine")

txn_clean.write.mode("overwrite").parquet("data/out/txns_clean")
txn_quarantine.write.mode("overwrite").parquet("data/out/txns_quarantine")

spark.stop()
