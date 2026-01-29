import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 02_cleaning.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.02_cleaning
# -----------------------------------------------------------------------
# Description:
#   Cleaning + basic data quality rules.
#   Output pattern:
#     - Clean datasets: data that passes minimal quality checks
#     - Quarantine datasets: data that fails checks (kept for audit/debug)
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("02_cleaning")
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
# - prevents false mismatches (e.g., " ca " vs "CA")
# - keeps downstream joins/filters consistent
###################################################################################
customers_std = (
    customers
    .withColumn("first_name", F.trim(F.col("first_name")))         # remove leading/trailing spaces
    .withColumn("last_name", F.trim(F.col("last_name")))           # remove leading/trailing spaces
    .withColumn("state", F.upper(F.trim(F.col("state"))))          # normalize to uppercase state codes
)

###################################################################################
# 2) Customer quarantine rules (basic but realistic)
# Why quarantine:
# - you don't silently drop bad data
# - you can measure quality and debug upstream issues
#
# Rule:
# - missing first_name (null or empty)
###################################################################################
cust_quarantine = customers_std.where(
    F.col("first_name").isNull() | (F.col("first_name") == "")
)

# Clean customers are the remainder
# NOTE: subtract works for small training data. At scale, you often prefer joins (anti-join).
cust_clean = customers_std.subtract(cust_quarantine)

###################################################################################
# 3) Transaction quarantine rules (basic but realistic)
# Rules:
# - amount <= 0 (non-positive payment)
# - customer_id missing (can't link to a customer)
###################################################################################
txn_quarantine = txns.where(
    (F.col("amount") <= 0) |
    (F.col("customer_id").isNull())
)

# Clean transactions are the remainder
txn_clean = txns.subtract(txn_quarantine)

# -----------------------------
# Simple run summary (counts)
# Why:
# - makes runs auditable
# - easy sanity check during development
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
