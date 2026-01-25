import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 02_cleaning.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd: 
#   cd /projects/pyspark-local-intro    
#   python -m scripts.02_cleaning
# ---------------------------------------------------------------------------------
# Description:
#   Cleaning + basic data quality rules. We create "clean" and "quarantine" datasets.
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("02_cleaning")

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
# 1) Standardize and trim text
###################################################################################
customers_std = (
    customers
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
)

###################################################################################
# 2) Quarantine rules (basic but realistic)
#    - missing first_name
###################################################################################
cust_quarantine = customers_std.where(F.col("first_name").isNull() | (F.col("first_name") == ""))
cust_clean = customers_std.subtract(cust_quarantine)

###################################################################################
# 3) Transactions rules
#    - amount <= 0
#    - customer_id missing
###################################################################################
txn_quarantine = txns.where(
    (F.col("amount") <= 0) |
    (F.col("customer_id").isNull())
)

txn_clean = txns.subtract(txn_quarantine)

print("\n--- Customers clean/quarantine counts ---")
print("clean:", cust_clean.count(), "quarantine:", cust_quarantine.count())

print("\n--- Transactions clean/quarantine counts ---")
print("clean:", txn_clean.count(), "quarantine:", txn_quarantine.count())

# Write outputs (Spark writes folders; that's normal)
cust_clean.write.mode("overwrite").parquet("data/out/customers_clean")
cust_quarantine.write.mode("overwrite").parquet("data/out/customers_quarantine")

txn_clean.write.mode("overwrite").parquet("data/out/txns_clean")
txn_quarantine.write.mode("overwrite").parquet("data/out/txns_quarantine")

spark.stop()
