import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : scripts/05_foundations/02_basics_df.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.05_foundations.02_basics_df
# -----------------------------------------------------------------------
# Description:
#   DataFrame basics:
#   - read CSVs
#   - view schema + sample rows
#   - select + filter
#   - withColumn for derived columns#
# Notes:
#   - This is intentionally small + local (training mode).
#   - inferSchema is convenient for demos; production pipelines often use explicit schemas.
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("02_basics_df")
spark.sparkContext.setLogLevel("ERROR")  # reduce Spark startup noise for local runs

###################################################################################
# 1) LOAD CUSTOMERS (raw input)
# Why:
# - demonstrate CSV read patterns
# - verify Spark can infer schema and load header columns correctly
###################################################################################
customers = (
    spark.read.option("header", True)       # first row contains column names
    .option("inferSchema", True)            # infer types (ok for training)
    .csv("data/raw/customers.csv")
)

print("\n--- Customers schema (types) ---")
customers.printSchema()                     # schema = column names + data types

print("\n--- Customers sample rows ---")
customers.show(3, truncate=False)           # show a few rows without truncating values

###################################################################################
# 2) LOAD TRANSACTIONS (raw input)
# Why:
# - same read pattern, different dataset
# - sets you up for common DE actions (filters, derived cols, future joins)
###################################################################################
txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

print("\n--- Transactions schema (types) ---")
txns.printSchema()

print("\n--- Transactions sample rows ---")
txns.show(3, truncate=False)

###################################################################################
# 3) SELECT + FILTER (classic “slice of data” pattern)
# Goal:
# - return customer_id, first_name, state for CA customers
# Why:
# - shows column projection + row filtering
# - encourages best practice: assign DataFrame to a variable, then show/compute
###################################################################################
'''
ca_customers_df = (
    customers
    .select("customer_id", "first_name", "state")   # keep only the columns you need
    .filter(F.col("state") == "CA")                 # filter rows (where() works too)
)

print("\n--- Select + filter example (CA customers) ---")
ca_customers_df.show(truncate=False)                # action: prints output

# Count triggers a Spark job (another action). Useful for sanity checks.
print("ca_customers_df.count:", ca_customers_df.count())
'''

###################################################################################
# 4) WITHCOLUMN (derived columns / feature engineering / bucketing)
# Goal:
# - create amount_bucket based on transaction amount
# Why:
# - this is the same pattern used for business rules, segmentation, flags, etc.
###################################################################################
'''
txns2 = (
    txns
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 20, "small")
         .when(F.col("amount") < 100, "medium")
         .otherwise("large")
    )
)

print("\n--- Create derived column (amount_bucket) ---")
txns2.show(truncate=False)
'''

###################################################################################
# 5) WITHCOLUMN (simple dimension enrichment)
# Goal:
# - map state codes to readable state names (state_full)
# Why:
# - demonstrates rule-based enrichment without needing another lookup table yet
# - later you can replace this with a proper dimension table + join
###################################################################################
'''
customers2 = (
    customers
    .withColumn(
        "state_full",
        F.when(F.col("state") == "CA", "California")
         .when(F.col("state") == "NY", "New York")
         .when(F.col("state") == "TX", "Texas")
         .when(F.col("state") == "WA", "Washington")
         .otherwise("Other")
    )
)

print("\n--- Create derived column (state_full) ---")
customers2.show(truncate=False)
'''

# Always stop SparkSession to release resources (especially on local runs)
spark.stop()
