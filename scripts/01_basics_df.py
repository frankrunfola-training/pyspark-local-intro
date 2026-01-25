import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : scripts/01_basics_df.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd: 
#   cd /projects/pyspark-local-intro    
#   python -m scripts.01_basics_df
# ---------------------------------------------------------------------------------
# Description:
#   DataFrame basics: read CSVs, schema, select, filter, withColumn.
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("01_basics_df")

###################################################################################
#  Return full Customers data
###################################################################################
customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)
print("\n--- Customers schema ---")
customers.show(3, truncate=False)

###################################################################################
# Return full Transactions data
###################################################################################
txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)
print("\n--- Transactions schema ---")
txns.show(3, truncate=False)



###################################################################################
# SELECT + FILTER
# Return  "customer_id", "first_name", "state" for CA customers
###################################################################################
'''
ca_customers_df = (
    customers
    .select("customer_id", "first_name", "state")
    .filter(F.col("state") == "CA")
)
print("\n--- Select + filter example (CA customers) ---")
ca_customers_df.show(truncate=False)
print("ca_customers_df.count:", ca_customers_df.count())
'''



###################################################################################
# WITHCOLUMN
# Return transactions with amount > 100,  plus a new column "amount_with_tax"
###################################################################################
'''
txns2 = txns.withColumn(
    "amount_bucket",
    F.when(F.col("amount") < 20, "small")
     .when(F.col("amount") < 100, "medium")
     .otherwise("large")
)
print("\n--- Create derived column (amount_bucket) ---")
txns2.show(truncate=False)
'''


###################################################################################
# WITHCOLUMN
# Return customers plus a new column "state_full"
###################################################################################
'''
customers2 = customers.withColumn(
    "state_full",
    F.when(F.col("state") == "CA", "California")
    .when(F.col("state") == "NY", "New York")
    .when(F.col("state") == "TX", "Texas")
    .when(F.col("state") == "WA", "Washington")
    .otherwise("Other")
)
print("\n--- Create derived column (state_full) ---")
customers2.show(truncate=False)
'''


spark.stop()