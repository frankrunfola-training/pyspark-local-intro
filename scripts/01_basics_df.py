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
#   python -m scripts.00_smoke_test.py
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
#print("\n--- Customers schema ---")
#customers.printSchema()

###################################################################################
# Return full Transactions data
###################################################################################
txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)
#print("\n--- Transactions schema ---")
#txns.printSchema()

###################################################################################
# Return  "customer_id", "first_name", "state" for CA customers
###################################################################################
ca_customers_df = (
    customers
    .select("customer_id", "first_name", "state")
    .filter(F.col("state") == "CA")
)
print("\n--- Select + filter example (CA customers) ---")
ca_customers_df.show(truncate=False)
print("ca_customers_df.count:", ca_customers_df.count())

###################################################################################
# Return transactions with amount > 100,  plus a new column "amount_with_tax"
###################################################################################
print("\n--- Create derived column (amount_bucket) ---")
txns2 = txns.withColumn(
    "amount_bucket",
    F.when(F.col("amount") < 20, "small")
     .when(F.col("amount") < 100, "medium")
     .otherwise("large")
)
txns2.select("txn_id", "amount", "amount_bucket").show()


spark.stop()