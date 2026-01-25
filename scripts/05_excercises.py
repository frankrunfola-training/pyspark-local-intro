import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 05_exercises.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd: 
#   cd /projects/pyspark-local-intro    
#   python -m scripts.05_exercises
# ---------------------------------------------------------------------------------
# Description:
#   Fill in TODOs. Keep it readable.
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("05_exercises")

customers = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/customers.csv")
txns = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/transactions.csv")

###################################################################################
# EXERCISE 1: Fix customer names
# TODO:
# - trim first/last name
# - create full_name = "First Last"
###################################################################################
# customers_fixed = ...

###################################################################################
# EXERCISE 2: Quarantine transaction rules
# TODO:
# - quarantine if amount <= 0
# - quarantine if customer_id not in customers list
# - produce: txns_clean2, txns_quarantine2
# Hint: use a left_anti join or a broadcast join
###################################################################################
# txns_clean2 = ...
# txns_quarantine2 = ...

###################################################################################
# EXERCISE 3: KPI table
# TODO:
# - output one row per customer_id
# - columns: txn_count, total_spend, last_txn_ts
# - order by total_spend desc and show top 10
###################################################################################
# kpi2 = ...

###################################################################################
# EXERCISE 4: Write results
# TODO:
# - write txns_clean2 and kpi2 to data/out/
###################################################################################
# txns_clean2.write.mode("overwrite").parquet("data/out/txns_clean2")
# kpi2.write.mode("overwrite").parquet("data/out/customer_kpis2")

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
