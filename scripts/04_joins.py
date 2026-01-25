import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 04_joins.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd: 
#   cd /projects/pyspark-local-intro    
#   python -m scripts.04_joins
# ---------------------------------------------------------------------------------
# Description:
#   Join dim (customers) + fact (transactions) + KPIs.
###################################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("04_joins")

customers_clean = spark.read.parquet("data/out/customers_clean")
txns_clean = spark.read.parquet("data/out/txns_clean")
kpis = spark.read.parquet("data/out/customer_kpis")

###################################################################################
# 1) Join transactions to customers
###################################################################################
enriched_txns = (
    txns_clean.join(customers_clean, on="customer_id", how="left")
)

print("\n--- Enriched transactions ---")
enriched_txns.select("txn_id", "customer_id", "first_name", "state", "amount").show()

###################################################################################
# 2) Join KPIs to customers (one row per customer)
###################################################################################
customer_analytics = (
    customers_clean.join(kpis, on="customer_id", how="left")
    .fillna({"txn_count": 0, "total_spend": 0.0, "avg_spend": 0.0})
)

print("\n--- Customer analytics ---")
customer_analytics.orderBy(F.desc("total_spend")).show()

customer_analytics.write.mode("overwrite").parquet("data/out/customer_analytics")

spark.stop()
