import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1] # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 03_aggregations.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd: 
#   cd /projects/pyspark-local-intro    
#   python -m scripts.03_aggregations
# ---------------------------------------------------------------------------------
# Description:
#   Aggregations + window: customer spend KPIs.
###################################################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.spark_utils import get_spark

spark = get_spark("03_aggregations")

txns_clean = spark.read.parquet("data/out/txns_clean")

###################################################################################
# 1) Customer-level KPIs
###################################################################################
kpis = (
    txns_clean.groupBy("customer_id")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_spend"),
        F.max("amount").alias("max_spend")
    )
)

print("\n--- Customer KPIs ---")
kpis.orderBy(F.desc("total_spend")).show()

###################################################################################
# 2) Rank customers by spend
###################################################################################
w = Window.orderBy(F.desc("total_spend"))
ranked = kpis.withColumn("spend_rank", F.dense_rank().over(w))

print("\n--- Ranked KPIs ---")
ranked.show()

ranked.write.mode("overwrite").parquet("data/out/customer_kpis")

spark.stop()
