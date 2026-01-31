import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_01_temp_views_sql_basics.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_01_temp_views_sql_basics
# -----------------------------------------------------------------------
# Description:
#   Spark SQL basics: create temp views and run SQL joins/filters.
#########################################################################

from src.spark_utils import get_spark

spark = get_spark("10_01_temp_views_sql_basics")
spark.sparkContext.setLogLevel("ERROR")

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

customers.createOrReplaceTempView("customers")
txns.createOrReplaceTempView("txns")

q = """
SELECT
  t.customer_id,
  COUNT(*) AS txn_cnt,
  ROUND(SUM(t.amount), 2) AS total_amount
FROM txns t
WHERE t.amount IS NOT NULL AND t.amount > 0
GROUP BY t.customer_id
ORDER BY total_amount DESC
LIMIT 10
"""

print("\n--- top customers by spend ---")
spark.sql(q).show(truncate=False)

spark.stop()
