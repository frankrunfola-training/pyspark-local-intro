import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_05_sql_perf_hints.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_05_sql_perf_hints
# -----------------------------------------------------------------------
# Description:
#   SQL performance hints: caching temp views and using broadcast hints in SQL.
#########################################################################

from src.spark_utils import get_spark

spark = get_spark("10_05_sql_perf_hints")
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

# Cache the dimension table (common pattern when reused across many queries)
spark.sql("CACHE TABLE customers")

q = """
SELECT /*+ BROADCAST(c) */
  c.state,
  COUNT(*) AS txn_cnt,
  ROUND(SUM(t.amount), 2) AS total_amount
FROM txns t
LEFT JOIN customers c
  ON t.customer_id = c.customer_id
WHERE t.amount IS NOT NULL AND t.amount > 0
GROUP BY c.state
ORDER BY total_amount DESC
"""

df = spark.sql(q)

print("\n--- state rollup ---")
df.show(truncate=False)

print("\n--- explain (broadcast hint may show) ---")
df.explain(True)

spark.stop()
