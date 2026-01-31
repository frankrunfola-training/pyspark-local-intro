import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_03_windows_dedupe.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_03_windows_dedupe
# -----------------------------------------------------------------------
# Description:
#   Window functions + dedupe: keep the latest record per key (classic DE task).
#########################################################################

from src.spark_utils import get_spark

spark = get_spark("10_03_windows_dedupe")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

# Try to auto-detect a timestamp/date column for ordering
order_col = None
for c in txns.columns:
    if c.lower() in {"txn_date", "transaction_date", "created_at", "event_ts", "txn_ts"}:
        order_col = c
        break

txns.createOrReplaceTempView("txns")

if not order_col:
    print("\nNo obvious date column found for dedupe ordering.")
    print("Add one (txn_date / created_at / etc) to make this a true 'latest record' drill.")
    spark.stop()
    raise SystemExit(0)

q = f"""
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY {order_col} DESC) AS rn
  FROM txns
)
SELECT customer_id, {order_col}, amount
FROM ranked
WHERE rn = 1
ORDER BY customer_id
LIMIT 20
"""

print("\n--- latest txn per customer ---")
spark.sql(q).show(truncate=False)

spark.stop()
