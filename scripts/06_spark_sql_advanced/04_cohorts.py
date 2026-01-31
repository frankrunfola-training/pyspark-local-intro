import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_04_cohorts.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_04_cohorts
# -----------------------------------------------------------------------
# Description:
#   Cohort-style SQL: first activity month + activity by month index (retention-ish pattern).
#########################################################################

from src.spark_utils import get_spark

spark = get_spark("10_04_cohorts")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

# auto-detect date col
date_col = None
for c in txns.columns:
    if c.lower() in {"txn_date", "transaction_date", "created_at"}:
        date_col = c
        break

txns.createOrReplaceTempView("txns")

if not date_col:
    print("\nNo obvious date column found for cohort logic.")
    print("Add a date column (txn_date / transaction_date / created_at) to run this drill.")
    spark.stop()
    raise SystemExit(0)

q = f"""
WITH base AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', {date_col}) AS txn_month
  FROM txns
  WHERE {date_col} IS NOT NULL
),
first_month AS (
  SELECT customer_id, MIN(txn_month) AS cohort_month
  FROM base
  GROUP BY customer_id
),
joined AS (
  SELECT
    b.customer_id,
    f.cohort_month,
    b.txn_month,
    MONTHS_BETWEEN(b.txn_month, f.cohort_month) AS month_index
  FROM base b
  JOIN first_month f USING (customer_id)
)
SELECT
  cohort_month,
  month_index,
  COUNT(DISTINCT customer_id) AS active_customers
FROM joined
GROUP BY cohort_month, month_index
ORDER BY cohort_month, month_index
"""

print("\n--- cohort activity ---")
spark.sql(q).show(50, truncate=False)

spark.stop()
