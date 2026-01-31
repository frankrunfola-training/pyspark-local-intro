import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/10_spark_sql_advanced/10_02_ctes.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.10_spark_sql_advanced.10_02_ctes
# -----------------------------------------------------------------------
# Description:
#   CTEs (WITH ...) to structure SQL transformations the way you'd do it in real pipelines.
#########################################################################

from src.spark_utils import get_spark

spark = get_spark("10_02_ctes")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

txns.createOrReplaceTempView("txns")

q = """
WITH base AS (
  SELECT
    customer_id,
    CAST(amount AS DOUBLE) AS amount
  FROM txns
  WHERE amount IS NOT NULL
),
clean AS (
  SELECT *
  FROM base
  WHERE amount > 0
),
kpis AS (
  SELECT
    customer_id,
    COUNT(*) AS txn_cnt,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
  FROM clean
  GROUP BY customer_id
)
SELECT *
FROM kpis
ORDER BY total_amount DESC
LIMIT 10
"""

print("\n--- CTE KPIs ---")
spark.sql(q).show(truncate=False)

spark.stop()
