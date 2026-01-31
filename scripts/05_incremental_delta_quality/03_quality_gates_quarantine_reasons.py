import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/09_incremental_delta_quality/09_03_quality_gates_quarantine_reasons.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_incremental_delta_quality.09_03_quality_gates_quarantine_reasons
# -----------------------------------------------------------------------
# Description:
#   Data quality gates: quarantine with explicit reasons (audit-friendly).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("09_03_quality_gates_quarantine_reasons")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

# Basic realistic rules:
# - customer_id missing
# - amount is null or <= 0
rules = [
    (F.col("customer_id").isNull(), F.lit("missing_customer_id")),
    (F.col("amount").isNull(), F.lit("missing_amount")),
    (F.col("amount") <= 0, F.lit("non_positive_amount")),
]

# Build a single reason column (first match wins)
reason = None
for cond, msg in rules:
    reason = msg.when(cond, msg) if reason is None else F.when(cond, msg).otherwise(reason)

with_reason = txns.withColumn("dq_reason", reason)

quarantine = with_reason.filter(F.col("dq_reason").isNotNull())
good = with_reason.filter(F.col("dq_reason").isNull()).drop("dq_reason")

print(f"\nGood: {good.count()} | Quarantine: {quarantine.count()}")
print("\n--- quarantine sample ---")
quarantine.select("dq_reason", *[c for c in txns.columns[:6]]).show(10, truncate=False)

good_out = "data/out/09_good_txns"
bad_out = "data/out/09_quarantine_txns"

good.write.mode("overwrite").parquet(good_out)
quarantine.write.mode("overwrite").parquet(bad_out)

print(f"\nWrote good -> {good_out}")
print(f"Wrote quarantine -> {bad_out}")

spark.stop()
