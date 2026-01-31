import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/09_incremental_delta_quality/09_02_incremental_watermarks.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_incremental_delta_quality.09_02_incremental_watermarks
# -----------------------------------------------------------------------
# Description:
#   Incremental loads with watermarks: process only 'new' rows since last run.
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("09_02_incremental_watermarks")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

# Watermark (pretend this is stored in a control table)
# For demo: choose a fixed timestamp/date that exists in your dataset
last_processed = "2026-01-15"

# If your dataset has a timestamp/date column, set it here.
# Many people name it txn_ts, transaction_ts, created_at, etc.
date_col = None
for c in txns.columns:
    if c.lower() in {"txn_date", "transaction_date", "created_at", "event_ts", "txn_ts"}:
        date_col = c
        break

if not date_col:
    print("\nNo obvious date column found. Add one to your CSV for real incremental drills.")
    print("Expected one of: txn_date / transaction_date / created_at / event_ts / txn_ts")
    spark.stop()
    raise SystemExit(0)

new_batch = txns.filter(F.col(date_col) > F.lit(last_processed))
print(f"\nUsing watermark: {last_processed} on column: {date_col}")
print("new rows:", new_batch.count())
new_batch.show(5, truncate=False)

# Write the new batch to a landing path (Bronze increment)
out = "data/out/09_bronze_increment"
new_batch.write.mode("overwrite").parquet(out)
print(f"\nWrote incremental batch -> {out}")

spark.stop()
