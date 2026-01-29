import sys
import os
import uuid
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : 09_exercises_incremental_delta_quality.py
# Author : Frank Runfola
# Date   : 1/25/2026
# ---------------------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_exercises_incremental_delta_quality
# ---------------------------------------------------------------------------------
# Description:
#   DE prep beyond transformations:
#   - incremental loads using watermarks
#   - run logging + auditability
#   - data quality gates + quarantine with reasons
#   - Delta Lake MERGE (upserts) for "Silver"
#   - Gold aggregates updated incrementally
#
#   Exercises 56–70. Fill in TODOs. No hints.
# ---------------------------------------------------------------------------------
# Local setup (pip PySpark + Delta Lake)
#
# Why: Exercises 63/66/69 use Delta MERGE (upserts). Plain pyspark alone can’t do MERGE.
#
# 1) Create/activate venv (recommended)
#    python -m venv .venv
#    # Windows: .venv\Scripts\activate
#    # Mac/Linux: source .venv/bin/activate
#
# 2) Install compatible pyspark + delta-spark versions (PIN them)
#    - Delta and Spark versions must match (major/minor compatibility matters).
#    - Quick mapping:
#        * Delta 4.0.x  <-> Spark 4.0.x
#        * Delta 3.0.x–3.3.x <-> Spark 3.5.x
#        * Delta 2.4.x  <-> Spark 3.4.x
#
#    Option A (Spark 3.5.x — common & stable):
#      pip install "pyspark==3.5.3" "delta-spark==3.3.1"
#
#    Option B (Spark 4.0.x — newest line):
#      pip install "pyspark==4.0.0" "delta-spark==4.0.1"
#
#    (Check your current Spark version with:)
#      python -c "import pyspark; print(pyspark.__version__)"
#
# 3) Ensure your SparkSession is Delta-enabled (do this in src/spark_utils.get_spark)
#    Recommended approach (delta pip utility):
#
#      import pyspark
#      from delta.pip_utils import configure_spark_with_delta_pip
#
#      builder = (
#          pyspark.sql.SparkSession.builder
#          .appName(app_name)
#          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#      )
#      spark = configure_spark_with_delta_pip(builder).getOrCreate()
#
# 4) Quick sanity check (should succeed if Delta is wired correctly):
#      from delta.tables import DeltaTable
#      spark.range(1).write.format("delta").mode("overwrite").save("data/out/_delta_smoke_test")
#
# If you skip step 3, you’ll usually see errors like:
#   "Failed to find data source: delta" or MERGE not recognized.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_utils import get_spark

# Delta Lake is strongly recommended for these exercises.
# If delta isn't installed, several exercises will be blocked (MERGE).
try:
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except Exception:
    DELTA_AVAILABLE = False

spark = get_spark("09_exercises_incremental_delta_quality")

# Paths (local-friendly)
OUT_ROOT = Path("data/out")
WATERMARK_DIR = OUT_ROOT / "_watermarks"
RUN_LOG_DIR = OUT_ROOT / "run_logs"
SILVER_TXNS_DELTA = OUT_ROOT / "delta" / "txns_silver"
GOLD_DAILY_KPIS_DELTA = OUT_ROOT / "delta" / "daily_kpis_gold"
QUARANTINE_TXNS_PATH = OUT_ROOT / "quarantine" / "txns"
DQ_REPORTS_PATH = OUT_ROOT / "run_logs" / "dq_reports"

WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)

# Base reads (same data as your other scripts)
customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)

txns_raw = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

#########################################################################
# Helper metadata for run logging (use in multiple exercises)
#########################################################################
RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.utcnow().isoformat(timespec="seconds")

#########################################################################
# EXERCISE 56 (Medium)
#------------------------------------------------------------------------
# Goal: Standardize + type txns:
# - txn_ts_ts: parsed timestamp
# - txn_date: date derived from txn_ts_ts
# - amount: cast to double
# Return columns: txn_id, customer_id, txn_ts_ts, txn_date, amount, merchant
#########################################################################
txns_typed = (
    # TODO
)
# txns_typed.printSchema()
# txns_typed.show(truncate=False)

#########################################################################
# EXERCISE 57 (Medium)
#------------------------------------------------------------------------
# Goal: Create a synthetic "new batch" of transactions for incremental loading:
# - Use txns_typed as base
# - Create a DataFrame new_batch_txns that includes:
#   * some "new" rows (txn_date > existing max date)
#   * some "late arriving" rows (older txn_date but new txn_id)
#   * some "updates" (same txn_id, changed amount or merchant)
# Return: new_batch_txns (same schema/cols as txns_typed)
#########################################################################
new_batch_txns = (
    # TODO
)
# new_batch_txns.show(truncate=False)

#########################################################################
# EXERCISE 58 (Hard)
#------------------------------------------------------------------------
# Goal: Implement watermark read/write:
# - Watermark file path: data/out/_watermarks/txns_last_txn_date.txt
# - If file doesn't exist, default watermark = '1900-01-01'
# Return: watermark_date_str (yyyy-MM-dd)
#########################################################################
WATERMARK_FILE = WATERMARK_DIR / "txns_last_txn_date.txt"

watermark_date_str = (
    # TODO
)
# print("watermark_date_str:", watermark_date_str)

#########################################################################
# EXERCISE 59 (Hard)
#------------------------------------------------------------------------
# Goal: Filter incremental candidate rows from new_batch_txns:
# - Keep rows where txn_date >= watermark_date_str (date compare)
# Return: incremental_candidates
#########################################################################
incremental_candidates = (
    # TODO
)
# incremental_candidates.show(truncate=False)

#########################################################################
# EXERCISE 60 (Hard)
#------------------------------------------------------------------------
# Goal: Data quality quarantine rules on incremental_candidates:
# Quarantine rows where ANY of:
# - txn_id is null/blank
# - customer_id is null
# - txn_ts_ts is null
# - amount is null OR amount <= 0
# - merchant is null/blank
#
# Create:
# - txns_quarantine (with quarantine_reason string column)
# - txns_valid (remaining rows)
#
# quarantine_reason rules (pick ONE consistent priority):
# - missing_txn_id
# - missing_customer_id
# - bad_timestamp
# - bad_amount
# - missing_merchant
#########################################################################
txns_quarantine = (
    # TODO
)

txns_valid = (
    # TODO
)
# print("valid:", txns_valid.count(), "quarantine:", txns_quarantine.count())
# txns_quarantine.show(truncate=False)

#########################################################################
# EXERCISE 61 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: FK check (customer_id must exist in customers)
# - Produce txns_bad_fk and txns_good_fk from txns_valid
# - Use a left_anti join for bad FK
#########################################################################
txns_bad_fk = (
    # TODO
)

txns_good_fk = (
    # TODO
)
# print("good_fk:", txns_good_fk.count(), "bad_fk:", txns_bad_fk.count())

#########################################################################
# EXERCISE 62 (Very Hard)
#------------------------------------------------------------------------
# Goal: Write quarantine outputs:
# - Append txns_quarantine to: data/out/quarantine/txns (parquet)
# - Append txns_bad_fk to the SAME quarantine area with reason "bad_fk"
# (Store a quarantine_reason column for both.)
#########################################################################
# TODO

#########################################################################
# EXERCISE 63 (Very Hard)
#------------------------------------------------------------------------
# Goal: Create (or upsert into) Silver Delta table for transactions:
# - Path: data/out/delta/txns_silver
# - Key: txn_id
# - Upsert from txns_good_fk:
#   * if matched: update amount, merchant, txn_ts_ts, txn_date, customer_id
#   * if not matched: insert all columns
#
# Return: silver_txns_df (read back the delta table)
#########################################################################
if not DELTA_AVAILABLE:
    print("\nDelta not available: install delta-spark and enable Delta in SparkSession.\n")

silver_txns_df = (
    # TODO
)
# silver_txns_df.show(truncate=False)

#########################################################################
# EXERCISE 64 (Very Hard)
#------------------------------------------------------------------------
# Goal: Update watermark after successful Silver upsert:
# - New watermark = max(txn_date) from txns_good_fk (or silver_txns_df)
# - Write it back to WATERMARK_FILE as yyyy-MM-dd
#########################################################################
# TODO

#########################################################################
# EXERCISE 65 (Hard)
#------------------------------------------------------------------------
# Goal: Build daily KPIs from ONLY the newly processed good rows (txns_good_fk):
# - daily_txn_count
# - daily_total_spend (rounded 2)
# Return columns: txn_date, daily_txn_count, daily_total_spend
#########################################################################
daily_kpis_increment = (
    # TODO
)
# daily_kpis_increment.orderBy("txn_date").show(truncate=False)

#########################################################################
# EXERCISE 66 (Very Hard)
#------------------------------------------------------------------------
# Goal: Upsert daily KPIs into Gold Delta table:
# - Path: data/out/delta/daily_kpis_gold
# - Key: txn_date
# - If matched: add increment totals into existing totals
# - If not matched: insert new row
#
# Return: gold_daily_kpis_df (read back)
#########################################################################
gold_daily_kpis_df = (
    # TODO
)
# gold_daily_kpis_df.orderBy("txn_date").show(truncate=False)

#########################################################################
# EXERCISE 67 (Hard)
#------------------------------------------------------------------------
# Goal: Run log record as a small DataFrame with:
# - run_id, run_ts
# - watermark_before, watermark_after
# - rows_in_batch, rows_candidates, rows_good_fk
# - rows_quarantine, rows_bad_fk
# Return: run_log_df (1 row)
#########################################################################
run_log_df = (
    # TODO
)
# run_log_df.show(truncate=False)

#########################################################################
# EXERCISE 68 (Hard)
#------------------------------------------------------------------------
# Goal: Append run_log_df to: data/out/run_logs (parquet)
# Then read it back and show last 20 (order by run_ts desc)
#########################################################################
# TODO

#########################################################################
# EXERCISE 69 (Very Hard -> Brutal)
#------------------------------------------------------------------------
# Goal: Idempotency check:
# Simulate re-running the SAME batch and ensure Silver does NOT duplicate rows.
# - Re-run the upsert logic using the same txns_good_fk again
# - Validate counts are stable (no extra txn_id rows)
# Return: silver_count_after_rerun
#########################################################################
silver_count_after_rerun = (
    # TODO
)
# print("silver_count_after_rerun:", silver_count_after_rerun)

#########################################################################
# EXERCISE 70 (Brutal)
#------------------------------------------------------------------------
# Goal: Build a "data quality report" DataFrame for this run:
# - metric_name, metric_value (string or numeric)
# Include at least:
# - null_txn_id_count
# - bad_amount_count
# - bad_timestamp_count
# - bad_fk_count
# - quarantine_total
# - good_total
# Write it to: data/out/run_logs/dq_reports (parquet) partitioned by run_id
#########################################################################
dq_report_df = (
    # TODO
)
# dq_report_df.show(truncate=False)

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
