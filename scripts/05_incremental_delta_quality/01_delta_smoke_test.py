import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/09_incremental_delta_quality/09_01_delta_smoke_test.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.09_incremental_delta_quality.09_01_delta_smoke_test
# -----------------------------------------------------------------------
# Description:
#   Delta Lake smoke test: write/read a Delta table locally (graceful if Delta isn't installed).
#########################################################################

from pyspark.sql import functions as F
from src.spark_utils import get_spark

spark = get_spark("09_01_delta_smoke_test")
spark.sparkContext.setLogLevel("ERROR")

try:
    # delta-spark provides the datasource for format("delta")
    import delta  # noqa: F401
except Exception as e:
    print("\nDelta not available in this environment.")
    print("Install it (example): pip install delta-spark")
    print("Then ensure your SparkSession is configured for Delta in src/spark_utils.py.")
    spark.stop()
    raise SystemExit(0)

df = spark.createDataFrame([(1, "NY"), (2, "CA"), (3, "TX")], ["customer_id", "state"])
out = "data/out/09_delta_smoke"

(
    df.write.mode("overwrite")
    .format("delta")
    .save(out)
)

read_back = spark.read.format("delta").load(out)
print("\n--- delta read back ---")
read_back.show(truncate=False)

spark.stop()
