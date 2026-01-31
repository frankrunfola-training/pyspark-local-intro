import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_02_json_parsing.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_02_json_parsing
# -----------------------------------------------------------------------
# Description:
#   JSON parsing: from_json(), schema-on-read for nested objects, and handling malformed JSON.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_utils import get_spark

spark = get_spark("07_02_json_parsing")
spark.sparkContext.setLogLevel("ERROR")

# Small, controlled JSON strings (you see this a lot in event pipelines)
rows = [
    (1, '{"device":"ios","app_version":"1.2.3","meta":{"ip":"10.1.2.3","lang":"en"}}'),
    (2, '{"device":"android","app_version":"2.0.0","meta":{"ip":"10.9.8.7","lang":"es"}}'),
    (3, '{"device":"web","app_version":null,"meta":{"ip":"10.0.0.1","lang":"en"}}'),
    (4, '{bad json here}'),  # malformed
]
df = spark.createDataFrame(rows, ["event_id", "payload_json"])

payload_schema = T.StructType([
    T.StructField("device", T.StringType(), True),
    T.StructField("app_version", T.StringType(), True),
    T.StructField("meta", T.StructType([
        T.StructField("ip", T.StringType(), True),
        T.StructField("lang", T.StringType(), True),
    ]), True),
])

parsed = df.withColumn("payload", F.from_json("payload_json", payload_schema))

print("\n--- raw ---")
df.show(truncate=False)

print("\n--- parsed (malformed -> null) ---")
parsed.select(
    "event_id",
    "payload.device",
    "payload.app_version",
    "payload.meta.ip",
    "payload.meta.lang",
    F.col("payload").isNull().alias("parse_failed")
).show(truncate=False)

# Quality: quarantine parse failures with reason
quarantine = parsed.filter(F.col("payload").isNull()).withColumn("reason", F.lit("malformed_json"))
good = parsed.filter(F.col("payload").isNotNull())

print(f"\nGood: {good.count()} | Quarantine: {quarantine.count()}")
quarantine.show(truncate=False)

spark.stop()
