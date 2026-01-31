import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

#########################################################################
# File   : scripts/07_engineering_patterns/07_01_schema_enforcement.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.07_engineering_patterns.07_01_schema_enforcement
# -----------------------------------------------------------------------
# Description:
#   Schema enforcement basics: explicit schema, safe casts, and catching bad rows early.
#########################################################################

from pyspark.sql import functions as F
from pyspark.sql import types as T
from src.spark_utils import get_spark

spark = get_spark("07_01_schema_enforcement")
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------
# 1) Read with inferSchema (demo-only)
# ----------------------------------------
customers_infer = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)
print("\n--- inferSchema customers ---")
customers_infer.printSchema()
customers_infer.show(5, truncate=False)

# ----------------------------------------
# 2) Read with an explicit schema (preferred)
#    This prevents Spark from guessing types.
# ----------------------------------------
customer_schema = T.StructType([
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("signup_date", T.DateType(), True),
])

customers = (
    spark.read.option("header", True)
    .schema(customer_schema)  # enforce types
    .csv("data/raw/customers.csv")
)

print("\n--- explicit schema customers ---")
customers.printSchema()
customers.show(5, truncate=False)

# ----------------------------------------
# 3) Safe-cast pattern + bad-row capture
#    (simulate a 'dirty' numeric column)
# ----------------------------------------
dirty = customers.select(
    F.col("customer_id").cast("string").alias("customer_id_str"),
    "first_name",
    "last_name",
    "state"
).withColumn(
    "customer_id_int",
    F.col("customer_id_str").cast("int")
)

bad = dirty.filter(F.col("customer_id_int").isNull())
good = dirty.filter(F.col("customer_id_int").isNotNull())

print("\n--- safe cast results ---")
print(f"good rows: {good.count()} | bad rows (cast failed): {bad.count()}")
bad.show(5, truncate=False)

# ----------------------------------------
# 4) Minimal 'quality gate' example:
#    fail if any bad rows exist
# ----------------------------------------
if bad.count() > 0:
    print("\nQUALITY GATE FAILED: customer_id had non-numeric values.")
else:
    print("\nQUALITY GATE PASSED: customer_id cast cleanly.")

spark.stop()
