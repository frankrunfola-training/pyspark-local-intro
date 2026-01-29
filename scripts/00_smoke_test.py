import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # add project root to sys.path so `import src.<FOLDER>` works
sys.path.insert(0, str(PROJECT_ROOT))

###################################################################################
# File   : 00_smoke_test.py
# Author : Frank Runfola
# Date   : 1/25/2026
# -----------------------------------------------------------------------
# Run cmd:
#   cd /projects/pyspark-local-intro
#   python -m scripts.00_smoke_test
# -----------------------------------------------------------------------
# Description:
#   Smoke test:
#   - confirm Spark starts locally
#   - confirm you can create a DataFrame from in-memory Python data
#   - confirm explicit schema works (no type guessing surprises)
###################################################################################

from datetime import date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType
)
from src.spark_utils import get_spark

spark = get_spark("00_smoke_test")
spark.sparkContext.setLogLevel("ERROR")  # reduce Spark startup noise for local runs

###################################################################################
# 1) Data as a Python list (each tuple = one row)
# Why:
# - quickest way to prove Spark + PySpark are working
# - no files needed; isolates environment/setup issues
#
# Row format:
#   customer_id, name, state, amount, order_date
###################################################################################
rows = [
    (101, "Alice",  "NY", 120.50, date(2026, 1, 10)),
    (102, "Bob",    "CA",  75.00, date(2026, 1, 12)),
    (103, "Carla",  "TX", 220.10, date(2026, 1, 18)),
    (104, "Dev",    "NY",  15.99, date(2026, 1, 19)),
    (105, "Eva",    "FL",  42.25, date(2026, 1, 20)),
    (106, "Frank",  "WA", 310.00, date(2026, 1, 21)),
    (107, "Grace",  "IL",  88.40, date(2026, 1, 22)),
    (108, "Hank",   "NJ",  19.95, date(2026, 1, 23)),
    (109, "Ivy",    "PA", 150.75, date(2026, 1, 24)),
    (110, "Jack",   "OH",  63.10, date(2026, 1, 25)),
    (111, "Kara",   "GA",  29.99, date(2026, 1, 26)),
    (112, "Liam",   "NC",  99.95, date(2026, 1, 27)),
    (113, "Mia",    "MI", 130.00, date(2026, 1, 28)),
    (114, "Noah",   "AZ",  55.55, date(2026, 1, 29)),
    (115, "Olivia", "MA", 210.20, date(2026, 1, 30)),
    (116, "Paul",   "VA",  12.49, date(2026, 1, 31)),
    (117, "Quinn",  "CO",  77.77, date(2026, 2,  1)),
    (118, "Rita",   "TN",  66.60, date(2026, 2,  2)),
    (119, "Sam",    "MO", 145.90, date(2026, 2,  3)),
    (120, "Tina",   "MN",  34.10, date(2026, 2,  4)),
    (121, "Uma",    "OR", 189.99, date(2026, 2,  5)),
    (122, "Vince",  "NV",  25.00, date(2026, 2,  6)),
    (123, "Wendy",  "WI",  98.25, date(2026, 2,  7)),
    (124, "Xander", "UT", 110.00, date(2026, 2,  8)),
    (125, "Yara",   "MD",  54.30, date(2026, 2,  9)),
    (126, "Zane",   "IN",  18.80, date(2026, 2, 10)),
    (127, "Aiden",  "SC", 250.00, date(2026, 2, 11)),
    (128, "Bella",  "AL",  33.33, date(2026, 2, 12)),
    (129, "Caleb",  "KY",  79.90, date(2026, 2, 13)),
    (130, "Daisy",  "LA", 105.40, date(2026, 2, 14)),
    (131, "Ethan",  "OK",  92.10, date(2026, 2, 15)),
    (132, "Fiona",  "IA",  44.44, date(2026, 2, 16)),
    (133, "Gavin",  "CT", 160.00, date(2026, 2, 17)),
    (134, "Hazel",  "KS",  22.22, date(2026, 2, 18)),
    (135, "Ian",    "AR",  58.70, date(2026, 2, 19)),
    (136, "Jade",   "MS",  17.60, date(2026, 2, 20)),
    (137, "Kai",    "NM", 199.95, date(2026, 2, 21)),
    (138, "Luna",   "NE",  40.00, date(2026, 2, 22)),
    (139, "Mason",  "NH",  73.25, date(2026, 2, 23)),
    (140, "Nina",   "RI",  65.00, date(2026, 2, 24)),
    (141, "Omar",   "ID",  84.80, date(2026, 2, 25)),
    (142, "Pia",    "ME",  27.15, date(2026, 2, 26)),
    (143, "Rohan",  "VT", 115.00, date(2026, 2, 27)),
    (144, "Sara",   "DE",  31.00, date(2026, 2, 28)),
    (145, "Theo",   "DC", 175.75, date(2026, 3,  1)),
    (146, "Una",    "HI",  90.00, date(2026, 3,  2)),
    (147, "Victor", "AK", 205.05, date(2026, 3,  3)),
    (148, "Willow", "WY",  14.95, date(2026, 3,  4)),
    (149, "Zoe",    "NY",  60.60, date(2026, 3,  5)),
    (150, "Aria",   "CA", 135.35, date(2026, 3,  6)),
]

###################################################################################
# 2) Explicit schema (prevents Spark from guessing types wrong)
# Why:
# - schema inference can guess incorrectly (especially dates/ints/decimals)
# - explicit schema makes your pipeline stable and predictable
###################################################################################
schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name",        StringType(),  nullable=False),
    StructField("state",       StringType(),  nullable=False),
    StructField("amount",      DoubleType(),  nullable=False),
    StructField("order_date",  DateType(),    nullable=False),
])

###################################################################################
# 3) Create DataFrame
# Why:
# - proves Spark can materialize a DataFrame using your schema + data
###################################################################################
df = spark.createDataFrame(rows, schema)

print(f"\nSpark version: {spark.version}\n")

# Show schema first (types) so you can confirm the schema is applied correctly
print("--- DataFrame schema ---")
df.printSchema()

# Show a small sample of rows (n=5) for quick confirmation
print("\n--- DataFrame sample rows ---")
df.show(n=5, truncate=True)

# Quick sanity check: row count (action)
print("\n--- Row count ---")
print("rows:", df.count())

# Always stop SparkSession to release resources (important on local runs)
spark.stop()
