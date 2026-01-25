# src/spark_utils.py
# Shared SparkSession builder so every script is consistent.

from pyspark.sql import SparkSession

def get_spark(app_name: str = "pyspark-local-intro") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)  
        .master("local[*]")                           # Local mode; use all cores available
        .config("spark.sql.shuffle.partitions", "4")  # Keep things snappy and deterministic for small local runs
        .getOrCreate()
    )