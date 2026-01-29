import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pyspark-local-intro-tests")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_type_parsing_date_and_amount(spark):
    rows = [
        ("1001", 1, "2025-03-01 10:12:00", "25.10", "CoffeeCo"),
        ("1002", 1, "bad-ts", "120.00", "ElectroMart"),
        ("1003", 2, "2025-03-05 12:30:00", "xx", "GroceryTown"),
    ]
    df = spark.createDataFrame(rows, ["txn_id", "customer_id", "txn_ts", "amount", "merchant"])

    typed = (
        df
        .withColumn("txn_ts_ts", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("txn_date", F.to_date("txn_ts_ts"))
        .withColumn("amount_num", F.col("amount").cast("double"))
    )

    out = typed.select("txn_id", "txn_ts_ts", "txn_date", "amount_num").orderBy("txn_id").collect()

    assert out[0]["txn_date"] is not None
    assert out[0]["amount_num"] == pytest.approx(25.10)

    # bad timestamp -> null
    assert out[1]["txn_ts_ts"] is None
    assert out[1]["txn_date"] is None

    # bad amount -> null
    assert out[2]["amount_num"] is None


def test_quarantine_reason_priority(spark):
    rows = [
        (None, 1, "2025-03-01 10:12:00", 10.0, "CoffeeCo"),     # missing txn_id
        ("1002", None, "2025-03-01 10:12:00", 10.0, "CoffeeCo"), # missing customer_id
        ("1003", 1, None, 10.0, "CoffeeCo"),                    # bad timestamp
        ("1004", 1, "2025-03-01 10:12:00", -1.0, "CoffeeCo"),   # bad amount
        ("1005", 1, "2025-03-01 10:12:00", 10.0, ""),           # missing merchant
        ("1006", 1, "2025-03-01 10:12:00", 10.0, "CoffeeCo"),   # valid
    ]
    df = spark.createDataFrame(rows, ["txn_id", "customer_id", "txn_ts", "amount", "merchant"])

    # Type it similarly to your pipeline style
    staged = (
        df
        .withColumn("txn_id", F.col("txn_id"))
        .withColumn("txn_ts_ts", F.to_timestamp("txn_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("txn_date", F.to_date("txn_ts_ts"))
        .withColumn("merchant", F.col("merchant"))
        .withColumn("amount", F.col("amount").cast("double"))
    )

    # Priority rule: missing_txn_id -> missing_customer_id -> bad_timestamp -> bad_amount -> missing_merchant
    quarantined = (
        staged
        .withColumn(
            "quarantine_reason",
            F.when(F.col("txn_id").isNull() | (F.trim(F.col("txn_id")) == ""), F.lit("missing_txn_id"))
             .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
             .when(F.col("txn_ts_ts").isNull(), F.lit("bad_timestamp"))
             .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("bad_amount"))
             .when(F.col("merchant").isNull() | (F.trim(F.col("merchant")) == ""), F.lit("missing_merchant"))
             .otherwise(F.lit(None))
        )
    )

    q = quarantined.filter(F.col("quarantine_reason").isNotNull()).select("txn_id", "quarantine_reason").collect()
    q_map = {(r["txn_id"] if r["txn_id"] is not None else "NULL"): r["quarantine_reason"] for r in q}

    assert q_map["NULL"] == "missing_txn_id"
    assert q_map["1002"] == "missing_customer_id"
    assert q_map["1003"] == "bad_timestamp"
    assert q_map["1004"] == "bad_amount"
    assert q_map["1005"] == "missing_merchant"

    valid_count = quarantined.filter(F.col("quarantine_reason").isNull()).count()
    assert valid_count == 1


def test_fk_anti_join_detects_bad_customers(spark):
    customers = spark.createDataFrame([(1,), (2,), (3,)], ["customer_id"])
    txns = spark.createDataFrame([(1, "t1"), (99, "t2"), (2, "t3")], ["customer_id", "txn_id"])

    bad_fk = txns.join(customers, on="customer_id", how="left_anti")
    good_fk = txns.join(customers, on="customer_id", how="inner")

    assert bad_fk.count() == 1
    assert good_fk.count() == 2


def test_kpi_aggregation(spark):
    rows = [
        (1, 10.0),
        (1, 20.25),
        (2, 5.0),
    ]
    df = spark.createDataFrame(rows, ["customer_id", "amount"])

    kpis = (
        df.groupBy("customer_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.round(F.avg("amount"), 2).alias("avg_spend"),
        )
        .orderBy("customer_id")
    )

    out = kpis.collect()
    assert out[0]["customer_id"] == 1
    assert out[0]["txn_count"] == 2
    assert float(out[0]["total_spend"]) == pytest.approx(30.25)
    assert float(out[0]["avg_spend"]) == pytest.approx(15.12)

    assert out[1]["customer_id"] == 2
    assert out[1]["txn_count"] == 1
    assert float(out[1]["total_spend"]) == pytest.approx(5.00)
