#########################################################################
# File   : scripts/02_exercises/01_select_filter_excercises.py
# Author : Frank Runfola
# Date   : 2/3/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.02_exercises.01_select_filter_excercises
# -----------------------------------------------------------------------
# Description:
#   Select + Filter exercises (from the original combined file).
#   Exercises 01-10.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("01_select_filter_excercises")

customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

#########################################################################
# EXERCISE 01 (Easy)
#------------------------------------------------------------------------
# Goal: Select specific columns from customers, transaction
#########################################################################
# TODO
ex01_df = (customers.select("customer_id", "first_name","last_name", "state","signup_date"))
ex01_df.show(n=3,truncate=False)

ex01_df = (txns.select("txn_id", "customer_id","txn_ts", "amount","merchant"))
ex01_df.show(n=3,truncate=False)
#########################################################################
# EXERCISE 02 (Easy)
#------------------------------------------------------------------------
# Goal: Filter customers where state == "CA"
# Return columns: customer_id, first_name, state
#########################################################################
# TODO
'''
ex02_df = (customers.filter(F.col("state")=="CA"))
ex02_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 03 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where amount > 50
# Return columns: txn_id, customer_id, amount
#########################################################################
# TODO
'''
ex03_df = (
    txns
    .filter(F.col("amount")>50)
    .select("txn_id","customer_id","amount")
)
ex03_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 04 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant == "ElectroMart"
# Return columns: txn_id, customer_id, merchant, amount
#########################################################################
# TODO
'''
ex04_df = (
    txns
    .filter(F.col("merchant")=="ElectroMart")
    .select("txn_id","customer_id","merchant","amount")
)
ex04_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 05 (Easy)
#------------------------------------------------------------------------
# Goal: Filter txns where customer_id is in {1,2,3}
# Return columns: txn_id, customer_id, txn_ts, amount
#########################################################################
# TODO
'''
ex05_df = (
    txns
    .filter(F.col("customer_id").isin(1,2,3))
    .select("txn_id","customer_id","txn_ts","amount")
)
ex05_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 06 (Easy)
# Goal: Filter customers where first_name is missing OR blank
# Return columns: customer_id, first_name, last_name
#########################################################################
# TODO
'''
ex06_df = (
    customers
    .filter((F.col("first_name").isNull()) | (F.trim(F.col("first_name"))==""))
    .select("customer_id","first_name","last_name")
)
ex06_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 07 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where amount is between 10 and 100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
# TODO
'''
ex07_df = (
    txns
    .filter(F.col("amount").between(10,100))
    .select("txn_id","customer_id","amount","merchant")
)
ex07_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 08 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Filter txns where merchant != "GroceryTown" AND amount 10->100 inclusive
# Return columns: txn_id, customer_id, amount, merchant
#########################################################################
# TODO
'''
ex08_df = (
    txns
    .filter((F.col("merchant")!="GroceryTown") & (F.col("amount").between(10,100)))
    .select("txn_id","customer_id","amount","merchant")
)
ex08_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 09 (Medium)
#------------------------------------------------------------------------
# Goal: Customers who signed up on or after 2025-03-01
# Return columns: customer_id, signup_date
#########################################################################
# TODO
'''
ex09_df = (
    customers
    .withColumn("signup_date", F.to_date("signup_date")) # Convert signup_date to a proper DATE type
    .filter(F.col("signup_date") >= F.lit("2025-03-01").cast("date"))
    .select("customer_id", "signup_date")
)
ex09_df.show(truncate=False)
'''

#########################################################################
# EXERCISE 10 (Medium)
#------------------------------------------------------------------------
# Goal: Create a new column on txns: amount_bucket with values:
# - "small" if amount < 20
# - "medium" if amount < 100
# - "large" otherwise
# Return columns: txn_id, amount, amount_bucket
#########################################################################
# TODO
'''
ex10_df = (
    txns
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 20,  F.lit("small"))
         .when(F.col("amount") < 100, F.lit("medium"))
         .otherwise(F.lit("large"))
    )
)
ex10_df.show(truncate=False)
'''
