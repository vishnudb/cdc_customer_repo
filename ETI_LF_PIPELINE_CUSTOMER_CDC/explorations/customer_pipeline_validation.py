# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

silver = spark.table(
    "customer_dev.lakeflow_schema.silver_customer_events"
)
quarantine = spark.table(
    "customer_dev.lakeflow_schema.silver_customer_quarantine"
)
scd1 = spark.table(
    "customer_dev.lakeflow_schema.gold_customer_scd1"
)
scd2 = spark.table(
    "customer_dev.lakeflow_schema.gold_customer_scd2"
)

operation_counts = {
    row["operation"]: row["count"]
    for row in silver.groupBy("operation").count().collect()
}
assert operation_counts == {"I": 14, "U": 3, "D": 2}, operation_counts
assert quarantine.count() == 1

assert scd1.count() == 12
assert scd1.filter("customer_id IN (9104, 9105, 9115)").count() == 0

customer_9102_scd1 = (
    scd1.filter("customer_id = 9102").select("phone").first()
)
assert customer_9102_scd1 is not None
assert customer_9102_scd1["phone"] == "555-0192"

assert scd2.count() == 16
assert scd2.filter("is_current = true").count() == 12
assert scd2.filter("customer_id = 9101").count() == 3
assert scd2.filter("customer_id = 9102").count() == 1

customer_9102_scd2 = (
    scd2.filter("customer_id = 9102").select("phone").first()
)
assert customer_9102_scd2["phone"] == "555-0192"

duplicate_current = (
    scd2.filter("is_current = true")
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
)
assert duplicate_current.count() == 0

print("All customer pipeline validation checks passed.")