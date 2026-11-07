from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_SOURCE = "customer_dev.dev_customer_schema.bronz_customer_360"


# Read your existing Bronze Delta table as the pipeline input.
@dp.materialized_view(name="bronze_customer_changes")
def bronze_customer_changes():
    return spark.read.table(BRONZE_SOURCE)


# Normalize the CDC fields and assign a reason to invalid records.
@dp.temporary_view(name="normalized_customer_changes")
def normalized_customer_changes():
    source = spark.read.table("bronze_customer_changes")

    normalized = (
        source
        .withColumnRenamed("Op", "operation")
        .withColumn("operation", F.upper(F.trim(F.col("operation"))))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        # Keep this as a string; it is the CDC event ordering field.
        .withColumn("dms_change_seq", F.trim(F.col("dms_change_seq")))
        .withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp(F.col("last_modified_ts")),
                F.col("updated_at")
            )
        )
    )

    valid_email = F.col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    return normalized.withColumn(
        "quality_error",
        F.when(F.col("customer_id").isNull(), "missing_customer_id")
         .when(
             F.col("operation").isNull()
             | ~F.col("operation").isin("I", "U", "D"),
             "invalid_operation"
         )
         .when(
             F.col("dms_change_seq").isNull()
             | (F.col("dms_change_seq") == ""),
             "missing_dms_change_seq"
         )
         .when(F.col("event_ts").isNull(), "missing_event_timestamp")
         .when(
             (F.col("operation") != "D")
             & (F.col("email").isNull() | ~valid_email),
             "invalid_email"
         )
    )


# Silver: one valid row for each accepted DMS event, including deletes.
@dp.materialized_view(name="silver_customer_events")
@dp.expect_all_or_fail({
    "customer_id_present": "customer_id IS NOT NULL",
    "supported_operation": "operation IN ('I', 'U', 'D')",
    "change_sequence_present": (
        "dms_change_seq IS NOT NULL AND dms_change_seq != ''"
    ),
    "event_timestamp_present": "event_ts IS NOT NULL",
    "non_delete_email_valid": (
        "operation = 'D' OR "
        "(email IS NOT NULL AND email RLIKE "
        "'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$')"
    )
})
def silver_customer_events():
    return (
        spark.read.table("normalized_customer_changes")
        .filter(F.col("quality_error").isNull())
        .drop("quality_error")
    )


# Quarantine: invalid events and the reason each event failed validation.
@dp.materialized_view(name="silver_customer_quarantine")
def silver_customer_quarantine():
    return (
        spark.read.table("normalized_customer_changes")
        .filter(F.col("quality_error").isNotNull())
    )


# Gold SCD1: latest state per customer. Latest deletes are omitted.
@dp.materialized_view(name="gold_customer_scd1")
def gold_customer_scd1():
    events = spark.read.table("silver_customer_events")

    latest_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("dms_change_seq").desc(),
            F.col("event_ts").desc()
        )
    )

    return (
        events
        .withColumn("_row_num", F.row_number().over(latest_window))
        .filter(
            (F.col("_row_num") == 1)
            & (F.col("operation") != "D")
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "state",
            "loyalty_tier",
            "updated_at"
        )
    )


# Gold SCD2: a new version for changes to the tracked columns.
# Phone is intentionally not tracked; phone-only changes update that
# version's phone value without creating another version.
@dp.materialized_view(name="gold_customer_scd2")
def gold_customer_scd2():
    events = spark.read.table("silver_customer_events")

    tracked_columns = ["email", "city", "state", "loyalty_tier"]

    order_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("dms_change_seq").asc(),
            F.col("event_ts").asc()
        )
    )

    compared = events.withColumn(
        "_previous_sequence",
        F.lag("dms_change_seq").over(order_window)
    )

    for column_name in tracked_columns:
        compared = compared.withColumn(
            f"_previous_{column_name}",
            F.lag(F.col(column_name)).over(order_window)
        )

    tracked_change = F.lit(False)

    for column_name in tracked_columns:
        tracked_change = tracked_change | ~(
            F.col(column_name).eqNullSafe(
                F.col(f"_previous_{column_name}")
            )
        )

    # An insert starts a version. A delete closes the current version.
    # A first-seen update also starts a version, in case the initial
    # insert event was not included in the input.
    with_boundaries = (
        compared
        .withColumn(
            "_starts_boundary",
            F.when(
                (F.col("operation") == "I")
                | (F.col("operation") == "D")
                | (
                    (F.col("operation") == "U")
                    & (
                        F.col("_previous_sequence").isNull()
                        | tracked_change
                    )
                ),
                1
            ).otherwise(0)
        )
        .withColumn(
            "_version_group",
            F.sum("_starts_boundary").over(
                order_window.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow
                )
            )
        )
    )

    # Each insert, tracked update, or delete is a boundary.
    boundary_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("dms_change_seq").asc(),
            F.col("event_ts").asc()
        )
    )

    boundaries = (
        with_boundaries
        .filter(F.col("_starts_boundary") == 1)
        .select(
            "customer_id",
            "_version_group",
            "operation",
            "event_ts",
            "dms_change_seq"
        )
        .withColumn(
            "valid_to",
            F.lead("event_ts").over(boundary_window)
        )
    )

    # Keep the latest event inside each version so that non-tracked
    # attributes, such as phone, carry their latest value.
    latest_in_version_window = (
        Window
        .partitionBy("customer_id", "_version_group")
        .orderBy(
            F.col("dms_change_seq").desc(),
            F.col("event_ts").desc()
        )
    )

    latest_in_version = (
        with_boundaries
        .withColumn(
            "_version_row_num",
            F.row_number().over(latest_in_version_window)
        )
        .filter(F.col("_version_row_num") == 1)
    )

    return (
        latest_in_version.alias("e")
        .join(
            boundaries.alias("b"),
            on=["customer_id", "_version_group"],
            how="inner"
        )
        # A delete boundary closes the prior version; it is not a
        # customer attribute version itself.
        .filter(F.col("b.operation") != "D")
        .select(
            F.col("e.customer_id").alias("customer_id"),
            F.col("e.first_name").alias("first_name"),
            F.col("e.last_name").alias("last_name"),
            F.col("e.email").alias("email"),
            F.col("e.phone").alias("phone"),
            F.col("e.city").alias("city"),
            F.col("e.state").alias("state"),
            F.col("e.loyalty_tier").alias("loyalty_tier"),
            F.col("b.event_ts").alias("valid_from"),
            F.col("b.valid_to").alias("valid_to"),
            F.col("b.valid_to").isNull().alias("is_current"),
            F.col("b.dms_change_seq").alias(
                "version_start_dms_change_seq"
            )
        )
    )