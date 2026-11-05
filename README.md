# Customer CDC Lakehouse

A learning and interview project that processes customer change data from
PostgreSQL through AWS DMS, Amazon S3, and Databricks.

## Architecture

PostgreSQL → AWS DMS → Amazon S3 (Parquet) → Databricks → Delta tables

## Databricks layers

- **Bronze:** DMS customer change events
- **Silver:** validated customer events and quarantined bad records
- **Gold SCD1:** latest customer state
- **Gold SCD2:** customer history with validity dates

## Tools

AWS RDS PostgreSQL, AWS DMS, Amazon S3, Databricks Lakeflow Spark Declarative
Pipelines, PySpark, SQL, GitHub, and Databricks Asset Bundles.

## Validation

A validation notebook checks Silver operation counts, quarantined rows,
Gold SCD1 current state, and Gold SCD2 version history.
