"""
SparkSession construction and reusable Delta Lake helpers.

Kept separate from business logic so unit tests can build a local SparkSession
(local[*], no cluster) while production jobs run on a Databricks-managed
SparkSession without any code changes.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str = "retail-sales-etl") -> SparkSession:
    """
    Return the active SparkSession.

    On Databricks, SparkSession.builder.getOrCreate() returns the cluster's
    pre-configured session. Locally (e.g. in CI), it builds a local[*] session
    with Delta Lake support so the same code path is exercised in tests.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()


def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.read.table(full_table_name)
        return True
    except Exception:
        return False


def merge_upsert(
    spark: SparkSession,
    updates_df: DataFrame,
    target_table: str,
    merge_keys: list[str],
    partition_cols: list[str] | None = None,
) -> None:
    """
    Idempotent upsert into a Delta table: insert new rows, update existing
    ones matched on merge_keys. Creates the table on first run.
    """
    merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

    if not table_exists(spark, target_table):
        writer = updates_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("target")
        .merge(updates_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def optimize_and_vacuum(spark: SparkSession, full_table_name: str, retain_hours: int = 168) -> None:
    """Compact small files and clean up old versions. Call periodically, not on every run."""
    spark.sql(f"OPTIMIZE {full_table_name}")
    spark.sql(f"VACUUM {full_table_name} RETAIN {retain_hours} HOURS")
