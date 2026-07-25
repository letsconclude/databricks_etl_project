"""
Silver layer: clean, deduplicate, validate Bronze data.

Responsibilities:
  - Deduplicate on business key (order_id), keeping the latest ingested record.
  - Compute derived columns (line_total) used downstream.
  - Run declarative data quality checks; route failing rows to a quarantine
    Delta table instead of dropping them or crashing the job.
  - Idempotent MERGE write, partitioned by run date.
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import PipelineConfig, load_config
from src.utils.data_quality import DQCheck, apply_checks, dq_pass_rate, split_pass_fail
from src.utils.logger import StageTimer, get_logger
from src.utils.spark_utils import get_spark, merge_upsert

logger = get_logger(__name__)

DQ_CHECKS = [
    DQCheck("order_id_not_null", "order_id IS NOT NULL"),
    DQCheck("store_id_not_null", "store_id IS NOT NULL"),
    DQCheck("product_id_not_null", "product_id IS NOT NULL"),
    DQCheck("quantity_positive", "quantity IS NOT NULL AND quantity > 0"),
    DQCheck("unit_price_non_negative", "unit_price IS NOT NULL AND unit_price >= 0"),
]


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested record per order_id."""
    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def enrich(df: DataFrame) -> DataFrame:
    return df.withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))


def run(env: str, run_date: str) -> None:
    cfg = load_config(env)
    spark = get_spark()
    spark.conf.set("spark.sql.shuffle.partitions", cfg.processing.shuffle_partitions)

    with StageTimer(logger, "silver_transform"):
        bronze_table = cfg.full_table_name(cfg.bronze.table)
        bronze_df = spark.read.table(bronze_table).filter(F.col("_run_date") == run_date)

        deduped_df = deduplicate(bronze_df)
        enriched_df = enrich(deduped_df)
        checked_df = apply_checks(enriched_df, DQ_CHECKS)

        pass_rate = dq_pass_rate(checked_df)
        logger.info(f"data quality pass rate: {pass_rate:.2%}", extra={"extra_fields": {"dq_pass_rate": pass_rate}})

        passing_df, failing_df = split_pass_fail(checked_df)

        silver_table = cfg.full_table_name(cfg.silver.table)
        merge_upsert(
            spark,
            updates_df=passing_df,
            target_table=silver_table,
            merge_keys=["order_id"],
            partition_cols=["_run_date"],
        )
        logger.info(f"silver write complete: {silver_table}, rows={passing_df.count()}")

        if failing_df.take(1):
            quarantine_table = cfg.full_table_name(cfg.silver.quarantine_table)
            merge_upsert(
                spark,
                updates_df=failing_df,
                target_table=quarantine_table,
                merge_keys=["order_id"],
                partition_cols=["_run_date"],
            )
            logger.warning(f"quarantined rows written: {quarantine_table}, rows={failing_df.count()}")

        # Fail the job loudly if quality drops below an acceptable threshold,
        # rather than silently shipping bad data downstream to Gold/BI.
        if pass_rate < 0.90:
            raise RuntimeError(
                f"Data quality pass rate {pass_rate:.2%} is below the 90% threshold for run_date={run_date}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver layer transform")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    try:
        run(env=args.env, run_date=args.run_date)
    except Exception:
        logger.error("silver_transform failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
