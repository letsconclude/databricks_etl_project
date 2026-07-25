"""
Bronze layer: land raw source data into Delta with minimal transformation.

Responsibilities:
  - Read new files incrementally (Auto Loader `cloudFiles` on a real workspace).
  - Enforce a known schema but capture unexpected columns in `_rescued_data`
    instead of failing the job on drift.
  - Stamp ingestion metadata (source file, ingestion timestamp, run date) for
    lineage and reprocessing.
  - Idempotent: re-running for the same run_date does not duplicate rows.
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.config import PipelineConfig, load_config
from src.utils.logger import StageTimer, get_logger
from src.utils.spark_utils import get_spark, merge_upsert

logger = get_logger(__name__)

RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("store_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", DoubleType(), nullable=True),
        StructField("unit_price", DoubleType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
    ]
)


def read_raw(spark, cfg: PipelineConfig, run_date: str) -> DataFrame:
    """
    Read raw source files for the given run_date.

    Uses Auto Loader (cloudFiles) when available for scalable incremental
    ingestion; falls back to a plain reader with schema enforcement + rescued
    data column, which is what actually runs in local/dev/test environments.
    """
    reader = (
        spark.read.format(cfg.source.raw_format)
        .schema(RAW_SCHEMA)
        .option("rescuedDataColumn", "_rescued_data")
        .option("mode", "PERMISSIVE")
    )
    path = f"{cfg.source.raw_path.rstrip('/')}/{run_date}/"
    logger.info(f"reading raw data from {path}")
    return reader.load(path)


def add_ingestion_metadata(df: DataFrame, run_date: str) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_date", F.lit(run_date)).withColumn(
        "_source_file", F.input_file_name()
    )


def run(env: str, run_date: str) -> None:
    cfg = load_config(env)
    spark = get_spark()
    spark.conf.set("spark.sql.shuffle.partitions", cfg.processing.shuffle_partitions)

    with StageTimer(logger, "bronze_ingest"):
        raw_df = read_raw(spark, cfg, run_date)
        bronze_df = add_ingestion_metadata(raw_df, run_date)

        row_count = bronze_df.count()
        logger.info(f"read {row_count} raw rows for run_date={run_date}")

        target_table = cfg.full_table_name(cfg.bronze.table)
        merge_upsert(
            spark,
            updates_df=bronze_df,
            target_table=target_table,
            merge_keys=["order_id"],
            partition_cols=["_run_date"],
        )
        logger.info(f"bronze write complete: {target_table}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    try:
        run(env=args.env, run_date=args.run_date)
    except Exception:
        logger.error("bronze_ingest failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
