"""
Gold layer: business-level aggregates ready for BI consumption.

Computes daily sales totals by store and product from Silver, written as a
Delta table optimized for downstream dashboard queries (partitioned by date,
Z-ordered on store_id).
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import PipelineConfig, load_config
from src.utils.logger import StageTimer, get_logger
from src.utils.spark_utils import get_spark, merge_upsert

logger = get_logger(__name__)


def build_daily_sales(silver_df: DataFrame) -> DataFrame:
    return silver_df.groupBy("_run_date", "store_id", "product_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("total_sales"),
        F.count("order_id").alias("order_count"),
        F.round(F.avg("line_total"), 2).alias("avg_order_value"),
    )


def run(env: str, run_date: str) -> None:
    cfg = load_config(env)
    spark = get_spark()
    spark.conf.set("spark.sql.shuffle.partitions", cfg.processing.shuffle_partitions)

    with StageTimer(logger, "gold_aggregate"):
        silver_table = cfg.full_table_name(cfg.silver.table)
        silver_df = spark.read.table(silver_table).filter(F.col("_run_date") == run_date)

        gold_df = build_daily_sales(silver_df)

        gold_table = cfg.full_table_name(cfg.gold.daily_sales_table)
        merge_upsert(
            spark,
            updates_df=gold_df,
            target_table=gold_table,
            merge_keys=["_run_date", "store_id", "product_id"],
            partition_cols=["_run_date"],
        )
        logger.info(f"gold write complete: {gold_table}, rows={gold_df.count()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold layer aggregation")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    try:
        run(env=args.env, run_date=args.run_date)
    except Exception:
        logger.error("gold_aggregate failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()


