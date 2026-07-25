from datetime import datetime

from src.silver.transform import DQ_CHECKS, deduplicate, enrich
from src.utils.data_quality import apply_checks, split_pass_fail


def test_deduplicate_keeps_most_recently_ingested_row(spark):
    df = spark.createDataFrame(
        [
            ("o1", "s1", "p1", 2.0, 10.0, datetime(2026, 7, 24, 8, 0, 0)),
            ("o1", "s1", "p1", 5.0, 10.0, datetime(2026, 7, 24, 9, 0, 0)),  # newer, should win
            ("o2", "s1", "p2", 1.0, 20.0, datetime(2026, 7, 24, 8, 0, 0)),
        ],
        ["order_id", "store_id", "product_id", "quantity", "unit_price", "_ingested_at"],
    )

    result = deduplicate(df).orderBy("order_id").collect()

    assert len(result) == 2
    assert result[0]["order_id"] == "o1"
    assert result[0]["quantity"] == 5.0  # kept the later record, not the earlier one


def test_enrich_computes_line_total(spark):
    df = spark.createDataFrame(
        [("o1", 3.0, 9.5)],
        ["order_id", "quantity", "unit_price"],
    )

    result = enrich(df).collect()[0]

    assert result["line_total"] == 28.5


def test_dq_checks_flag_invalid_rows_without_dropping_them(spark):
    df = spark.createDataFrame(
        [
            ("o1", "s1", "p1", 2.0, 10.0),   # valid
            ("o2", "s1", "p1", -1.0, 10.0),  # invalid: negative quantity
            (None, "s1", "p1", 1.0, 10.0),   # invalid: null order_id
        ],
        ["order_id", "store_id", "product_id", "quantity", "unit_price"],
    )

    checked = apply_checks(df, DQ_CHECKS)
    passing, failing = split_pass_fail(checked)

    assert passing.count() == 1
    assert failing.count() == 2
    failed_names = failing.select("_dq_failures").collect()
    all_failures = {name for row in failed_names for name in row["_dq_failures"]}
    assert "quantity_positive" in all_failures
    assert "order_id_not_null" in all_failures


def test_dq_checks_pass_rate_is_computed_correctly(spark):
    from src.utils.data_quality import dq_pass_rate

    df = spark.createDataFrame(
        [
            ("o1", "s1", "p1", 2.0, 10.0),
            ("o2", "s1", "p1", -1.0, 10.0),
        ],
        ["order_id", "store_id", "product_id", "quantity", "unit_price"],
    )
    checked = apply_checks(df, DQ_CHECKS)

    assert dq_pass_rate(checked) == 0.5
