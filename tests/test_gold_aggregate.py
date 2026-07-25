from src.gold.aggregate import build_daily_sales


def test_build_daily_sales_aggregates_correctly(spark):
    df = spark.createDataFrame(
        [
            ("2026-07-24", "s1", "p1", 2.0, 20.0, "o1"),
            ("2026-07-24", "s1", "p1", 3.0, 30.0, "o2"),
            ("2026-07-24", "s1", "p2", 1.0, 15.0, "o3"),
        ],
        ["_run_date", "store_id", "product_id", "quantity", "line_total", "order_id"],
    )

    result = (
        build_daily_sales(df)
        .filter("product_id = 'p1'")
        .collect()[0]
    )

    assert result["total_quantity"] == 5.0
    assert result["total_sales"] == 50.0
    assert result["order_count"] == 2
    assert result["avg_order_value"] == 25.0
