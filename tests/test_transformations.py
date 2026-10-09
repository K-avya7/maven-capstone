import pytest
from pyspark.sql.functions import col

def test_fact_sales_revenue_and_profit_calculation(spark):
    """Validates total_revenue and total_profit calculations."""
    sample_data = [
        # (quantity, product_retail_price, product_cost)
        (10, 25.0, 15.0),
        (2, 100.0, 60.0)
    ]
    columns = ["quantity", "product_retail_price", "product_cost"]
    df = spark.createDataFrame(sample_data, columns)

    result_df = df.select(
        (col("quantity") * col("product_retail_price")).alias("total_revenue"),
        ((col("quantity") * col("product_retail_price")) - (col("quantity") * col("product_cost"))).alias("total_profit")
    )

    rows = result_df.collect()
    # Record 1: 10 * 25.0 = 250.0 revenue | 250.0 - 150.0 = 100.0 profit
    assert rows[0]["total_revenue"] == 250.0
    assert rows[0]["total_profit"] == 100.0

    # Record 2: 2 * 100.0 = 200.0 revenue | 200.0 - 120.0 = 80.0 profit
    assert rows[1]["total_revenue"] == 200.0
    assert rows[1]["total_profit"] == 80.0


def test_scd_type2_active_record_filter(spark):
    """Validates filtering of active SCD Type 2 records where __END_AT is NULL."""
    sample_data = [
        ("C1001", "Active User", None),
        ("C1001", "Historical User", "2023-01-01 00:00:00")
    ]
    columns = ["customer_id", "customer_name", "__END_AT"]
    df = spark.createDataFrame(sample_data, columns)

    active_df = df.filter(col("__END_AT").isNull())

    assert active_df.count() == 1
    assert active_df.first()["customer_name"] == "Active User"