import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat_ws
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ==========================================
# PYSPARK TEST FIXTURE
# ==========================================
@pytest.fixture(scope="session")
def spark():
    """Initializes a local SparkSession for lightweight unit testing."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("MavenMarketUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


# ==========================================
# 1. CORE BUSINESS CALCULATIONS
# ==========================================
def test_fact_sales_revenue_and_profit_calculation(spark):
    """Validates total_revenue and total_profit calculations in fact_sales."""
    sample_data = [
        # (quantity, product_retail_price, product_cost)
        (10, 25.0, 15.0),
        (2, 100.0, 60.0),
    ]
    columns = ["quantity", "product_retail_price", "product_cost"]
    df = spark.createDataFrame(sample_data, columns)

    result_df = df.select(
        (col("quantity") * col("product_retail_price")).alias("total_revenue"),
        (
            (col("quantity") * col("product_retail_price"))
            - (col("quantity") * col("product_cost"))
        ).alias("total_profit"),
    )

    rows = result_df.collect()
    # Record 1: 10 * 25.0 = 250.0 revenue | 250.0 - 150.0 = 100.0 profit
    assert rows[0]["total_revenue"] == 250.0
    assert rows[0]["total_profit"] == 100.0

    # Record 2: 2 * 100.0 = 200.0 revenue | 200.0 - 120.0 = 80.0 profit
    assert rows[1]["total_revenue"] == 200.0
    assert rows[1]["total_profit"] == 80.0


def test_fact_returns_cost_calculation(spark):
    """Validates total_return_cost calculation in fact_returns."""
    sample_data = [
        # (quantity, product_cost)
        (5, 12.50),
        (1, 40.00),
    ]
    columns = ["quantity", "product_cost"]
    df = spark.createDataFrame(sample_data, columns)

    result_df = df.select(
        (col("quantity") * col("product_cost")).alias("total_return_cost")
    )
    rows = result_df.collect()

    assert rows[0]["total_return_cost"] == 62.50
    assert rows[1]["total_return_cost"] == 40.00


# ==========================================
# 2. DLT EXPECTATIONS & DATA CLEANSING
# ==========================================
def test_dlt_expectation_valid_quantity(spark):
    """Validates the DLT expectation @dlt.expect_or_drop('valid_quantity', 'quantity > 0')."""
    sample_data = [
        ("T1", 5),
        ("T2", 0),  # invalid boundary
        ("T3", -3),  # invalid negative
        ("T4", 12),
    ]
    columns = ["transaction_id", "quantity"]
    df = spark.createDataFrame(sample_data, columns)

    # Simulates expect_or_drop filter rule
    cleansed_df = df.filter(col("quantity") > 0)
    surviving_ids = [row["transaction_id"] for row in cleansed_df.collect()]

    assert cleansed_df.count() == 2
    assert surviving_ids == ["T1", "T4"]


def test_silver_transactions_deduplication(spark):
    """Validates deduplication across unioned batch CSV and Kafka stream records."""
    batch_records = [
        ("batch-1-10-101", "10", "101", "1", 5, "BATCH_CSV"),
        ("batch-1-11-102", "11", "102", "1", 2, "BATCH_CSV"),
    ]
    stream_records = [
        ("order-uuid-999", "12", "103", "2", 1, "KAFKA_STREAM"),
        (
            "order-uuid-999",
            "12",
            "103",
            "2",
            1,
            "KAFKA_STREAM",
        ),  # duplicate Kafka event
    ]
    schema = [
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "source_type",
    ]

    batch_df = spark.createDataFrame(batch_records, schema)
    stream_df = spark.createDataFrame(stream_records, schema)

    unified_deduped_df = batch_df.unionByName(stream_df).dropDuplicates(
        ["transaction_id"]
    )

    assert unified_deduped_df.count() == 3
    assert (
        unified_deduped_df.filter(
            col("transaction_id") == "order-uuid-999"
        ).count()
        == 1
    )


def test_silver_stores_regions_denormalization(spark):
    """Validates store-to-region left join and ensures duplicate source region keys do not multiply store rows."""
    stores_data = [
        ("1", "R1", "Supermarket", "Seattle"),
        ("2", "R2", "Deluxe", "Portland"),
    ]
    stores_schema = ["store_id", "region_id", "store_type", "store_city"]

    # Simulating dirty Bronze regions table containing duplicate region_id rows
    regions_data = [
        ("R1", "North America", "West District"),
        ("R1", "North America", "West District"),  # duplicate
        ("R2", "North America", "Southwest District"),
    ]
    regions_schema = ["region_id", "sales_region", "sales_district"]

    stores_df = spark.createDataFrame(
        stores_data, stores_schema
    ).dropDuplicates(["store_id"])
    regions_df = spark.createDataFrame(
        regions_data, regions_schema
    ).dropDuplicates(["region_id"])

    denormalized_df = stores_df.join(regions_df, "region_id", "left")

    assert denormalized_df.count() == 2
    row_store_1 = (
        denormalized_df.filter(col("store_id") == "1").first().asDict()
    )
    assert row_store_1["sales_region"] == "North America"
    assert row_store_1["sales_district"] == "West District"


# ==========================================
# 3. DIMENSIONAL MODELING & SCD TYPE 2
# ==========================================
def test_scd_type2_active_record_filter(spark):
    """Validates filtering of active SCD Type 2 records where __END_AT is NULL."""
    sample_data = [
        ("C1001", "Active User", None),
        ("C1001", "Historical User", "2026-01-01 00:00:00"),
        ("C1002", "Current Customer", None),
    ]
    columns = ["customer_id", "customer_name", "__END_AT"]
    df = spark.createDataFrame(sample_data, columns)

    active_df = df.filter(col("__END_AT").isNull())

    assert active_df.count() == 2
    active_ids = [row["customer_id"] for row in active_df.collect()]
    assert "C1001" in active_ids
    assert "C1002" in active_ids


# ==========================================
# 4. SECURITY & ABAC POLICIES (CLS / RLS)
# ==========================================
def test_abac_dynamic_column_masking_pii(spark):
    """Validates Tag-Based Dynamic Column-Level Masking for users not in Admin or Data_Compliance."""
    sample_data = [
        ("user_admin@maven.com", "admin_card_1234", "Admin"),
        ("analyst_bob@maven.com", "analyst_card_5678", "Analyst"),
    ]
    columns = ["user_email", "member_card", "active_group"]
    df = spark.createDataFrame(sample_data, columns)

    # Dynamic policy expression mimicking the Unity Catalog tag masking function
    masked_df = df.withColumn(
        "masked_member_card",
        when(
            col("active_group").isin("Admin", "Data_Compliance"),
            col("member_card"),
        ).otherwise(lit("****-****-****-MASKED")),
    )

    results = {
        row["user_email"]: row["masked_member_card"]
        for row in masked_df.collect()
    }
    assert results["user_admin@maven.com"] == "admin_card_1234"
    assert results["analyst_bob@maven.com"] == "****-****-****-MASKED"


def test_abac_attribute_driven_row_filter(spark):
    """Validates Row-Level Filtering isolating Regional Managers to their mapped territory."""
    sales_data = [
        ("T100", "Store_EU", "EU", 500.0),
        ("T101", "Store_NA", "NA", 300.0),
        ("T102", "Store_APAC", "APAC", 450.0),
    ]
    sales_schema = [
        "transaction_id",
        "store_name",
        "data_region",
        "total_revenue",
    ]
    sales_df = spark.createDataFrame(sales_data, sales_schema)

    # Simulated audit.user_region_mapping row for an NA Regional Manager
    current_user_mapped_region = "NA"

    # Row-filter policy condition: data_region == user_mapped_region
    filtered_df = sales_df.filter(col("data_region") == current_user_mapped_region)

    rows = filtered_df.collect()
    assert filtered_df.count() == 1
    assert rows[0]["transaction_id"] == "T101"
    assert rows[0]["data_region"] == "NA"


# ==========================================
# 5. UNITY CATALOG STRUCTURAL GOVERNANCE
# ==========================================
def test_gold_table_structural_governance_tags():
    """Validates structural rule requiring valid owner_team and data_sensitivity tags on Gold tables."""
    gold_table_metadata = {
        "delta.enableChangeDataFeed": "true",
        "owner_team": "analytics_engineering",
        "data_sensitivity": "internal",
    }

    # Mandatory governance tag checklist defined in project specifications
    required_tags = ["owner_team", "data_sensitivity"]
    allowed_sensitivities = ["public", "internal", "confidential", "restricted"]

    for tag in required_tags:
        assert tag in gold_table_metadata, f"Missing required tag: {tag}"
        assert (
            gold_table_metadata[tag] is not None
        ), f"Tag {tag} cannot be null"

    assert (
        gold_table_metadata["data_sensitivity"].lower()
        in allowed_sensitivities
    )