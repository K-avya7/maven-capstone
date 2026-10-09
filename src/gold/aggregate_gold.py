import dlt
from pyspark.sql.functions import col

# Load dynamic configurations for 100% config-driven modularity
CATALOG = spark.conf.get("pipeline.catalog", "dbx_maven_market")
SILVER_SCHEMA = spark.conf.get("pipeline.silver_schema", "silver")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema", "bronze")

# Mandatory table properties required by Unity Catalog structural governance
GOLD_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "owner_team": "analytics_engineering",
    "data_sensitivity": "internal"
}

# ==========================================
# 1. GOLD DIMENSION TABLES
# ==========================================

@dlt.table(
    name="dim_customers",
    comment="Active Gold Customer Dimension",
    table_properties=GOLD_TABLE_PROPERTIES
)
def dim_customers():
    return (
        spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_customers")
        .filter(col("__END_AT").isNull())
        .withColumn("customer_id", col("customer_id").cast("string"))
    )


@dlt.table(
    name="dim_products",
    comment="Active Gold Product Dimension",
    table_properties=GOLD_TABLE_PROPERTIES
)
def dim_products():
    return (
        spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_products")
        .filter(col("__END_AT").isNull())
        .withColumn("product_id", col("product_id").cast("string"))
    )


@dlt.table(
    name="dim_stores",
    comment="Active Gold Store Dimension",
    table_properties=GOLD_TABLE_PROPERTIES
)
def dim_stores():
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_stores")


@dlt.table(
    name="dim_calendar",
    comment="Gold Calendar Date Dimension for BI Time Series Analysis",
    table_properties=GOLD_TABLE_PROPERTIES
)
def dim_calendar():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.calendar")


# ==========================================
# 2. GOLD FACT TABLES
# ==========================================

@dlt.table(
    name="fact_sales",
    comment="Gold Fact Sales Table with Liquid Clustering",
    cluster_by=["store_id", "product_id"],
    table_properties=GOLD_TABLE_PROPERTIES
)
def fact_sales():
    tx = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions")
    prod = dlt.read("dim_products")
    stores = dlt.read("dim_stores")

    return (
        tx.join(prod, "product_id", "inner")
        .join(stores, "store_id", "left")
        .select(
            tx.transaction_id,
            tx.customer_id,
            tx.product_id,
            tx.store_id,
            tx.quantity,
            tx.transaction_date,
            stores.sales_region.alias("data_region"),
            (tx.quantity * prod.product_retail_price).alias("total_revenue"),
            ((tx.quantity * prod.product_retail_price) - (tx.quantity * prod.product_cost)).alias("total_profit")
        )
    )


@dlt.table(
    name="fact_returns",
    comment="Gold Fact Returns Table with Liquid Clustering",
    cluster_by=["store_id", "product_id"],
    table_properties=GOLD_TABLE_PROPERTIES
)
def fact_returns():
    returns_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_returns")
    products_df = dlt.read("dim_products")
    stores_df = dlt.read("dim_stores")

    return (
        returns_df.join(products_df, "product_id", "inner")
        .join(stores_df, "store_id", "left")
        .select(
            returns_df.return_date,
            returns_df.store_id,
            returns_df.product_id,
            returns_df.quantity.alias("returned_quantity"),
            (returns_df.quantity * products_df.product_cost).alias("total_return_cost"),
            stores_df.sales_region.alias("data_region")
        )
    )


@dlt.table(
    name="fact_inventory_snapshot",
    comment="Gold Fact Inventory Table for Real time Stock Alerts",
    cluster_by=["store_id", "product_id"],
    table_properties=GOLD_TABLE_PROPERTIES
)
def fact_inventory_snapshot():
    inv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_inventory")
    prod = dlt.read("dim_products")
    stores_df = dlt.read("dim_stores")

    return (
        inv.join(prod, "product_id", "inner")
        .join(stores_df, "store_id", "left")
        .select(
            inv.inventory_date,
            inv.store_id,
            inv.product_id,
            inv.stock_on_hand,
            inv.reorder_flag,
            prod.product_name,
            stores_df.sales_region.alias("data_region")
        )
    )