import dlt
from pyspark.sql.functions import col

# --- DIMENSIONS ---
@dlt.table(
    name="dim_customers",
    comment="Active Gold Customer Dimension",
    table_properties={"delta.enableChangeDataFeed": "true"}
)
def dim_customers():
    return spark.table("dbx_maven_market.silver.silver_customers").filter(col("__END_AT").isNull())


@dlt.table(
    name="dim_products",
    comment="Active Gold Product Dimension"
)
def dim_products():
    return spark.table("dbx_maven_market.silver.silver_products").filter(col("__END_AT").isNull())


@dlt.table(
    name="dim_stores",
    comment="Active Gold Store Dimension"
)
def dim_stores():
    return spark.table("dbx_maven_market.silver.silver_stores")


# --- FACT TABLES ---
@dlt.table(
    name="fact_sales",
    comment="Gold Fact Sales Table with Liquid Clustering",
    cluster_by=["store_id", "product_id"]
)
def fact_sales():
    tx = spark.table("dbx_maven_market.silver.silver_transactions")
    prod = dlt.read("dim_products")

    return tx.join(prod, "product_id", "inner").select(
        tx.transaction_id,
        tx.customer_id,
        tx.product_id,
        tx.store_id,
        tx.quantity,
        tx.transaction_date,
        (tx.quantity * prod.product_retail_price).alias("total_revenue"),
        ((tx.quantity * prod.product_retail_price) - (tx.quantity * prod.product_cost)).alias("total_profit")
    )


@dlt.table(
    name="fact_returns",
    comment="Gold Fact Returns Table with Liquid Clustering",
    cluster_by=["store_id", "product_id"]
)
def fact_returns():
    returns_df = spark.table("dbx_maven_market.silver.silver_returns")
    products_df = dlt.read("dim_products")

    return returns_df.join(products_df, "product_id", "inner").select(
        returns_df.return_date,
        returns_df.store_id,
        returns_df.product_id,
        returns_df.quantity.alias("returned_quantity"),
        (returns_df.quantity * products_df.product_cost).alias("total_return_cost")
    )