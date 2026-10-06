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

# --- FACT TABLE ---
@dlt.table(
    name="fact_sales",
    comment="Gold Fact Sales Table with Liquid Clustering",
    table_properties={
        "delta.liquidClusteringColumns": "store_id,product_id"
    }
)
def fact_sales():
    tx = spark.table("dbx_maven_market.silver.silver_transactions")
    prod = dlt.read("dim_products")
    
    return tx.join(prod, tx.product_id == prod.product_id, "inner").select(
        tx.transaction_id,
        tx.customer_id,
        tx.product_id,
        tx.store_id,
        tx.quantity,
        tx.transaction_date,
        (tx.quantity * prod.product_retail_price).alias("total_revenue"),
        ((tx.quantity * prod.product_retail_price) - (tx.quantity * prod.product_cost)).alias("total_profit")
    )