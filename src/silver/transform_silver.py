import dlt
from pyspark.sql.functions import col, lit, to_date, to_timestamp, concat_ws, coalesce

# Load dynamic pipeline parameters
CATALOG = spark.conf.get("pipeline.catalog", "dbx_maven_market")
BRONZE_SCHEMA = spark.conf.get("pipeline.bronze_schema", "bronze")

# ==========================================
# 1. BRONZE SOURCE VIEWS
# ==========================================

@dlt.view
def view_bronze_transactions_batch():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_1997")

@dlt.view
def view_bronze_orders_stream():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_orders")

@dlt.view
def view_bronze_customers():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("ignoreChanges", "true")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_customers")
    )

@dlt.view
def view_bronze_products():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("ignoreChanges", "true")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_products")
    )

@dlt.view
def view_bronze_stores():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.stores")

@dlt.view
def view_bronze_regions():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.regions")

@dlt.view
def view_bronze_returns():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.analyst_returns_csv")

@dlt.view
def view_bronze_inventory():
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_inventory")


# ==========================================
# 2. SILVER TABLES & TRANSFORMATIONS
# ==========================================

# --- UNIFIED TRANSACTIONS ---
@dlt.table(
    name="silver_transactions",
    comment="Unified view of historical batch transactions and streaming live orders",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def silver_transactions():
    batch_df = dlt.read("view_bronze_transactions_batch").select(
        concat_ws("-", lit("batch"), col("store_id"), col("customer_id"), col("product_id"), col("transaction_date")).alias("transaction_id"),
        col("customer_id").cast("string"),
        col("product_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("int"),
        coalesce(to_timestamp(col("transaction_date")), to_date(col("transaction_date"))).alias("transaction_date"),
        lit("BATCH_CSV").alias("source_type")
    )
    
    stream_df = dlt.read("view_bronze_orders_stream").select(
        concat_ws("-", lit("stream"), col("customer_id"), col("product_id"), col("transaction_date")).alias("transaction_id"),
        col("customer_id").cast("string"),
        col("product_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("int"),
        coalesce(to_timestamp(col("transaction_date")), to_date(col("transaction_date"))).alias("transaction_date"),
        lit("KAFKA_STREAM").alias("source_type")
    )
    
    return batch_df.unionByName(stream_df)


# --- STORES & REGIONS DENORMALIZATION ---
@dlt.table(
    name="silver_stores",
    comment="Enriched store dimension denormalized with region attributes",
    table_properties={"quality": "silver"}
)
def silver_stores():
    stores_df = dlt.read("view_bronze_stores")
    regions_df = dlt.read("view_bronze_regions")
    
    return stores_df.join(regions_df, "region_id", "left").select(
        col("store_id").cast("string"),
        col("region_id").cast("string"),
        col("store_type"),
        col("store_name"),
        col("store_city"),
        col("store_state"),
        col("store_country"),
        col("sales_region"),
        col("sales_district"),
        col("total_sqft").cast("int"),
        col("grocery_sqft").cast("int")
    )


# --- RETURNS ---
@dlt.table(
    name="silver_returns",
    comment="Cleaned product returns data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_return_quantity", "quantity > 0")
def silver_returns():
    return dlt.read("view_bronze_returns").select(
        coalesce(to_timestamp(col("return_date")), to_date(col("return_date"))).alias("return_date"),
        col("product_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("int")
    )


# --- REAL-TIME INVENTORY ---
@dlt.table(
    name="silver_inventory",
    comment="Cleaned real-time inventory snapshot from Kafka stream",
    table_properties={"quality": "silver"}
)
def silver_inventory():
    return dlt.read("view_bronze_inventory").select(
        coalesce(to_timestamp(col("inventory_date")), to_date(col("inventory_date"))).alias("inventory_date"),
        col("store_id").cast("string"),
        col("product_id").cast("string"),
        col("stock_on_hand").cast("int"),
        col("reorder_flag").cast("int")
    )


# --- SCD TYPE 2: CUSTOMERS ---
dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 tracking table for Customer master attributes",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_customers",
    source="view_bronze_customers",
    keys=["customer_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "customer_address", "customer_city", "customer_state_province",
        "marital_status", "yearly_income", "member_card"
    ]
)


# --- SCD TYPE 2: PRODUCTS ---
dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 2 tracking table for Product catalog attributes",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_products",
    source="view_bronze_products",
    keys=["product_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["product_retail_price", "product_cost"]
)