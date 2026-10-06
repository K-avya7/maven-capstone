import dlt
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit, expr

# --- UNIFIED TRANSACTIONS (STREAM + BATCH) ---
@dlt.view
def view_bronze_transactions_batch():
    return dlt.read("dbx_maven_market.bronze.transactions_1997")

@dlt.view
def view_bronze_orders_stream():
    return dlt.read("dbx_maven_market.bronze.raw_orders")

@dlt.table(
    name="silver_transactions",
    comment="Unified view of historical batch transactions and streaming live orders",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def silver_transactions():
    batch_df = dlt.read("view_bronze_transactions_batch").select(
        col("transaction_id").cast("string"),
        col("customer_id").cast("string"),
        col("product_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("int"),
        col("transaction_date").cast("timestamp"),
        lit("BATCH_CSV").alias("source_type")
    )
    
    stream_df = dlt.read("view_bronze_orders_stream").select(
        col("order_id").alias("transaction_id").cast("string"),
        col("customer_id").cast("string"),
        col("product_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("int"),
        col("order_timestamp").alias("transaction_date").cast("timestamp"),
        lit("KAFKA_STREAM").alias("source_type")
    )
    
    return batch_df.unionByName(stream_df)

# --- SCD TYPE 2: CUSTOMERS ---
dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 tracking table for Customer master attributes"
)

dlt.apply_changes(
    target="silver_customers",
    source="dbx_maven_market.bronze.customers",
    keys=["customer_id"],
    sequence_by=col("acct_open_date"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "customer_address", "customer_city", "customer_state_province",
        "marital_status", "yearly_income", "member_card"
    ]
)

# --- SCD TYPE 2: PRODUCTS ---
dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 2 tracking table for Product catalog attributes"
)

dlt.apply_changes(
    target="silver_products",
    source="dbx_maven_market.bronze.raw_products",
    keys=["product_id"],
    sequence_by=col("_ingest_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["product_retail_price", "product_cost"]
)