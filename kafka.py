from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Define JSON Schema matching the 6-column payload
transaction_schema = StructType([
    StructField("transaction_date", StringType(), True),
    StructField("stock_date", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])

# 2. Retrieve Secrets & Setup Confluent Connection (Shaded JAAS Module for Serverless)
kafka_server = "pkc-w77k7w.centralus.azure.confluent.cloud:9092"
kafka_topic = "orders-stream"

kafka_key = dbutils.secrets.get(scope="maven-secrets", key="kafka-api-key")
kafka_secret = dbutils.secrets.get(scope="maven-secrets", key="kafka-api-secret")

# Use shaded LoginModule prefix required by Serverless Compute
jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'

# 3. Read Stream from Kafka
raw_kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_server)
    .option("subscribe", kafka_topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Cast Binary Payload to String & Extract JSON Columns
parsed_transactions_df = (
    raw_kafka_stream
    .select(
        col("key").cast("string").alias("customer_key"),
        from_json(col("value").cast("string"), transaction_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )
    .select(
        "customer_key",
        "data.transaction_date",
        "data.stock_date",
        "data.product_id",
        "data.customer_id",
        "data.store_id",
        "data.quantity",
        "kafka_timestamp"
    )
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", expr("'confluent_kafka_orders_stream'"))
)

# 5. Write Stream into Unity Catalog Bronze Delta Table
checkpoint_path = "/Volumes/dbx_maven_market/default/my_volume/checkpoints/orders_stream_checkpoint_serverless2/"

streaming_query = (
    parsed_transactions_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)  # Native trigger mode for Serverless runs
    .option("checkpointLocation", checkpoint_path)
    .toTable("dbx_maven_market.bronze.raw_orders")
)