import os
import sys
from datetime import datetime, timezone

# 1. Path resolution for Databricks Bundle workspace execution
try:
    script_dir = os.path.dirname(__file__)
except NameError:
    script_dir = os.getcwd()

for p in [
    script_dir,
    os.path.abspath(os.path.join(script_dir, "../..")),
    os.path.abspath(os.path.join(script_dir, "..")),
]:
    if p not in sys.path:
        sys.path.insert(0, p)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.dbutils import DBUtils
from src.common.logger import CentralLogger


def run():
    spark = SparkSession.builder.appName("IngestKafkaOrders").getOrCreate()
    dbutils = DBUtils(spark)
    logger = CentralLogger(spark)

    start_time = datetime.now(timezone.utc)
    pipeline_name = "bronze_kafka_ingestion"
    task_name = "raw_orders"

    try:
        # 2. Schema definition matching incoming Kafka JSON payload
        transaction_schema = StructType([
            StructField("transaction_date", StringType(), True),
            StructField("stock_date", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
        ])

        # 3. Retrieve secrets & Confluent Kafka connection setup
        kafka_server = "pkc-w77k7w.centralus.azure.confluent.cloud:9092"
        kafka_topic = "orders-stream"

        kafka_key = dbutils.secrets.get(scope="maven-secrets", key="kafka-api-key")
        kafka_secret = dbutils.secrets.get(scope="maven-secrets", key="kafka-api-secret")

        # Shaded login module required for Databricks Serverless Compute
        jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'

        # 4. Stream extraction from Kafka
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

        # 5. Parse JSON payload and construct Bronze attributes
        parsed_transactions_df = (
            raw_kafka_stream
            .select(
                col("key").cast("string").alias("customer_key"),
                from_json(col("value").cast("string"), transaction_schema).alias("data"),
                col("timestamp").alias("kafka_timestamp"),
            )
            .select(
                "customer_key",
                "data.transaction_date",
                "data.stock_date",
                "data.product_id",
                "data.customer_id",
                "data.store_id",
                "data.quantity",
                "kafka_timestamp",
            )
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source", expr("'confluent_kafka_orders_stream'"))
        )

        # 6. Stream sink configuration to Unity Catalog Delta Table
        checkpoint_path = "/Volumes/dbx_maven_market/default/my_volume/checkpoints/orders_stream_checkpoint_serverless/"
        target_table = "dbx_maven_market.bronze.raw_orders"

        query = (
            parsed_transactions_df.writeStream
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_path)
            .toTable(target_table)
        )
        query.awaitTermination()

        # 7. Extract processed record count directly from streaming metrics
        record_count = sum(progress.get("numInputRows", 0) for progress in query.recentProgress)

        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=task_name,
            execution_status="SUCCESS",
            start_time=start_time,
            records_processed=record_count,
        )

    except Exception as e:
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=task_name,
            execution_status="FAILED",
            start_time=start_time,
            error=e,
        )
        raise e


if __name__ == "__main__":
    run()