from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from src.common.logger import CentralLogger

CATALOG = "dbx_maven_market"
SCHEMA = "bronze"
STORAGE_ACCOUNT = "sgmavenmarket"
CONTAINER = "uc-root"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

def ingest_adls_entity(spark: SparkSession, logger: CentralLogger, entity_name: str, target_table_name: str):
    start_time = datetime.utcnow()
    pipeline_name = "bronze_adls_ingestion"
    
    landing_path = f"{BASE_PATH}/landing/{entity_name}/"
    schema_path = f"{BASE_PATH}/schemas/{entity_name}/"
    checkpoint_path = f"{BASE_PATH}/checkpoints/{entity_name}/"
    target_table = f"{CATALOG}.{SCHEMA}.{target_table_name}"
    
    try:
        df_stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(landing_path)
        )
        
        df_enriched = (
            df_stream
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
        )
        
        query = (
            df_enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table)
        )
        query.awaitTermination()
        
        processed_count = spark.table(target_table).count()
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=target_table_name,
            execution_status="SUCCESS",
            start_time=start_time,
            records_processed=processed_count
        )
    except Exception as e:
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=target_table_name,
            execution_status="FAILED",
            start_time=start_time,
            error=e
        )
        raise e

def run():
    spark = SparkSession.builder.appName("IngestADLSBatch").getOrCreate()
    logger = CentralLogger(spark)
    
    entities = [
        ("transactions", "transactions_1997"),
        ("customers", "customers"),
        ("regions", "regions"),
        ("stores", "stores"),
        ("calendar", "calendar"),
        ("returns", "returns")
    ]
    
    for entity_folder, table_name in entities:
        ingest_adls_entity(spark, logger, entity_folder, table_name)

if __name__ == "__main__":
    run()