import os
import sys
import yaml
from datetime import datetime, timezone
import traceback

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
from pyspark.sql.functions import current_timestamp, col
from src.common.logger import CentralLogger


def load_config():
    config_path = os.environ.get("CONFIG_PATH", "config/config.yml")
    if not os.path.exists(config_path):
        config_path = os.path.abspath(os.path.join(script_dir, "../../config/config.yml"))

    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {
            "catalog": "dbx_maven_market",
            "schemas": {"bronze": "bronze"},
            "storage": {
                "root_path": "abfss://uc-root@sgmavenmarket.dfs.core.windows.net"
            },
            "ingestion": {
                "batch_csv_entities": [
                    {"folder": "transactions", "table": "transactions_1997"},
                    {"folder": "regions", "table": "regions"},
                    {"folder": "stores", "table": "stores"},
                    {"folder": "calendar", "table": "calendar"},
                ]
            },
        }


def ingest_adls_entity(
    spark: SparkSession,
    logger: CentralLogger,
    entity_folder: str,
    target_table_name: str,
    catalog: str,
    schema: str,
    base_path: str,
):
    start_time = datetime.now(timezone.utc)
    pipeline_name = "bronze_adls_ingestion"
    target_table = f"{catalog}.{schema}.{target_table_name}"

    landing_path = f"{base_path}/landing/{entity_folder}/"
    schema_path = f"{base_path}/schemas/{entity_folder}/"
    checkpoint_path = f"{base_path}/checkpoints/{entity_folder}/"

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

        records_processed = sum(
            progress.get("numInputRows", 0) for progress in query.recentProgress
        )

        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=entity_folder,
            execution_status="SUCCESS",
            start_time=start_time,
            records_processed=records_processed,
            records_failed=0,
        )
        print(f"✅ Successfully ingested {records_processed} rows from 'landing/{entity_folder}/' into '{target_table}'.")

    except Exception as e:
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=entity_folder,
            execution_status="FAILED",
            start_time=start_time,
            records_processed=0,
            records_failed=0,
            error=e,
        )
        print(f"❌ Error ingesting entity '{entity_folder}': {str(e)}")
        traceback.print_exc()
        raise e


def run():
    spark = SparkSession.builder.appName("IngestADLSBatch").getOrCreate()
    logger = CentralLogger(spark)
    config = load_config()

    catalog = config.get("catalog", "dbx_maven_market")
    schema = config.get("schemas", {}).get("bronze", "bronze")
    base_path = config.get("storage", {}).get("root_path", "abfss://uc-root@sgmavenmarket.dfs.core.windows.net")
    entities_config = config.get("ingestion", {}).get(
        "batch_csv_entities",
        [
            {"folder": "transactions", "table": "transactions_1997"},
            {"folder": "regions", "table": "regions"},
            {"folder": "stores", "table": "stores"},
            {"folder": "calendar", "table": "calendar"},
        ],
    )

    for item in entities_config:
        entity_folder = item.get("folder")
        table_name = item.get("table")
        print(f"\n--- Ingesting landing/{entity_folder}/ -> {catalog}.{schema}.{table_name} ---")
        ingest_adls_entity(
            spark=spark,
            logger=logger,
            entity_folder=entity_folder,
            target_table_name=table_name,
            catalog=catalog,
            schema=schema,
            base_path=base_path,
        )


if __name__ == "__main__":
    run()