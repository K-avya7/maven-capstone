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


def get_config_path() -> str:
    """Resolves config path from Databricks widgets, environment variables, or local workspace paths."""
    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        widget_path = dbutils.widgets.get("config_path")
        if widget_path and os.path.exists(widget_path):
            return widget_path
    except Exception:
        pass

    env_path = os.environ.get("CONFIG_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    default_paths = [
        "config/config.yml",
        os.path.abspath(os.path.join(script_dir, "../../config/config.yml")),
        os.path.abspath(os.path.join(script_dir, "../config/config.yml")),
    ]
    for p in default_paths:
        if os.path.exists(p):
            return p

    raise FileNotFoundError("CRITICAL: Configuration file 'config.yml' could not be located.")


def load_config() -> dict:
    config_path = get_config_path()
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            if not config:
                raise ValueError(f"Config file at '{config_path}' is empty.")
            return config
    except Exception as e:
        print(f"❌ Error loading YAML config at '{config_path}': {e}")
        raise e


def ingest_adls_entity(
    spark: SparkSession,
    logger: CentralLogger,
    entity_folder: str,
    target_table_name: str,
    catalog: str,
    schema: str,
    base_path: str,
    checkpoint_base_path: str,
):
    start_time = datetime.now(timezone.utc)
    pipeline_name = "bronze_adls_ingestion"
    target_table = f"{catalog}.{schema}.{target_table_name}"

    landing_path = f"{base_path.rstrip('/')}/landing/{entity_folder}/"
    schema_path = f"{checkpoint_base_path.rstrip('/')}/schemas/{entity_folder}/"
    checkpoint_path = f"{checkpoint_base_path.rstrip('/')}/checkpoints/{entity_folder}/"

    try:
        df_stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.useNotifications", "false")
            .option("pathGlobFilter", "*.csv")
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

    catalog = config["catalog"]
    schema = config["schemas"]["bronze"]
    base_path = config["storage"]["root_path"]
    checkpoint_base_path = config["storage"].get("checkpoint_path", f"{base_path}/checkpoints")
    entities_config = config["ingestion"]["batch_csv_entities"]

    for item in entities_config:
        entity_folder = item["folder"]
        table_name = item["table"]
        print(f"\n--- Ingesting landing/{entity_folder}/ -> {catalog}.{schema}.{table_name} ---")
        ingest_adls_entity(
            spark=spark,
            logger=logger,
            entity_folder=entity_folder,
            target_table_name=table_name,
            catalog=catalog,
            schema=schema,
            base_path=base_path,
            checkpoint_base_path=checkpoint_base_path,
        )


if __name__ == "__main__":
    run()