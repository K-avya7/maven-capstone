# Databricks notebook source
import os
import sys
import urllib.parse
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pyspark.sql.functions import current_timestamp, lit

# Programmatically import CentralLogger from project root
try:
  notebook_path = (
      dbutils.notebook.entry_point.getDbutils()
      .notebook()
      .getContext()
      .notebookPath()
      .get()
  )
  project_root = "/Workspace" + os.path.dirname(
      os.path.dirname(os.path.dirname(notebook_path))
  )
  if project_root not in sys.path:
    sys.path.insert(0, project_root)
except Exception:
  pass

from src.common.logger import CentralLogger


def ingest_mongodb_products_native():
  start_time = datetime.utcnow()
  logger = CentralLogger(spark)

  try:
    # 1. Retrieve password securely from Unity Catalog Secret
    raw_password = dbutils.secrets.get(
        catalog="maven_market", schema="mongodb", key="mongodb_password"
    )

    # 2. URL-encode password
    encoded_password = urllib.parse.quote_plus(raw_password)

    # 3. Connect via native PyMongo
    cluster_host = "cluster0.sfjjdaf.mongodb.net"
    mongo_uri = f"mongodb+srv://maven_admin:{encoded_password}@{cluster_host}/?retryWrites=true&w=majority"

    client = MongoClient(mongo_uri)
    db = client["mavenmarket"]
    collection = db["products"]

    # Fetch records as a list of Python dicts
    records = list(collection.find())

    # Validate non-empty records
    if not records:
      raise ValueError("MongoDB collection 'mavenmarket.products' is empty!")

    # Drop BSON ObjectIDs (_id)
    for record in records:
      record.pop("_id", None)

    # 4. Convert Pandas DataFrame to PySpark DataFrame
    pandas_df = pd.DataFrame(records)
    df_raw = spark.createDataFrame(pandas_df)

    # 5. Append Bronze audit metadata columns
    df_bronze = df_raw.withColumn(
        "_ingestion_timestamp", current_timestamp()
    ).withColumn("_source_system", lit("MongoDB Atlas - Products Master"))

    # 6. Write to Unity Catalog Delta Bronze Table
    target_table = "dbx_maven_market.bronze.raw_products"
    (
        df_bronze.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    row_count = df_bronze.count()
    print(
        f"Ingestion complete! Successfully wrote {row_count} records to"
        f" '{target_table}'."
    )

    logger.log_execution(
        pipeline_name="bronze_mongodb_ingestion",
        task_name="raw_products",
        execution_status="SUCCESS",
        start_time=start_time,
        records_processed=row_count,
    )
  except Exception as e:
    logger.log_execution(
        pipeline_name="bronze_mongodb_ingestion",
        task_name="raw_products",
        execution_status="FAILED",
        start_time=start_time,
        error=e,
    )
    raise e


# Execute ingestion
ingest_mongodb_products_native()