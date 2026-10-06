import urllib.parse
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from src.common.logger import CentralLogger

def run():
    spark = SparkSession.builder.appName("IngestMongoDBProducts").getOrCreate()
    logger = CentralLogger(spark)
    start_time = datetime.utcnow()
    pipeline_name = "bronze_mongodb_ingestion"
    task_name = "raw_products"
    
    try:
        # Retrieve password securely from Secret Scope
        try:
            raw_password = dbutils.secrets.get(scope="maven-secrets", key="mongodb_password")
        except Exception:
            raw_password = dbutils.secrets.get(catalog="maven_market", schema="mongodb", key="mongodb_password")

        encoded_password = urllib.parse.quote_plus(raw_password)
        cluster_host = "cluster0.sfjjdaf.mongodb.net"
        mongo_uri = f"mongodb+srv://maven_admin:{encoded_password}@{cluster_host}/?retryWrites=true&w=majority"
        
        client = MongoClient(mongo_uri)
        db = client["mavenmarket"]
        collection = db["products"]
        
        records = list(collection.find())
        if not records:
            raise ValueError("MongoDB collection 'mavenmarket.products' is empty!")
            
        for record in records:
            record.pop("_id", None)
            
        pandas_df = pd.DataFrame(records)
        df_raw = spark.createDataFrame(pandas_df)

        df_bronze = (
            df_raw
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_system", lit("MongoDB Atlas - Products Master"))
        )

        target_table = "dbx_maven_market.bronze.raw_products"
        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        row_count = df_bronze.count()
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=task_name,
            execution_status="SUCCESS",
            start_time=start_time,
            records_processed=row_count
        )
    except Exception as e:
        logger.log_execution(
            pipeline_name=pipeline_name,
            task_name=task_name,
            execution_status="FAILED",
            start_time=start_time,
            error=e
        )
        raise e

if __name__ == "__main__":
    run()