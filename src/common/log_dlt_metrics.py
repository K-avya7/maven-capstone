import os
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, sum as spark_sum
from src.common.logger import CentralLogger

def log_dlt_pipeline_execution(pipeline_id: str, pipeline_name: str):
    spark = SparkSession.builder.getOrCreate()
    logger = CentralLogger(spark)

    # Query pipeline system event log
    events = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")

    # Extract pipeline execution status and timestamps
    state_events = events.filter(col("event_type") == "pipeline_state_change")
    
    start_row = (
        state_events.filter(col("details:pipeline_state_change.state") == "INITIALIZING")
        .orderBy(col("timestamp").desc())
        .first()
    )
    
    end_row = (
        state_events.filter(col("details:pipeline_state_change.state").isin(["COMPLETED", "FAILED"]))
        .orderBy(col("timestamp").desc())
        .first()
    )

    start_time = start_row["timestamp"] if start_row else datetime.now(timezone.utc)
    status = end_row["details"]["pipeline_state_change"]["state"] if end_row else "SUCCESS"

    # Aggregate processed rows across pipeline flows
    flow_progress = events.filter(col("event_type") == "flow_progress")
    
    total_records = 0
    if not flow_progress.isEmpty():
        records_df = flow_progress.select(
            get_json_object(col("details"), "$.flow_progress.metrics.num_output_rows").cast("long").alias("num_rows")
        )
        agg_result = records_df.select(spark_sum("num_rows")).collect()[0][0]
        if agg_result:
            total_records = int(agg_result)

    # Persist log metrics into audit table
    logger.log_execution(
        pipeline_name=pipeline_name,
        task_name=f"{pipeline_name}_dlt_run",
        execution_status=status,
        start_time=start_time,
        records_processed=total_records,
        records_failed=0
    )
    print(f"Logged DLT pipeline {pipeline_name} status {status} with {total_records} records processed.")

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    dlt_id = spark_session.conf.get("pipeline.id", "default_pipeline_id")
    dlt_name = spark_session.conf.get("pipeline.name", "silver_gold_medallion")
    
    log_dlt_pipeline_execution(dlt_id, dlt_name)