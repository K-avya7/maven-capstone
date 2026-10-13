import json
import os
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, sum as spark_sum
from logger import CentralLogger
import argparse

def log_dlt_pipeline_execution(pipeline_id: str, pipeline_name: str):
    spark = SparkSession.builder.getOrCreate()
    logger = CentralLogger(spark)

    # Query pipeline system event log
    events = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")

    # Extract pipeline execution status and timestamps
    state_events = events.filter(col("event_type") == "pipeline_state_change")

    start_row = (
        state_events.filter(get_json_object(col("details"), "$.pipeline_state_change.state") == "INITIALIZING")
        .orderBy(col("timestamp").desc())
        .first()
    )

    end_row = (
        state_events.filter(get_json_object(col("details"), "$.pipeline_state_change.state").isin(["COMPLETED", "FAILED"]))
        .orderBy(col("timestamp").desc())
        .first()
    )

    start_time = start_row["timestamp"] if start_row else datetime.now(timezone.utc)
    status = json.loads(end_row["details"])["pipeline_state_change"]["state"] if end_row else "SUCCESS"

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
    parser = argparse.ArgumentParser(description="Log DLT Pipeline Metrics")
    parser.add_argument("--pipeline_id", type=str, required=False, default=None, help="DLT Pipeline ID")
    parser.add_argument("--pipeline_name", type=str, required=False, default="silver_gold_medallion", help="DLT Pipeline Name")

    args, _ = parser.parse_known_args()

    spark_session = SparkSession.builder.getOrCreate()

    # 1. Try to read from command-line arguments (passed by Workflow DAG)
    dlt_id = args.pipeline_id
    dlt_name = args.pipeline_name

    # 2. Fall back to Spark conf if running inside DLT notebook runtime
    if not dlt_id:
        try:
            dlt_id = spark_session.conf.get("pipeline.id")
        except Exception:
            dlt_id = None

    if not dlt_id:
        print("Warning: No pipeline_id provided or found. Skipping event_log telemetry extraction.")
        sys.exit(0)

    log_dlt_pipeline_execution(dlt_id, dlt_name)