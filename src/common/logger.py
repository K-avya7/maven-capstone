import uuid
import traceback
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
)


class CentralLogger:
    def __init__(self, spark: SparkSession, audit_table: str = "dbx_maven_market.audit.audit_logs"):
        self.spark = spark
        self.audit_table = audit_table

    def log_execution(
        self,
        pipeline_name: str,
        task_name: str,
        execution_status: str,
        start_time: datetime,
        records_processed: int = 0,
        records_failed: int = 0,
        error: Exception = None,
    ):
        end_time = datetime.now(timezone.utc)

        # Handle offset-naive start_time input automatically
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        duration_seconds = float((end_time - start_time).total_seconds())

        try:
            executed_by = self.spark.sql("SELECT current_user()").collect()[0][0]
        except Exception:
            executed_by = "system"

        error_message = str(error) if error else None
        stack_trace = traceback.format_exc() if error else None

        log_data = [(
            str(uuid.uuid4()),
            pipeline_name,
            task_name,
            execution_status,
            start_time,
            end_time,
            duration_seconds,
            int(records_processed),
            int(records_failed),
            error_message,
            stack_trace,
            executed_by,
            end_time,
        )]

        schema = StructType([
            StructField("log_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("task_name", StringType(), True),
            StructField("execution_status", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("records_processed", LongType(), True),
            StructField("records_failed", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("stack_trace", StringType(), True),
            StructField("executed_by", StringType(), True),
            StructField("log_timestamp", TimestampType(), True),
        ])

        log_df = self.spark.createDataFrame(log_data, schema=schema)
        log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)