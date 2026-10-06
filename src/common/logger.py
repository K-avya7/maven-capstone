import traceback
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class CentralLogger:

  def __init__(
      self,
      spark: SparkSession,
      catalog: str = "dbx_maven_market",
      schema: str = "audit",
  ):
    self.spark = spark
    self.audit_table = f"{catalog}.{schema}.audit_logs"

  def log_execution(
      self,
      pipeline_name: str,
      task_name: str,
      execution_status: str,
      start_time: datetime,
      end_time: datetime = None,
      records_processed: int = 0,
      records_failed: int = 0,
      error: Exception = None,
      executed_by: str = "Databricks_Workflow",
  ):
    if end_time is None:
      end_time = datetime.utcnow()

    duration_seconds = float((end_time - start_time).total_seconds())
    error_message = str(error) if error else None
    stack_trace = (
        "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        if error
        else None
    )

    log_schema = StructType([
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
        datetime.utcnow(),
    )]

    log_df = self.spark.createDataFrame(log_data, schema=log_schema)
    log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)