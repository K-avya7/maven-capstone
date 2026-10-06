import uuid
import traceback
from datetime import datetime
from pyspark.sql import SparkSession

class AuditLogger:
    def __init__(self, spark: SparkSession, pipeline_name: str, task_name: str):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.task_name = task_name
        self.log_id = str(uuid.uuid4())
        self.start_time = datetime.now()
        
    def log_success(self, records_processed: int = 0, records_failed: int = 0):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self._write_log(
            status="SUCCESS",
            end_time=end_time,
            duration=duration,
            records_processed=records_processed,
            records_failed=records_failed,
            error_message=None,
            stack_trace=None
        )

    def log_failure(self, error: Exception):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self._write_log(
            status="FAILED",
            end_time=end_time,
            duration=duration,
            records_processed=0,
            records_failed=0,
            error_message=str(error),
            stack_trace=traceback.format_exc()
        )

    def _write_log(self, status, end_time, duration, records_processed, records_failed, error_message, stack_trace):
        log_data = [(
            self.log_id, self.pipeline_name, self.task_name, status,
            self.start_time, end_time, duration, records_processed,
            records_failed, error_message, stack_trace,
            self.spark.eval("CURRENT_USER()"), datetime.now()
        )]
        
        columns = [
            "log_id", "pipeline_name", "task_name", "execution_status",
            "start_time", "end_time", "duration_seconds", "records_processed",
            "records_failed", "error_message", "stack_trace",
            "executed_by", "log_timestamp"
        ]
        
        df = self.spark.createDataFrame(log_data, schema=columns)
        df.write.format("delta").mode("append").saveAsTable("dbx_maven_market.audit.audit_logs")