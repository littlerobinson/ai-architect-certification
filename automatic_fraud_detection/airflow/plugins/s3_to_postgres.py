from typing import Sequence

import pandas as pd

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook


class S3ToPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "key",
        "table",
        "postgres_conn_id",
        "aws_conn_id",
    )

    def __init__(
        self,
        bucket,
        key,
        table,
        postgres_conn_id="postgres_id",
        aws_conn_id="aws_id",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket: str = bucket
        self.key: str = key
        self.table: str = table
        self.postgres_conn_id: str = postgres_conn_id
        self.aws_conn_id: str = aws_conn_id

    def execute(self, context):
        """Execute the transfer of data from S3 to PostgreSQL.

        Args:
            context: Airflow context containing runtime variables, task instance, etc.
                    Used to access execution_date, task instance, and other runtime information.
        """
        # Extract useful information from context
        task_instance = context["task_instance"]
        execution_date = context["execution_date"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        self.log.info(
            f"Starting S3 to PostgreSQL transfer for table {self.table} "
            f"(DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date})"
        )

        # Download from S3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_file = s3_hook.download_file(self.key, bucket_name=self.bucket)
        self.log.info(f"Downloaded file {self.key} from S3 bucket {self.bucket}")

        # Read data
        df = pd.read_csv(s3_file)
        self.log.info(f"Successfully read {len(df)} rows from the S3 file")

        # Add metadata columns from context if they don't exist
        row_count = len(df)
        # Convert execution_date to string format that PostgreSQL can handle
        execution_date_str = execution_date.strftime("%Y-%m-%d %H:%M:%S")
        df["execution_date"] = [execution_date_str] * row_count
        df["dag_id"] = [dag_id] * row_count
        df["task_id"] = [task_id] * row_count

        # # Convert timestamp column to datetime if it exists
        # if "timestamp" in df.columns:
        #     df["timestamp"] = pd.to_datetime(df["timestamp"])

        # PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Insert data
        self.log.info(f"Inserting data into table {self.table}")
        pg_hook.insert_rows(
            table=self.table, rows=df.values.tolist(), target_fields=list(df.columns)
        )

        # Push metrics to XCom
        task_instance.xcom_push(key="rows_inserted", value=len(df))
        task_instance.xcom_push(key="execution_time", value=str(execution_date))

        self.log.info(
            f"Successfully inserted {len(df)} rows into {self.table} "
            f"at {execution_date}"
        )
        return len(df)
