"""
Airflow DAG: Daily Snowflake Extraction Pipeline
--------------------------------------------------
Extracts daily order data from Snowflake and lands it in a staging area
as Parquet for downstream processing. Includes retry logic and
failure alerting suitable for production pipelines.

Author: Ana
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta


def alert_on_failure(context):
    """Send alert notification when a task fails (e.g. Slack, PagerDuty, email)."""
    task_instance = context.get("task_instance")
    print(f"ALERT: Task {task_instance.task_id} failed on {context.get('ds')}")


default_args = {
    "owner": "data-eng-lead",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


def extract_from_snowflake(**kwargs):
    """Pull previous day's orders from Snowflake and write to staging as Parquet."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_prod")
    query = "SELECT * FROM sales.orders WHERE order_date = CURRENT_DATE - 1"
    df = hook.get_pandas_df(query)
    output_path = f"/data/staging/orders_{kwargs['ds']}.parquet"
    df.to_parquet(output_path)
    print(f"Extracted {len(df)} rows to {output_path}")


with DAG(
    dag_id="snowflake_daily_extract",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["snowflake", "etl", "production"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_from_snowflake,
    )
