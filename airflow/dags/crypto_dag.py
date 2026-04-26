from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

from logger_config import setup_logger
from lakehouse_export import run as run_lakehouse_export

logger = setup_logger()

default_args = {
    "owner": "crypto_pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="ETH-USD pipeline: dbt transformations → Ducklake export",
    schedule="*/15 * * * *",
    start_date=datetime.now(),
    catchup=False,
    tags=["crypto", "ethusd", "dbt", "ducklake"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/workspace/crypto_pipeline_dbt && /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/workspace/crypto_pipeline_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/workspace/crypto_pipeline_dbt && /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/workspace/crypto_pipeline_dbt",
    )

    lakehouse_export = PythonOperator(
        task_id="lakehouse_export",
        python_callable=run_lakehouse_export,
    )

    dbt_run >> dbt_test >> lakehouse_export