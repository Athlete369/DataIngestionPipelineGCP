import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from file_routing import move_file_after_harness
from harness_monitor import monitor_harness_pipeline
from harness_trigger import trigger_harness_pipeline
from preprocessing import get_file_details, preprocess_file

RAW_BUCKET = os.getenv("RAW_BUCKET", "project-22640081-9e62-441c-891-test-files")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id="gcs_pubsub_ingest_2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes= 2*60),
    default_args=default_args,
) as dag:

    preprocess = PythonOperator(
        task_id="preprocess_file",
        python_callable=preprocess_file,
        execution_timeout=timedelta(minutes=30),
    )

    trigger_harness = PythonOperator(
        task_id="trigger_harness_pipeline",
        python_callable=trigger_harness_pipeline,
    )

    monitor_harness = PythonOperator(
        task_id="monitor_harness_pipeline",
        python_callable=monitor_harness_pipeline,
    )

    move_file = PythonOperator(
        task_id="move_file_after_harness",
        python_callable=move_file_after_harness,
    )

    preprocess >> trigger_harness >> monitor_harness >> move_file
