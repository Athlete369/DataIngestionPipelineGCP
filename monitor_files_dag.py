import csv
import io
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pandas as pd

RAW_BUCKET = os.getenv("RAW_BUCKET", "project-22640081-9e62-441c-891-test-files")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "project-22640081-9e62-441c-891-processed")
QUARANTINE_BUCKET = os.getenv("QUARANTINE_BUCKET", "project-22640081-9e62-441c-891-quarantine")

def process_uploaded_file(**context):
    """Handle a file uploaded to GCS and triggered by Cloud Function."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    bucket_name = conf.get("bucket_name") or RAW_BUCKET
    object_name = conf.get("object_name")

    if not object_name:
        raise ValueError("dag_run.conf is missing object_name.")

    print(f"Processing file: gs://{bucket_name}/{object_name}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


with DAG(
    dag_id="gcs_pubsub_ingest_2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_file = PythonOperator(
        task_id="process_uploaded_file",
        python_callable=process_uploaded_file,
    )
