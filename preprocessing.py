import os

from google.cloud import storage
from adhoc_xl_to_csv import main

RAW_BUCKET = os.getenv("RAW_BUCKET", "project-22640081-9e62-441c-891-test-files")


def validate_csv_content(file_bytes: bytes) -> str:
    """Return a normalized CSV payload or raise a validation error."""
    import csv
    import io

    text = file_bytes.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows or not any(row for row in rows):
        raise ValueError("CSV file is empty or contains no usable rows.")
    return text


def get_file_details(**context):
    """Read the bucket and object info from the Airflow DAG run config."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    bucket_name = conf.get("bucket_name") or RAW_BUCKET
    object_name = conf.get("object_name")

    if not object_name:
        raise ValueError("dag_run.conf is missing object_name.")

    return {
        "bucket_name": bucket_name,
        "object_name": object_name,
        "file_name": os.path.basename(object_name),
    }


def preprocess_file(**context):
    """Step 1: validate and normalize the uploaded file before invoking downstream processing."""
    file_meta = get_file_details(**context)
    bucket_name = file_meta["bucket_name"]
    object_name = file_meta["object_name"]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():
        raise FileNotFoundError(f"File not found in bucket: gs://{bucket_name}/{object_name}")

    file_bytes = blob.download_as_bytes()
    extension = object_name.lower().rsplit(".", 1)[-1] if "." in object_name else ""

    if extension == "csv":
        validate_csv_content(file_bytes)
    elif extension in {"xls", "xlsx"}:
        main(f"gs://{bucket_name}/{object_name}", "output-folder") # need to edit
    elif extension == "txt":
        file_bytes.decode("utf-8-sig")
    else:
        raise ValueError(f"Unsupported file type: {object_name}")

    context["ti"].xcom_push(key="file_meta", value=file_meta)
    return file_meta
