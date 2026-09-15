import os

from google.cloud import storage

PROCESSED_BUCKET = os.getenv("SUCCESS_BUCKET", os.getenv("PROCESSED_BUCKET", "project-22640081-9e62-441c-891-processed"))
FAILED_BUCKET = os.getenv("FAILED_BUCKET", os.getenv("QUARANTINE_BUCKET", "project-22640081-9e62-441c-891-quarantine"))


def move_blob_to_bucket(bucket_name: str, blob_name: str, target_bucket_name: str, target_name: str) -> None:
    client = storage.Client()
    source_bucket = client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = client.bucket(target_bucket_name)
    destination_blob = destination_bucket.blob(target_name)
    destination_blob.rewrite(source_blob)
    source_blob.delete()


def move_file_after_harness(**context):
    # TO REMOVE: I am adding for testing purposes
    print("Moving file after Harness pipeline...")
    return
    """Move the source file to the success or failed bucket depending on Harness status."""
    result = context["ti"].xcom_pull(task_ids="monitor_harness_pipeline") or {"status": "failed"}
    status = result.get("status", "failed")
    file_meta = context["ti"].xcom_pull(task_ids="preprocess_file", key="file_meta") or {}

    bucket_name = file_meta.get("bucket_name")
    object_name = file_meta.get("object_name")

    if not bucket_name or not object_name:
        raise ValueError("Missing bucket_name or object_name in the file metadata.")

    target_bucket = PROCESSED_BUCKET if status == "success" else FAILED_BUCKET
    move_blob_to_bucket(bucket_name, object_name, target_bucket, object_name)
    print(f"Moved {bucket_name}/{object_name} to {target_bucket} because Harness job status was {status}.")

    return {"status": status, "source_bucket": bucket_name, "target_bucket": target_bucket, "object_name": object_name}
