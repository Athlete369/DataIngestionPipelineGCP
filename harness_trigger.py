import os

import requests
from airflow.exceptions import AirflowException

from preprocessing import get_file_details

HARNESS_API_URL = os.getenv("HARNESS_API_URL", "https://app.harness.io/gateway")
HARNESS_ORG_ID = os.getenv("HARNESS_ORG_ID")
HARNESS_PROJECT_ID = os.getenv("HARNESS_PROJECT_ID")
HARNESS_PIPELINE_ID = os.getenv("HARNESS_PIPELINE_ID")
HARNESS_API_KEY = os.getenv("HARNESS_API_KEY")
HARNESS_PIPELINE_EXECUTION_URL = os.getenv("HARNESS_PIPELINE_EXECUTION_URL")


def trigger_harness_pipeline(**context):
    """Trigger the Harness pipeline using the current file metadata as input parameters."""

    # TO REMOVE: I am adding for testing purposes
    print("Triggering Harness pipeline...")
    return

    #################
    file_meta = context["ti"].xcom_pull(task_ids="preprocess_file", key="file_meta") or get_file_details(**context)

    if not HARNESS_API_KEY:
        raise AirflowException("HARNESS_API_KEY environment variable is required to trigger the Harness pipeline.")
    if not HARNESS_PIPELINE_ID:
        raise AirflowException("HARNESS_PIPELINE_ID environment variable is required to trigger the Harness pipeline.")

    if HARNESS_PIPELINE_EXECUTION_URL:
        trigger_url = HARNESS_PIPELINE_EXECUTION_URL.rstrip("/")
    else:
        if not HARNESS_ORG_ID or not HARNESS_PROJECT_ID:
            raise AirflowException(
                "Either HARNESS_PIPELINE_EXECUTION_URL or both HARNESS_ORG_ID and HARNESS_PROJECT_ID must be set."
            )
        trigger_url = (
            f"{HARNESS_API_URL.rstrip('/')}/pipeline/api/v1/orgs/{HARNESS_ORG_ID}/projects/{HARNESS_PROJECT_ID}/"
            f"pipelines/{HARNESS_PIPELINE_ID}/executions"
        )

    payload = {
        "inputParams": {
            "bucket_name": file_meta["bucket_name"],
            "object_name": file_meta["object_name"],
            "file_name": file_meta["file_name"],
            "triggered_by": "airflow",
        },
        "tags": {
            "source": "gcs",
            "dag_id": "gcs_pubsub_ingest_2",
        },
    }

    response = requests.post(
        trigger_url,
        headers={
            "Content-Type": "application/json",
            "x-api-key": HARNESS_API_KEY,
        },
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    result = response.json()
    execution_id = (
        result.get("identifier")
        or result.get("id")
        or result.get("executionId")
        or result.get("data", {}).get("identifier")
        or result.get("data", {}).get("id")
    )

    if not execution_id:
        raise AirflowException(f"Harness pipeline trigger response did not include an execution id: {result}")

    context["ti"].xcom_push(key="harness_execution_id", value=execution_id)
    context["ti"].xcom_push(key="harness_trigger_response", value=result)
    return {"execution_id": execution_id, "status": "STARTED", **file_meta}
