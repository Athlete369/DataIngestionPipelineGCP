import os
import time

import requests
from airflow.exceptions import AirflowException

from preprocessing import get_file_details

HARNESS_API_URL = os.getenv("HARNESS_API_URL", "https://app.harness.io/gateway")
HARNESS_ORG_ID = os.getenv("HARNESS_ORG_ID")
HARNESS_PROJECT_ID = os.getenv("HARNESS_PROJECT_ID")
HARNESS_PIPELINE_ID = os.getenv("HARNESS_PIPELINE_ID")
HARNESS_API_KEY = os.getenv("HARNESS_API_KEY")
HARNESS_PIPELINE_STATUS_URL = os.getenv("HARNESS_PIPELINE_STATUS_URL")


def monitor_harness_pipeline(**context):
    # TO REMOVE: I am adding for testing purposes
    print("Monitoring Harness pipeline...")
    return

    """Poll the Harness execution until it finishes and return the terminal status."""
    file_meta = context["ti"].xcom_pull(task_ids="preprocess_file", key="file_meta") or get_file_details(**context)
    execution_id = context["ti"].xcom_pull(task_ids="trigger_harness_pipeline", key="harness_execution_id")

    if not execution_id:
        raise AirflowException("Harness execution id not found after trigger step.")

    if HARNESS_PIPELINE_STATUS_URL:
        status_url = HARNESS_PIPELINE_STATUS_URL.rstrip("/")
        status_url = f"{status_url}/{execution_id}"
    else:
        if not HARNESS_ORG_ID or not HARNESS_PROJECT_ID:
            raise AirflowException(
                "Either HARNESS_PIPELINE_STATUS_URL or both HARNESS_ORG_ID and HARNESS_PROJECT_ID must be set."
            )
        status_url = (
            f"{HARNESS_API_URL.rstrip('/')}/pipeline/api/v1/orgs/{HARNESS_ORG_ID}/projects/{HARNESS_PROJECT_ID}/"
            f"pipelines/{HARNESS_PIPELINE_ID}/executions/{execution_id}"
        )

    status = "UNKNOWN"
    for _ in range(30):
        time.sleep(10)
        response = requests.get(
            status_url,
            headers={
                "Content-Type": "application/json",
                "x-api-key": HARNESS_API_KEY,
            },
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        status = (
            payload.get("status")
            or payload.get("state")
            or payload.get("data", {}).get("status")
            or payload.get("data", {}).get("state")
            or "UNKNOWN"
        )
        status = str(status).upper()

        if status in {"SUCCESS", "SUCCEEDED", "COMPLETED"}:
            context["ti"].xcom_push(key="pipeline_status", value="success")
            return {"status": "success", **file_meta}

        if status in {"FAILED", "FAILURE", "ERROR", "CANCELLED", "TIMED_OUT"}:
            context["ti"].xcom_push(key="pipeline_status", value="failed")
            return {"status": "failed", **file_meta}

    raise AirflowException(
        f"Harness pipeline execution {execution_id} did not finish successfully within the expected timeout. "
        f"Latest known status: {status}"
    )
