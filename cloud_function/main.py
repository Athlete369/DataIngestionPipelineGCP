import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

PROJECT_ID = os.environ["PROJECT_ID"]
DAG_ID = os.environ["DAG_ID"]
AIRFLOW_URL = os.environ["AIRFLOW_URL"]


def trigger_composer_dag(bucket_name: str, object_name: str) -> dict:
    """Trigger the Airflow DAG using the Cloud Function's default Google credentials."""
    logging.info("Triggering Composer DAG for bucket: %s, object: %s", bucket_name, object_name)

    airflow_web_server_url = AIRFLOW_URL.rstrip("/")
    session = AuthorizedSession(CREDENTIALS)

    logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    safe_object_name = object_name.replace("/", "-")
    dag_run_id = f"{DAG_ID}-{safe_object_name}-{uuid.uuid4().hex}"

    payload = {
        "conf": {
            "bucket_name": bucket_name,
            "object_name": object_name,
        },
        "logical_date": logical_date,
        "dag_run_id": dag_run_id,
    }
    url = f"{airflow_web_server_url}/api/v2/dags/{DAG_ID}/dagRuns"

    logging.info("Prepared request payload: %s", payload)
    logging.info("Initiating request to trigger Airflow DAG at target URL: %s", url)

    try:
        response = session.post(
            url,
            json=payload,
            timeout=30,
        )
        if response.status_code == 409:
            logging.warning(
                "Airflow DAG run already exists for %s/%s; treating as duplicate trigger.",
                bucket_name,
                object_name,
            )
            return {"status": "duplicate", "dag_id": DAG_ID, "bucket_name": bucket_name, "object_name": object_name}
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        error_text = str(exc)
        if status_code in {401, 403} or any(code in error_text for code in ("401", "403")):
            raise RuntimeError(
                "Airflow REST API rejected the authenticated service account. "
                "This commonly happens when the service account is not pre-registered as an Airflow user. "
                "Create an Airflow user with username 'accounts.google.com:NUMERIC_USER_ID' and any unique email value, "
                "using the numeric user ID from: "
                "gcloud iam service-accounts describe SA_NAME@PROJECT_ID.iam.gserviceaccount.com --format='value(oauth2ClientId)'. "
                "Then grant the appropriate Airflow role, for example 'Op'. If the problem persists, verify the service account has the required IAM role to access the environment."
            ) from exc
        raise
    return response.json()


def main(event, context):
    """Triggered by Pub/Sub messages emitted by GCS OBJECT_FINALIZE notifications."""
    if not event or "data" not in event:
        print("No Pub/Sub message payload found.")
        return

    try:
        message = base64.b64decode(event["data"]).decode("utf-8")
        payload = json.loads(message)
    except Exception as exc:
        raise ValueError(f"Unable to decode Pub/Sub message payload: {exc}") from exc

    bucket_name = payload.get("bucket")
    object_name = payload.get("name")

    if not bucket_name or not object_name:
        raise ValueError(f"Missing bucket or object name in payload: {payload}")

    result = trigger_composer_dag(bucket_name, object_name)
    print(f"Triggered DAG {DAG_ID} for {bucket_name}/{object_name}: {result}")
    return result
