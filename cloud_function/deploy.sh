#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT_ID="project-22640081-9e62-441c-891"
REGION="us-central1"
TOPIC_NAME="gcs-file-events-2"
FUNCTION_NAME="gcs-to-composer-trigger-2"
COMPOSER_ENV="data-ingest-dev-2"
DAG_ID="gcs_pubsub_ingest_2"
SA_NAME="test123"

SERVICE_ACCOUNT="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AIRFLOW_URL="$(gcloud composer environments describe "$COMPOSER_ENV" --location="$REGION" --project="$PROJECT_ID" --format="value(config.airflowUri)")"
export PROJECT_ID REGION TOPIC_NAME FUNCTION_NAME COMPOSER_ENV DAG_ID SERVICE_ACCOUNT AIRFLOW_URL

gcloud functions deploy "$FUNCTION_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --gen2 \
  --source="$SCRIPT_DIR" \
  --runtime=python311 \
  --trigger-topic="$TOPIC_NAME" \
  --entry-point=main \
  --service-account="$SERVICE_ACCOUNT" \
  --memory=512MB \
  --timeout=120s \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,COMPOSER_ENV=$COMPOSER_ENV,REGION=$REGION,DAG_ID=$DAG_ID,AIRFLOW_URL=$AIRFLOW_URL"

echo "Cloud Function deployed successfully."
