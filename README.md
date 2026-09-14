# Production-Ready Data Ingestion Platform

## 1. Project vision
This project is a production-grade, event-driven file ingestion pipeline that accepts files from external sources, validates and normalizes them, orchestrates the processing workflow, and integrates with deployment automation and monitoring.

The current concept already includes the key building blocks:
- file intake from external systems
- storage in Google Cloud Storage
- orchestration with Apache Airflow
- preprocessing and normalization
- CI/CD control via Harness

The goal is to evolve this from a working proof of concept into a robust enterprise-ready platform.

---

## 2. Business goals
- Ingest structured and semi-structured files reliably
- Support CSV, XLS, XLSX, and TXT inputs
- Reduce manual intervention in file processing
- Ensure data quality before downstream consumption
- Handle retries, failure isolation, and auditability
- Enable secure and traceable deployment across environments
- Provide operational visibility through monitoring and alerts

---

## 3. Functional scope

### Core flow
1. File arrives from a source system or client upload
2. File is stored in a staging bucket
3. Event trigger fires for the file object
4. Airflow DAG starts processing
5. File metadata is captured
6. Type and format are identified
7. Validation rules are applied
8. Conversion or normalization is performed if required
9. Processed data is moved to the target destination
10. Success or failure status is logged and surfaced

### File types supported
- CSV
- TXT
- XLS
- XLSX

### Processing outcomes
- Processed successfully
- Rejected due to schema issue
- Quarantined due to invalid content
- Retried on transient errors
- Failed permanently with logged exception details

---

## 4. Target architecture

```text
External Sources
    │
    ▼
File Upload / Webhook / Transfer
    │
    ▼
Google Cloud Storage (Raw + Staging)
    │
    ▼
GCS Object Finalize Event
    │
    ▼
Pub/Sub Topic: gcs-file-events
    │
    ▼
Cloud Function: gcs-to-composer-trigger
    │
    │  Extracts bucket_name + object_name
    │  Calls Composer DAG REST API
    │
    ▼
Google Cloud Composer / Apache Airflow
    │
    ├── DAG: gcs_pubsub_ingest
    ├── File Detection
    ├── Metadata Extraction
    ├── Validation Layer
    ├── File Conversion (XLS/XLSX -> CSV)
    ├── Business Rule Checks
    ├── Move valid file to processed bucket
    ├── Send invalid file to quarantine bucket
    └── Audit + Status Logging
    │
    ▼
Target System / Data Warehouse / API / Database
    │
    ▼
Monitoring, Logging, Alerts, and CI/CD Control
```

### Event flow in plain English
1. A file lands in the raw GCS bucket.
2. GCS emits an `OBJECT_FINALIZE` event.
3. The event is sent to the Pub/Sub topic.
4. The Cloud Function receives the message.
5. The function calls the Composer Airflow REST endpoint to trigger the DAG.
6. Airflow runs the pipeline to validate, transform, and move the file.

### IAM and IAP auth path
```text
GCS Object Finalize Event
    │
    ▼
Pub/Sub Topic: gcs-file-events
    │
    ▼
Cloud Function Runtime Identity
    │   serviceAccount: gcs-trigger-sa@project.iam.gserviceaccount.com
    │
    ├── needs: roles/composer.user
    │   └── allows the function to trigger DAGs in Composer
    │
    ├── if Composer is behind IAP:
    │   needs: roles/iap.httpsResourceAccessor
    │   └── allows the function to retrieve an ID token and access the protected webserver
    │
    └── also needs Cloud Run/Eventarc invocation access
        └── roles/run.invoker

                    │
                    ▼
      Composer Airflow Webserver (protected by IAP)
                    │
                    ▼
            REST endpoint: /api/v2/dags/<dag_id>/dagRuns
                    │
                    ▼
               DAG: gcs_pubsub_ingest
```

This explains why the function often fails with `401 Unauthorized` or `403 Forbidden`: the runtime identity is not allowed to reach the Composer webserver, or it is not registered with the right Airflow permission model.

### Quick troubleshooting matrix

| Symptom | Likely root cause | What to verify | Likely fix |
|---|---|---|---|
| No Pub/Sub message after upload | GCS notification not configured correctly | GCS bucket notification to the correct topic | Recreate the `OBJECT_FINALIZE` notification |
| Function never runs | Pub/Sub trigger is missing or `run.invoker` is missing | Function trigger config and Cloud Run permissions | Grant `roles/run.invoker` to the Eventarc/Pub/Sub service account |
| `401 Unauthorized` from Composer API | Function identity cannot access secured Composer URL | Composer webserver behind IAP, missing ID token flow | Grant `roles/iap.httpsResourceAccessor` and use the correct ID token flow |
| `403 Forbidden` from Composer API | Service account lacks `roles/composer.user` or Airflow user mapping | IAM and Airflow user permissions | Add `roles/composer.user` and confirm the runtime identity is recognized by Airflow |
| `422 Unprocessable Entity` | Payload structure or Airflow version endpoint mismatch | Request body and DAG endpoint version | Use the correct DAG endpoint for Airflow 3 and valid `conf` payload |
| DAG does not start even though function runs | Wrong DAG name or bad environment URL | `AIRFLOW_URL` and DAG ID | Use the Composer environment URL and `gcs_pubsub_ingest` exact DAG ID |
| File is uploaded but nothing moves to processed bucket | DAG ran but failed in task logic | Airflow task logs | Fix validation/conversion or bucket permissions |

---

## 5. Production-grade requirements

### Reliability
- Idempotent processing for repeated uploads
- Retry with exponential backoff for transient issues
- Dead-letter queue or quarantine bucket for invalid files
- Clear state transitions: received -> validated -> processing -> success/fail

### Data quality
- Schema validation
- Required column validation
- Empty or malformed values checks
- Duplicate detection
- Encoding and delimiter checks

### Security
- Least-privilege IAM roles
- Secret Manager for credentials
- Encrypted storage and transport
- Access controls for buckets and pipelines
- Data retention and masking for sensitive fields

### Observability
- Airflow task logs
- Cloud Logging and Monitoring
- Job success/failure dashboards
- Alerting on retries, pipeline failures, or SLA breaches
- Correlation IDs per file/job

### Governance
- Full audit trail with source details, timestamps, user/system origin
- Versioned file retention
- Clear ownership for pipeline operations
- Recovery and rollback playbook

---

## 6. Recommended technology stack

### Core platform
- Python for ingestion and validation logic
- Apache Airflow for orchestration
- Google Cloud Storage for raw and staging files
- Pub/Sub for event-driven notifications
- BigQuery or target DB for persistence

### Deployment and control
- Harness for CI/CD and pipeline control
- Terraform for infrastructure as code

### Security and operations
- Google Secret Manager
- Cloud Logging
- Cloud Monitoring
- Alerting policies

### Data processing libraries
- pandas
- openpyxl / xlrd / pyxlsb where needed
- csv module
- custom validation modules

---

---

## 8. Airflow DAG design

### DAG responsibilities
- Trigger on file arrival event
- Extract job metadata
- Route files into the correct processing path
- Validate file format and schema
- Convert unsupported formats
- Store processed output in staging or target storage
- Publish status to monitoring and downstream systems

### Suggested task flow
1. `start_job`
2. `check_file_exists`
3. `detect_file_type`
4. `extract_metadata`
5. `validate_schema`
6. `convert_excel_if_needed`
7. `normalize_data`
8. `move_to_processed_bucket`
9. `publish_success_event`
10. `on_failure_quarantine_and_alert`

### Retry behavior
- Retry transient network and validation errors
- Use Airflow retries and retry_delay
- Do not retry permanent business-rule failures automatically

---

## 9. Data validation strategy

### Inputs to validate
- file extension
- file size
- MIME type or content signature
- headers and column names
- null values
- invalid date or numeric formats
- duplicate rows
- unsupported delimiters or encoding

### Validation tiers
- Basic file checks
- Schema checks
- Data quality checks
- Business logic checks

### Failure handling
- invalid files go to a quarantine bucket
- exception metadata is saved
- alert is raised for operations teams
- reprocessing is possible through a controlled manual action

---

## 10. CI/CD and deployment strategy

### Recommended process
- Developer pushes changes to feature branch
- CI checks run: lint, unit tests, formatting, security scan
- Merge to main branch triggers deployment pipeline
- Harness pipeline is used for controlled deployment to dev/test/prod
- Deployment includes environment configuration validation and smoke tests

### Pipeline checks
- static code analysis
- unit tests
- integration tests
- dependency vulnerability scan
- schema validation tests
- deployment health check

---

## 11. Monitoring and alerting

### Monitor these signals
- failed DAG executions
- retries exceeding threshold
- file processing duration
- file rejection rate
- bucket size growth
- target write failures
- delayed file processing

### Alerts
- pipeline failed for more than X minutes
- file count dropped unexpectedly
- validation failure spike
- downstream sink failure
- infra or permission issue

---

## 12. Security and compliance controls

- Use IAM with least privilege
- Keep credentials in Secret Manager
- Encrypt bucket data at rest and in transit
- Restrict public access
- Log privileged actions
- Mask personally identifiable information before downstream datasets if required
- Maintain retention and deletion policies

---

## 13. Delivery roadmap

### Phase 1: Foundation
- validate architecture and source requirements
- create repo and environment structure
- provision GCP resources and IAM
- setup storage buckets
- define metadata model

### Phase 2: MVP ingestion
- build event trigger integration
- create Airflow DAG skeleton
- implement file type detection
- handle XLS/XLSX conversion to CSV
- create simple validation logic

### Phase 3: Production hardening
- add retries, quarantines, audit logs
- implement alerting and dashboards
- create deployment pipeline in Harness
- add infrastructure automation with Terraform

### Phase 4: Operational readiness
- load testing with sample data volume
- runbook and rollback testing
- incident response and escalation path
- production UAT and sign-off

---

## 14. Deliverables before production launch

- [ ] architecture and data flow documentation
- [ ] environment setup and IaC
- [ ] Airflow DAG implementation
- [ ] file validation and transformation logic
- [ ] retry and quarantine workflows
- [ ] monitoring and alert configuration
- [ ] Harness deployment pipeline
- [ ] unit and integration tests
- [ ] runbook for failure recovery
- [ ] security review and access policy approval
- [ ] UAT and go-live checklist

---

## 15. Composer + Cloud Function setup checklist

This is the practical sequence needed to connect a Cloud Function to Cloud Composer successfully and keep the DAG trigger reliable.

### Step 1: Create or identify the function service account
Create a dedicated service account for the Cloud Function so the runtime identity is explicit and manageable.

```bash
gcloud iam service-accounts create gcs-trigger-sa \
  --project=project-22640081-9e62-441c-891
```

Then attach the required permissions to the service account:

```bash
gcloud projects add-iam-policy-binding project-22640081-9e62-441c-891 \
  --member="serviceAccount:gcs-trigger-sa@project-22640081-9e62-441c-891.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"
```

If the function must access GCS and other platform resources, add the minimum required roles for those services as needed.

### Step 2: Check the function runtime identity
After deployment, verify the function is using the expected service account.

```bash
gcloud functions describe gcs-to-composer-trigger \
  --region=us-central1 \
  --format='value(serviceAccountEmail)'
```

This value must match the service account that will be registered in Airflow.

### Step 3: Get the numeric Google user ID
Use the service account email from the previous step.

```bash
gcloud iam service-accounts describe gcs-trigger-sa@project-22640081-9e62-441c-891.iam.gserviceaccount.com \
  --format='value(oauth2ClientId)'
```

The resulting value is the numeric user ID that becomes the Airflow username:

```text
accounts.google.com:10062717652170634220
```

### Step 4: Register the service account as an Airflow user
Use the Composer environment CLI to create a matching Airflow user. The username must match the exact service account Google identity.

```bash
gcloud composer environments run data-ingest-dev \
  --location=us-central1 \
  -- users create \
  --username=accounts.google.com:10062717652170634220 \
  --email=unique-id@example.com \
  --firstname=unique-id \
  --lastname=unique-id \
  --role=Op \
  --use-random-password
```

This is the most common fix for the 401 unauthorized error from the Composer REST API.

### Step 5: Deploy the Cloud Function with the same service account
When deploying the function, set the runtime service account explicitly.

```bash
gcloud functions deploy gcs-to-composer-trigger \
  --region=us-central1 \
  --runtime=python311 \
  --trigger-topic=YOUR_TOPIC_NAME \
  --service-account=gcs-trigger-sa@project-22640081-9e62-441c-891.iam.gserviceaccount.com
```

This ensures the function identity used for the Airflow token matches the Airflow user that was created.

### Step 6: Verify the Composer environment URL
Confirm the Composer URL before triggering the DAG.

```bash
gcloud composer environments describe data-ingest-dev \
  --location=us-central1 \
  --format='value(config.airflowUri)'
```

Use that exact value in the environment variable `AIRFLOW_URL`.

### Step 7: Trigger the DAG using the correct Airflow 3 endpoint
The environment in this project is Airflow 3.3.1, so the trigger endpoint is:

```text
https://<composer-url>/api/v2/dags/<dag_id>/dagRuns
```

A successful trigger payload should look like this:

```python
payload = {
    "conf": {
        "bucket_name": bucket_name,
        "object_name": object_name,
    },
    "logical_date": "2026-09-12T12:00:00Z",
    "dag_run_id": "gcs_pubsub_ingest-<object-name>-<uuid>",
}
```

### Step 8: Handle duplicate triggers safely
Pub/Sub and GCS notifications can be retried, so the function should treat 409 conflict responses as a duplicate trigger instead of a hard failure.

```python
if response.status_code == 409:
    logging.warning("Duplicate trigger received; DAG run already exists.")
    return {"status": "duplicate", "dag_id": DAG_ID}
```

This prevents false alarms when the same object event is delivered more than once.

### Known error patterns
- 401 or 403: service account is not registered in Airflow or wrong service account is running the function
- 422: request schema or body is not valid for the Composer Airflow version
- 409: duplicate DAG run or retried event; usually not a fatal condition for Pub/Sub triggers

### Minimum completion checklist
- [ ] Cloud Function has a dedicated runtime service account
- [ ] Service account numeric ID matches the Airflow user username
- [ ] Airflow user exists with role `Op`
- [ ] Composer `AIRFLOW_URL` matches the environment URL
- [ ] Function uses Airflow 3 endpoint `/api/v2/dags/.../dagRuns`
- [ ] Trigger payload includes a valid `conf` and unique `dag_run_id`
- [ ] Duplicate events are handled gracefully
- [ ] DAG can process the uploaded file and move it through the pipeline

---

## 16. Command log and explanations
This section captures the commands we ran while building and debugging the GCP ingestion flow, along with what each one was intended to accomplish.

### 16.1 GCP and Composer setup

#### Export the variable
```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export BUCKET_NAME="${PROJECT_ID}-raw-files"
export TOPIC_NAME="gcs-file-events"
export SUBSCRIPTION_NAME="gcs-file-events-sub"
```

#### Enable required APIs
```bash
gcloud services enable \
  storage.googleapis.com \
  pubsub.googleapis.com \
  composer.googleapis.com \
  cloudfunctions.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  --project "$PROJECT_ID"
```
Explanation: enables the services needed for Cloud Storage, Pub/Sub, Composer, Cloud Functions, logging, and monitoring.

#### Create a Composer service account
```bash
export SA_NAME="test123"
export SERVICE_ACCOUNT="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
gcloud iam service-accounts create "$SA_NAME" \
  --project="$PROJECT_ID"
```
Explanation: creates a dedicated identity for Composer-managed tasks so the app does not rely on a generic default account.

#### Grant Composer worker and storage permissions
```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/logging.logWriter"
```
Explanation: gives the service account the permissions needed to interact with Composer, GCS, Pub/Sub, and logs.

#### Create the Composer environment
```bash
gcloud composer environments create data-ingest-dev \
  --location "$REGION" \
  --project "$PROJECT_ID" \
  --image-version=composer-3-airflow-3.3.1 \
  --service-account="$SERVICE_ACCOUNT"
```
Explanation: creates the Cloud Composer 3 environment. The `--service-account` is required in the modern Composer setup.

#### Get the Composer Airflow URL
```bash
gcloud composer environments describe data-ingest-dev \
  --location=us-central1 \
  --project=project-22640081-9e62-441c-891 \
  --format="value(config.airflowUri)"
```
Explanation: resolves the exact Airflow webserver URL used by Composer. This value is passed to the Cloud Function as `AIRFLOW_URL`.

### 16.2 GCS and Pub/Sub trigger setup

#### Create the raw bucket
```bash
gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
```
Explanation: creates the bucket that receives uploaded files.

#### Create Pub/Sub topic and subscription
```bash
gcloud pubsub topics create "$TOPIC_NAME" --project "$PROJECT_ID"

gcloud pubsub subscriptions create "$SUBSCRIPTION_NAME" \
  --topic "$TOPIC_NAME" \
  --project "$PROJECT_ID"
```
Explanation: creates the Pub/Sub topic that receives object-finalize notifications and the subscription used for checking events manually.

#### Attach GCS bucket notifications to Pub/Sub
```bash
gcloud storage buckets notifications create "gs://$BUCKET_NAME" \
  --topic="$TOPIC_NAME" \
  --event-types=OBJECT_FINALIZE \
  --payload-format=json \
  --project "$PROJECT_ID"
```
Explanation: configures GCS so every new uploaded object sends a Pub/Sub event.

#### Validate message delivery
```bash
echo "hello world" > sample.txt
gsutil cp sample.txt "gs://$BUCKET_NAME/sample.txt"
gcloud pubsub subscriptions pull "$SUBSCRIPTION_NAME" \
  --project "$PROJECT_ID" \
  --auto-ack
```
Explanation: checks whether a new GCS upload created a Pub/Sub message. This helps confirm the trigger path is working.

#### Upload a test file
```bash
echo "id,name
1,alpha
2,beta" > sample.csv

gsutil cp sample.csv "gs://${PROJECT_ID}-raw-files/sample.csv"
```
Explanation: simulates a real file upload into the raw bucket to trigger the event-driven pipeline.

### 16.3 Cloud Function deployment and auth

#### Deploy the Cloud Function
1. Copy the **deploy.sh**, **requirements.txt** and **main.py**  in cloud shell, the deploy.sh has variable names which should be modified according to the environment.
```bash
FUNCTION_NAME="gcs-to-composer-trigger-2" # use the same name in deploy.sh and export it too
export FUNCTION_NAME="gcs-to-composer-trigger-2"
chmod +x deploy.sh
./deploy.sh
```
Explanation: packages the Cloud Function source and deploys it as a second-generation function listening on the Pub/Sub topic.

#### Grant Cloud Run invocation permission
```bash
export PROJECT_ID="project-22640081-9e62-441c-891"
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
export REGION="us-central1"
gcloud run services add-iam-policy-binding "$FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-eventarc.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

gcloud run services add-iam-policy-binding "$FUNCTION_NAME"     --region=us-central1     --member="serviceAccount:$SERVICE_ACCOUNT"     --role="roles/run.invoker"

```
Explanation: allows Eventarc/Pub/Sub to invoke the underlying Cloud Run service behind the function.

#### Grant Pub/Sub service account access
```bash
gcloud run services add-iam-policy-binding "$FUNCTION_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```
Explanation: adds the Pub/Sub service agent permission so events can invoke the function properly.

#### Check the Cloud Function trigger configuration
```bash
gcloud functions describe "$FUNCTION_NAME" \
  --region=us-central1 \
  --project=project-22640081-9e62-441c-891 \
  --format="yaml(eventTrigger)"
```
Explanation: verifies the function is bound to the correct Pub/Sub topic and is configured as a Pub/Sub trigger.

#### Read function logs
```bash
gcloud functions logs read "$FUNCTION_NAME" \
  --region=us-central1 \
  --project=project-22640081-9e62-441c-891 \
  --limit=50
```
Explanation: inspects runtime logs to verify that the Cloud Function is receiving the Pub/Sub events and whether the trigger logic is succeeding or failing.

### 16.4 Composer auth and DAG trigger debugging

#### Grant Composer access to the function service account
```bash
export COMPOSER_ENV="data-ingest-dev"
export SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role="roles/composer.user"
```
Explanation: grants the function runtime identity the permission needed to trigger Composer DAGs.

#### Grant IAP access if the Composer UI is behind IAP
```bash
gcloud iap web add-iam-policy-binding \
  --project="$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role="roles/iap.httpsResourceAccessor"
```
Explanation: allows the function service account to access the Composer webserver when the Airflow UI is protected by IAP.

#### Check whether the Airflow URL is behind IAP
Open the Composer Airflow URL in a browser.
Explanation: if the browser shows a Google sign-in page, the webserver is behind IAP.

### 16.5 IAM verification commands

#### Check all bindings for the service account
```bash
gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA}" \
  --format="json"
```
Explanation: verifies whether the service account has the expected IAM roles.

#### Check IAP policy
```bash
gcloud iap web get-iam-policy \
  --project="$PROJECT_ID"
```
Explanation: confirms whether IAP permissions for the service account are present.

#### Check whether the Composer webserver is behind IAP
```bash
# open this in a browser
https://<composer-airflow-url>
```
If the browser shows a Google Sign-In page, the Composer webserver is behind IAP. This is the most common indicator that the function must have `roles/iap.httpsResourceAccessor` to call the Airflow API successfully.

#### How to verify the permission is already granted
```bash
export SA="<function-service-account-email>"

gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA}" \
  --format="table(bindings.role,bindings.members)"
```
This should show entries such as:
- `roles/composer.user`
- `roles/iap.httpsResourceAccessor`

If the rows are missing, assign them again with:
```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role="roles/composer.user"

gcloud iap web add-iam-policy-binding \
  --project="$PROJECT_ID" \
  --member="serviceAccount:${SA}" \
  --role="roles/iap.httpsResourceAccessor"
```
Important: do not add `--resource-type=iap-web` for this CLI; newer versions reject it with `Invalid choice: 'iap-web'`.

### 16.6 Troubleshooting insights

These were the main root causes we worked through during implementation:
- missing `--service-account` when creating Composer environment
- Cloud Build service account lacked permission to build the Cloud Function
- Cloud Run / Eventarc invocation lacked `roles/run.invoker`
- Cloud Function was triggering Composer using the wrong authentication model for Composer 3
- Airflow webserver was behind IAP and required the service account to have IAP access
- the correct DAG name had to be explicitly matched: `gcs_pubsub_ingest`

### 16.7 Recommended command sequence for a fresh environment
```
1. Create a folder in cloud shell
2. Place deploy.sh, main.py and requirements.txt in it
3. after creating the composer, add monitor_files_dag.py in DAG folder
```


```bash
export PROJECT_ID="project-22640081-9e62-441c-891"
export REGION="us-central1"
export SA_NAME="composer-data-ingest"
export SERVICE_ACCOUNT="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export TOPIC_NAME="gcs-file-events"
export BUCKET_NAME="${PROJECT_ID}-raw-files"
export COMPOSER_ENV="data-ingest-dev"

# create service account
gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT_ID"

# grant permissions
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/logging.logWriter"

gcloud run services add-iam-policy-binding "$FUNCTION_NAME"     --region=us-central1     --member="serviceAccount:$SERVICE_ACCOUNT"     --role="roles/run.invoker"

# create bucket and Pub/Sub notification
gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
gcloud pubsub topics create "$TOPIC_NAME" --project "$PROJECT_ID"
gcloud storage buckets notifications create "gs://$BUCKET_NAME" \
  --topic="$TOPIC_NAME" \
  --event-types=OBJECT_FINALIZE \
  --payload-format=json \
  --project "$PROJECT_ID"

# deploy function
cd cloud_function
./deploy.sh

export NUMERIC_USER_ID=$(gcloud iam service-accounts describe "$SERVICE_ACCOUNT" --format="value(oauth2ClientId)")

export LOCATION=us-central1

gcloud composer environments run "$COMPOSER_ENV" \
    --location "$LOCATION" \
    users create -- \
    -u accounts.google.com:NUMERIC_USER_ID \
    -e UNIQUE_ID  \
    -f UNIQUE_ID \
    -l - -r Op --use-random-password


```
Explanation: this is the cleanest full setup sequence to reproduce the bucket-to-function-to-Composer flow in a fresh project.

---

## 17. End-to-end production validation checklist
Use this checklist to validate the full path after deployment.

### 17.1 Validate bucket notification
- [ ] A file is uploaded to the raw bucket.
- [ ] GCS emits an `OBJECT_FINALIZE` notification.
- [ ] The bucket notification is attached to the correct Pub/Sub topic.

```bash
gcloud storage buckets notifications list "gs://$BUCKET_NAME" --project "$PROJECT_ID"
```

### 17.2 Validate Pub/Sub delivery
- [ ] The topic exists.
- [ ] The subscription is active and the message appears after upload.

```bash
gcloud pubsub topics list --project "$PROJECT_ID"
gcloud pubsub subscriptions list --project "$PROJECT_ID"
gcloud pubsub subscriptions pull "$SUBSCRIPTION_NAME" --project "$PROJECT_ID" --auto-ack
```

### 17.3 Validate Cloud Function invocation
- [ ] The function is attached to the correct Pub/Sub topic.
- [ ] Function logs show the Pub/Sub event was received.
- [ ] No `401`, `403`, or `run.invoker` errors appear in the logs.

```bash
gcloud functions describe gcs-to-composer-trigger \
  --region=us-central1 \
  --project="$PROJECT_ID" \
  --format="yaml(eventTrigger)"

gcloud functions logs read gcs-to-composer-trigger \
  --region=us-central1 \
  --project="$PROJECT_ID" \
  --limit=50
```

### 17.4 Validate Composer auth and IAM
- [ ] Function service account has `roles/composer.user`.
- [ ] Function service account has `roles/iap.httpsResourceAccessor` if the Composer webserver is behind IAP.
- [ ] The Composer URL is reachable and the browser shows the Airflow page or sign-in page rather than a direct 401 from the app.

```bash
export SA="<function-service-account-email>"

gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA}" \
  --format="table(bindings.role,bindings.members)"
```

### 17.5 Validate DAG trigger path
- [ ] The function successfully calls the Composer REST endpoint.
- [ ] The DAG ID matches `gcs_pubsub_ingest`.
- [ ] The request payload includes `conf.bucket_name` and `conf.object_name`.
- [ ] The response is not a `401 Unauthorized` or `403 Forbidden`.

```bash
curl -X POST \
  "https://<composer-airflow-url>/api/v2/dags/gcs_pubsub_ingest/dagRuns" \
  -H "Authorization: Bearer <id_token>" \
  -H "Content-Type: application/json" \
  -d '{"conf":{"bucket_name":"<raw-bucket>","object_name":"sample.csv"}}'
```

### 17.6 Validate DAG execution
- [ ] The DAG shows as `running` or `success` in Airflow.
- [ ] Tasks complete without permission, auth, or file-format issues.
- [ ] Logs show the file was loaded, validated, and processed.

```bash
gcloud composer environments run data-ingest-dev \
  --location=us-central1 \
  --project="$PROJECT_ID" \
  -- tasks list gcs_pubsub_ingest
```

### 17.7 Validate file movement and quarantine handling
- [ ] Valid files move from raw bucket to processed bucket.
- [ ] Invalid files are moved to the quarantine bucket.
- [ ] `metadata` or error logs record the rejection reason.

```bash
gsutil ls "gs://${PROJECT_ID}-processed/"
gsutil ls "gs://${PROJECT_ID}-quarantine/"
```

### 17.8 Success criteria for production sign-off
The project is ready for production only when all of the following are true:
- [ ] Uploading a valid file triggers the DAG automatically.
- [ ] Uploading an invalid file is quarantined and logged.
- [ ] Access and auth errors are resolved for the function runtime identity.
- [ ] The event path works without a fixed cron schedule.
- [ ] Alerts and monitoring are in place for failed runs and repeated retries.
- [ ] IAM is least-privilege and matches the deployment environment.

---

## 18. Recommended next action
The immediate next step is to implement the project in phases:
1. create the repo structure
2. define the DAG and task interfaces
3. implement file detection and validation scripts
4. wire GCS trigger to Airflow
5. add Harness deployment pipeline
6. add monitoring and failure quarantine

This sequence keeps the work realistic, testable, and production-oriented.
