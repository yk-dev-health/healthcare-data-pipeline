#!/usr/bin/env bash
# Minimal-cost Cloud Run deployment for manual verification (issue #6).
#
# Deploys the API only, with Pub/Sub and Redis both disabled, so the service
# runs entirely inside GCP's free tier for a low-traffic smoke test:
#   - PUBSUB_ENABLED is left off (no GOOGLE_APPLICATION_CREDENTIALS is set,
#     so common/config.py's from_env() defaults it to false).
#   - IDEMPOTENCY_FAIL_MODE=open lets the process run without a reachable
#     Redis (see common/config.py's RedisSettings / IdempotencySettings).
#   - APP_ENV stays "development" on purpose: validate_for_runtime() in
#     common/config.py refuses to boot with fail_mode=open when APP_ENV is a
#     production alias, and this deployment is a cost-free smoke test, not a
#     production deployment of real PHI. Do not point this at real patient
#     data or flip APP_ENV to production without also provisioning Redis,
#     Pub/Sub, and a real PATIENT_HASH_SALT secret.
#   - min-instances=0 and a single max instance keep this inside the Cloud
#     Run free tier (2M requests / 180K vCPU-seconds / 360K GiB-seconds per
#     month) for anything but sustained traffic.
#
# Usage:
#   PROJECT_ID=your-gcp-project ./scripts/deploy_cloud_run.sh
#
# Requires: gcloud CLI authenticated (`gcloud auth login`) with a billing
# account attached to the project (Cloud Run requires billing to be enabled
# even though usage here should stay within the free tier).

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID to your GCP project id}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-healthcare-pipeline-api}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${SERVICE_NAME}/${SERVICE_NAME}:$(git rev-parse --short HEAD)"

echo "Project:  ${PROJECT_ID}"
echo "Region:   ${REGION}"
echo "Service:  ${SERVICE_NAME}"
echo "Image:    ${IMAGE}"

gcloud config set project "${PROJECT_ID}" >/dev/null

# Artifact Registry repo (idempotent; first 0.5 GB/month is free).
gcloud artifacts repositories describe "${SERVICE_NAME}" --location="${REGION}" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "${SERVICE_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Healthcare data pipeline API"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

docker build -t "${IMAGE}" .
docker push "${IMAGE}"

gcloud run deploy "${SERVICE_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform=managed \
  --allow-unauthenticated \
  --min-instances=0 \
  --max-instances=1 \
  --cpu=1 \
  --memory=512Mi \
  --port=8000 \
  --set-env-vars="APP_ENV=development,PROJECT_ID=${PROJECT_ID},IDEMPOTENCY_FAIL_MODE=open,PUBSUB_ENABLED=false,QUARANTINE_STORE_PAYLOAD=false"

echo
echo "Deployed. Smoke test:"
SERVICE_URL="$(gcloud run services describe "${SERVICE_NAME}" --region="${REGION}" --format='value(status.url)')"
echo "  curl ${SERVICE_URL}/health/live"
echo "  curl ${SERVICE_URL}/health/ready"
