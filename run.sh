#!/bin/bash
set -e

# Configuration: Update these before running the script
PROJECT="bigdatapivot"
REGION="us-central1"
ARTIFACT_REGISTRY_REPO="${REGION}-docker.pkg.dev/bigdatapivot/dannymccormick-demo-test"
# End configuration: You can leave all variables below this line unchanged

PREFIX="sentiment-analysis-demo" # Unique prefix for your particular run. You can optionally configure this.
IMAGE_NAME="${ARTIFACT_REGISTRY_REPO}/${PREFIX}-template:latest"
LAUNCHER_IMAGE_NAME="${ARTIFACT_REGISTRY_REPO}/${PREFIX}-launcher:latest"
TOPIC_NAME="${PREFIX}-sentiment-topic"
BUCKET_NAME="${PREFIX}-flex-templates"
BUCKET_URL="gs://${BUCKET_NAME}"
TEMPLATE_PATH="${BUCKET_URL}/templates/sentiment-metadata.json"
TEMP_LOCATION="${BUCKET_URL}/temp"

echo "=================================="
echo "Starting Flex Template Deployment"
echo "=================================="

# 1. Setup Pub/Sub topic for testing
echo "Setting up Pub/Sub topic: ${TOPIC_NAME}"
set +e
gcloud pubsub topics describe $TOPIC_NAME --project $PROJECT >/dev/null 2>&1
TOPIC_EXISTS=$?
set -e

if [ $TOPIC_EXISTS -ne 0 ]; then
    echo "Topic does not exist. Creating topic..."
    gcloud pubsub topics create $TOPIC_NAME --project $PROJECT
else
    echo "Topic already exists."
fi

# Ensure bucket exists
set +e
gcloud storage buckets describe $BUCKET_URL --project $PROJECT >/dev/null 2>&1
BUCKET_EXISTS=$?
set -e

if [ $BUCKET_EXISTS -ne 0 ]; then
    echo "Bucket ${BUCKET_URL} does not exist. Creating..."
    gcloud storage buckets create $BUCKET_URL --project $PROJECT --location $REGION
else
    echo "Bucket ${BUCKET_URL} already exists."
fi

# 2. Build the flex template
BUILD_TEMPLATE=true
set +e
gcloud storage ls $TEMPLATE_PATH >/dev/null 2>&1
TEMPLATE_EXISTS=$?
set -e

if [ $TEMPLATE_EXISTS -eq 0 ]; then
    read -p "Flex template already exists at $TEMPLATE_PATH. Do you want to overwrite it? (y/N) " overwrite
    if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
        echo "Skipping build step..."
        BUILD_TEMPLATE=false
    fi
fi

if [ "$BUILD_TEMPLATE" = true ]; then
    echo "Building custom Flex Template SDK/worker image..."
    gcloud builds submit --tag $IMAGE_NAME --project $PROJECT .

    echo "Building Flex Template launcher image..."
    gcloud builds submit \
        --project $PROJECT \
        --config cloudbuild.launcher.yaml \
        --substitutions=_SDK_IMAGE=$IMAGE_NAME,_LAUNCHER_IMAGE=$LAUNCHER_IMAGE_NAME \
        .

    echo "Building Flex Template spec..."
    gcloud dataflow flex-template build $TEMPLATE_PATH \
        --image $LAUNCHER_IMAGE_NAME \
        --sdk-language "PYTHON" \
        --metadata-file "metadata.json" \
        --project $PROJECT
fi

TIMESTAMP=$(date +%s)
TOKEN=$(gcloud auth application-default print-access-token)
API_URL="https://dataflow.googleapis.com/v1b3/projects/$PROJECT/locations/$REGION/flexTemplates:launch"

launch_job() {
  local job_name=$1
  local backend=$2
  local experiments=$3

  local payload
  if [ -n "$experiments" ]; then
    payload=$(cat <<EOF
{
  "launchParameter": {
    "jobName": "${job_name}",
    "containerSpecGcsPath": "${TEMPLATE_PATH}",
    "launchOptions": {
      "ft_launch_timeout_secs": "1500"
    },
    "parameters": {
      "input_topic": "projects/${PROJECT}/topics/${TOPIC_NAME}",
      "sdk_container_image": "${IMAGE_NAME}",
      "inference_backend": "${backend}",
      "disk_size_gb": "100"
    },
    "environment": {
      "tempLocation": "${TEMP_LOCATION}",
      "machineType": "n1-standard-16",
      "additionalExperiments": ["${experiments}"]
    }
  }
}
EOF
)
  else
    payload=$(cat <<EOF
{
  "launchParameter": {
    "jobName": "${job_name}",
    "containerSpecGcsPath": "${TEMPLATE_PATH}",
    "launchOptions": {
      "ft_launch_timeout_secs": "1500"
    },
    "parameters": {
      "input_topic": "projects/${PROJECT}/topics/${TOPIC_NAME}",
      "sdk_container_image": "${IMAGE_NAME}",
      "inference_backend": "${backend}",
      "disk_size_gb": "100"
    },
    "environment": {
      "tempLocation": "${TEMP_LOCATION}",
      "machineType": "n1-standard-16"
    }
  }
}
EOF
)
  fi

  curl -s -X POST "${API_URL}" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${payload}"
}

# 3a. Launch the Gemma job (local vLLM on GPU via right-fitting)
GEMMA_JOB_NAME="${PREFIX}-sentiment-gemma-${TIMESTAMP}"
echo "Launching Gemma job: ${GEMMA_JOB_NAME}"
launch_job "${GEMMA_JOB_NAME}" "gemma" "enable_streaming_rightfitting"
echo ""

# 3b. Launch the Gemini job (Vertex AI API, no GPU)
GEMINI_JOB_NAME="${PREFIX}-sentiment-gemini-${TIMESTAMP}"
echo "Launching Gemini job: ${GEMINI_JOB_NAME}"
launch_job "${GEMINI_JOB_NAME}" "gemini" ""
echo ""



# 4. Ask the user to input messages which are relayed to the pubsub topic
echo ""
echo "==========================================================="
echo "✅ Both pipelines launched and reading from the same topic!"
echo "  Gemma job:  ${GEMMA_JOB_NAME}"
echo "  Gemini job: ${GEMINI_JOB_NAME}"
echo "Topic: projects/$PROJECT/topics/$TOPIC_NAME"
echo ""
echo "Enter messages below to publish to the Pub/Sub topic."
echo "Type 'quit' or 'exit' to stop and exit the loop."
echo "==========================================================="

while true; do
    read -p "Message > " msg
    if [[ "$msg" == "quit" || "$msg" == "exit" ]]; then
        echo "Exiting interactive loop."
        break
    fi
    
    if [[ -n "$msg" ]]; then
        set +e
        gcloud pubsub topics publish $TOPIC_NAME --message "$msg" --project $PROJECT >/dev/null
        if [ $? -eq 0 ]; then
            echo "Published: $msg"
        else
            echo "Failed to publish message."
        fi
        set -e
    fi
done

echo ""
echo "==========================================================="
echo "Canceling Dataflow jobs..."
for JOB_NAME in "$GEMMA_JOB_NAME" "$GEMINI_JOB_NAME"; do
    JOB_ID=$(gcloud dataflow jobs list --project=$PROJECT --region=$REGION --filter="name=${JOB_NAME}" --format="value(id)")
    if [[ -n "$JOB_ID" ]]; then
        echo "Canceling ${JOB_NAME} (${JOB_ID})..."
        gcloud dataflow jobs cancel $JOB_ID --region $REGION --project $PROJECT
        echo "Canceled."
    else
        echo "Could not find job ${JOB_NAME} to cancel."
    fi
done
echo "==========================================================="
