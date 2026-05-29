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

echo "=================================="
echo "Gmail API Authentication Setup"
echo "=================================="
echo "Ensuring the Gmail API is enabled in your Google Cloud Project (${PROJECT})..."
gcloud services enable gmail.googleapis.com --project "${PROJECT}"
echo ""
echo "To allow the ADK agent to send emails via Gmail, the pipeline requires an OAuth access token."
echo "If you have not done so, run this exact command on a single line to log in with the required scopes:"
echo "  gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/gmail.send"
echo ""
echo "You can generate a fresh token by running:"
echo "  gcloud auth application-default print-access-token"
echo "=================================="
read -p "Enter OAuth Access Token (leave empty to attempt automatic retrieval): " USER_TOKEN

if [ -z "$USER_TOKEN" ]; then
    echo "Attempting to retrieve token automatically..."
    TOKEN=$(gcloud auth application-default print-access-token)
else
    TOKEN="$USER_TOKEN"
fi

# Validate token scopes and validity
echo "Validating OAuth access token..."
TOKEN_INFO=$(curl -s "https://oauth2.googleapis.com/tokeninfo?access_token=${TOKEN}")
if echo "$TOKEN_INFO" | grep -q "error"; then
    echo "ERROR: Invalid or expired access token."
    echo "Details: $TOKEN_INFO"
    exit 1
fi

if ! echo "$TOKEN_INFO" | grep -q "https://www.googleapis.com/auth/gmail.send"; then
    echo "ERROR: The access token is missing the required Gmail scope (https://www.googleapis.com/auth/gmail.send)."
    echo "To fix this, please run the following command exactly on a single line to re-authenticate:"
    echo "  gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/gmail.send"
    echo ""
    echo "Then run this script again."
    exit 1
fi
echo "Access token validated successfully (contains required Gmail scopes)."

# Read refresh token, client ID and client secret if available
ADC_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.config/gcloud/application_default_credentials.json}"
REFRESH_TOKEN=""
CLIENT_ID=""
CLIENT_SECRET=""

if [ -f "$ADC_PATH" ]; then
    echo "Found local Application Default Credentials: $ADC_PATH"
    REFRESH_TOKEN=$(python3 -c "import json; d=json.load(open('$ADC_PATH')); print(d.get('refresh_token', ''))" 2>/dev/null || echo "")
    CLIENT_ID=$(python3 -c "import json; d=json.load(open('$ADC_PATH')); print(d.get('client_id', ''))" 2>/dev/null || echo "")
    CLIENT_SECRET=$(python3 -c "import json; d=json.load(open('$ADC_PATH')); print(d.get('client_secret', ''))" 2>/dev/null || echo "")
fi

if [ -n "$REFRESH_TOKEN" ]; then
    echo "Found refresh token in local ADC. Auto-refresh will be enabled on the Dataflow worker."
else
    echo "WARNING: No refresh token found in local ADC. Credentials might expire on the worker after 1 hour."
fi

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
      "disk_size_gb": "100",
      "user_access_token": "${TOKEN}",
      "user_refresh_token": "${REFRESH_TOKEN}",
      "user_client_id": "${CLIENT_ID}",
      "user_client_secret": "${CLIENT_SECRET}"
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
      "disk_size_gb": "100",
      "user_access_token": "${TOKEN}",
      "user_refresh_token": "${REFRESH_TOKEN}",
      "user_client_id": "${CLIENT_ID}",
      "user_client_secret": "${CLIENT_SECRET}"
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

# ---------------------------------------------------------------------------
# BigQuery demo table setup
# ---------------------------------------------------------------------------
BQ_DATASET="sentiment_demo"

echo ""
echo "=== BigQuery Demo Table Setup ==="
read -p "Do you want to (re)create the BigQuery demo tables? (y/N) " setup_bq
if [[ "$setup_bq" =~ ^[Yy]$ ]]; then

  # Check how many of the 3 tables currently exist
  BQ_TABLES_EXIST=0
  set +e
  bq show --project_id=$PROJECT "${BQ_DATASET}.users"    > /dev/null 2>&1 && BQ_TABLES_EXIST=$((BQ_TABLES_EXIST+1))
  bq show --project_id=$PROJECT "${BQ_DATASET}.purchases" > /dev/null 2>&1 && BQ_TABLES_EXIST=$((BQ_TABLES_EXIST+1))
  bq show --project_id=$PROJECT "${BQ_DATASET}.products"  > /dev/null 2>&1 && BQ_TABLES_EXIST=$((BQ_TABLES_EXIST+1))
  set -e

  RECREATE_BQ=true
  if [ "$BQ_TABLES_EXIST" -eq 3 ]; then
    # All three exist — ask once
    read -p "All 3 demo tables already exist. Delete and recreate them? (y/N) " recreate_choice
    if [[ ! "$recreate_choice" =~ ^[Yy]$ ]]; then
      echo "Skipping BigQuery table setup."
    RECREATE_BQ=false
    fi
  elif [ "$BQ_TABLES_EXIST" -gt 0 ]; then
    # Partial state — auto-recreate without prompting
    echo "Only $BQ_TABLES_EXIST of 3 demo tables exist. Automatically recreating all tables..."
  fi

  if [ "$RECREATE_BQ" = true ]; then
    # Ask for demo email address once
    read -p "Enter a demo email address to use for all test users: " DEMO_EMAIL

    # Ensure dataset exists
    set +e
    bq show --project_id=$PROJECT "$BQ_DATASET" > /dev/null 2>&1
    DATASET_EXISTS=$?
    set -e
    if [ $DATASET_EXISTS -ne 0 ]; then
      echo "Creating BigQuery dataset: $BQ_DATASET"
      bq mk --dataset --project_id=$PROJECT "$BQ_DATASET"
    fi

    # Drop any tables that already exist
    set +e
    bq rm -f --table --project_id=$PROJECT "${BQ_DATASET}.users"
    bq rm -f --table --project_id=$PROJECT "${BQ_DATASET}.purchases"
    bq rm -f --table --project_id=$PROJECT "${BQ_DATASET}.products"
    set -e

    # Create tables
    echo "Creating table: users"
    bq mk --table --project_id=$PROJECT \
      "${BQ_DATASET}.users" \
      "user_id:INTEGER,user_email:STRING"

    echo "Creating table: products"
    bq mk --table --project_id=$PROJECT \
      "${BQ_DATASET}.products" \
      "product_id:INTEGER,remaining_inventory:INTEGER,price:FLOAT"

    echo "Creating table: purchases"
    bq mk --table --project_id=$PROJECT \
      "${BQ_DATASET}.purchases" \
      "user_id:INTEGER,order_id:INTEGER,product_id:INTEGER"

    # Populate: users (IDs 1-10, all sharing the provided email)
    echo "Populating users table..."
    bq query --use_legacy_sql=false --project_id=$PROJECT \
      "INSERT INTO \`${PROJECT}.${BQ_DATASET}.users\` (user_id, user_email) VALUES
       (1,'${DEMO_EMAIL}'),(2,'${DEMO_EMAIL}'),(3,'${DEMO_EMAIL}'),
       (4,'${DEMO_EMAIL}'),(5,'${DEMO_EMAIL}'),(6,'${DEMO_EMAIL}'),
       (7,'${DEMO_EMAIL}'),(8,'${DEMO_EMAIL}'),(9,'${DEMO_EMAIL}'),
       (10,'${DEMO_EMAIL}')"

    # Populate: products (5 products with varying inventory)
    echo "Populating products table..."
    bq query --use_legacy_sql=false --project_id=$PROJECT \
      "INSERT INTO \`${PROJECT}.${BQ_DATASET}.products\` (product_id, remaining_inventory, price) VALUES
       (1, 52, 29.99),
       (2,  3, 89.99),
       (3, 120, 14.99),
       (4,  0, 249.99),
       (5, 18, 49.99)"

    # Populate: purchases (3 orders per user, user IDs 1-10 = 30 rows)
    echo "Populating purchases table..."
    bq query --use_legacy_sql=false --project_id=$PROJECT \
      "INSERT INTO \`${PROJECT}.${BQ_DATASET}.purchases\` (user_id, order_id, product_id) VALUES
       (1,1001,1),(1,1002,3),(1,1003,5),
       (2,1004,2),(2,1005,4),(2,1006,1),
       (3,1007,3),(3,1008,5),(3,1009,2),
       (4,1010,4),(4,1011,1),(4,1012,3),
       (5,1013,5),(5,1014,2),(5,1015,4),
       (6,1016,1),(6,1017,3),(6,1018,5),
       (7,1019,2),(7,1020,4),(7,1021,1),
       (8,1022,3),(8,1023,5),(8,1024,2),
       (9,1025,4),(9,1026,1),(9,1027,3),
       (10,1028,5),(10,1029,2),(10,1030,4)"

    echo "✅ BigQuery demo tables created and populated."
    echo "   Products: 5 (IDs 1-5, varying inventory: 52, 3, 120, 0, 18)"
    echo "   Users: 10 (IDs 1-10, email: ${DEMO_EMAIL})"
    echo "   Purchases: 30 (3 orders per user)"
  fi
fi
echo ""

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

# 3c. Launch the ADK agent job (Google ADK, no GPU)
ADK_JOB_NAME="${PREFIX}-sentiment-adk-${TIMESTAMP}"
echo "Launching ADK job: ${ADK_JOB_NAME}"
launch_job "${ADK_JOB_NAME}" "adk" ""
echo ""
# 4. Ask the user to input messages which are relayed to the pubsub topic
echo ""
echo "==========================================================="
echo "✅ All three pipelines launched and reading from the same topic!"
echo "  Gemma job:  ${GEMMA_JOB_NAME}"
echo "  Gemini job: ${GEMINI_JOB_NAME}"
echo "  ADK job:    ${ADK_JOB_NAME}"
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
for JOB_NAME in "$GEMMA_JOB_NAME" "$GEMINI_JOB_NAME" "$ADK_JOB_NAME"; do
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
