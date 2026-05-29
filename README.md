# Dataflow GenAI Response Pipeline

This project deploys an Apache Beam (Dataflow) Flex Template pipeline that reads incoming messages from a Pub/Sub topic and runs inference models to generate a synthesized response.

It supports three inference backends:
- **Gemma (Local GPU Inference)**: Uses vLLM to run `google/gemma-2b-it` natively on Dataflow worker instances using NVIDIA GPUs (T4 or L4) via Dataflow Rightfitting.
- **Gemini (Cloud API Inference)**: Uses the Vertex AI Gemini REST API to process requests without requiring local GPUs.
- **ADK (Agent Inference)**: Uses the [Google Agent Development Kit (ADK)](https://google.github.io/adk-docs/) to run a `gemini-2.0-flash`-backed LLM agent equipped with BigQuery lookup tools and a Gmail email-sending tool. The agent looks up customer and order data, decides the best remediation action, emails the customer, and returns a summary string.

## Prerequisites

- **Google Cloud SDK (`gcloud`)** and **`bq` CLI** installed and authenticated.
- **Docker** installed for local testing or custom image modification.
- A **Hugging Face Token** with access to gated models (e.g., Gemma).
- *(ADK path only)* Gmail API enabled in your project and Application Default Credentials that include the `gmail.send` scope (see ADK section below).

### Setup

To set up your project so that it is ready to run:

1. Replace `"your_hf_token_here"` in the Dockerfile with your actual Hugging Face Token.
2. Create an Artifact Registry repository to host your template image.
3. Update the `run.sh` script with your project's unique configuration

## Building the Flex Templates

The worker environment requires a custom Docker image that ships the CUDA drivers and Dataflow dependencies. This project uses Cloud Build to construct the images and push them to your Artifact Registry.

## Deployment

The `run.sh` script automates the full deployment flow:
1. **Set up the Pub/Sub topic:** Creates the necessary topic to stream the raw data in.
2. **Build and stage the template:** Submits the Docker build process to Cloud Build (using `gcloud builds submit`), and stages the `metadata.json` Flex Template spec to Google Cloud Storage.
3. **Launch the jobs:** Uses the Dataflow REST API to trigger parallel pipeline jobs using all three inference engines. The GPU pipeline will dynamically ask Dataflow to provision GPU workers at runtime using Rightfitting.

To run the script:
```bash
# Make sure you are authenticated with application default credentials so the REST API calls succeed
gcloud auth application-default login

# Execute the run script
./run.sh
```

## Architecture Notes
- The Dataflow pipeline relies exclusively on the **Flex Templates** feature.
- **Apache Beam 2.73.0** — required for `ADKAgentModelHandler` support in `apache_beam.ml.inference.agent_development_kit`.
- **Rightfitting:** The Gemma execution graph requests a specific hardware accelerator via `with_resource_hints(accelerator="type:nvidia-tesla-t4;...")`. Dataflow will automatically allocate workers with GPUs for this step independently of the main pipeline. Ensure that GCP Compute quotas are sufficient in your selected region.
- **ADK Agent:** The `adk` backend constructs a `google.adk.agents.LlmAgent` backed by `gemini-2.0-flash` and wraps it with `ADKAgentModelHandler`. Each element is passed as a fresh user turn (stateless by default).

## ADK Agent Details

### Message Format
The ADK path requires Pub/Sub messages to be formatted as:
```
<user_id>: <message text>
```
Example: `3: I received a broken item and I want a refund`

Messages that cannot be parsed (missing or non-numeric user ID) are logged as warnings and dropped.

### Tools
The agent has access to three tools:

| Tool | Description | Data source |
|------|-------------|-------------|
| `lookup_user(user_id)` | Returns the customer's email address | BigQuery `sentiment_demo.users` |
| `lookup_orders(user_id)` | Returns order history with product inventory and price | BigQuery `sentiment_demo.purchases` + `sentiment_demo.products` |
| `send_email(to, subject, body)` | Sends a plain-text email to the customer | Gmail API |

### BigQuery Tables
The `run.sh` script creates and seeds the following tables in the `sentiment_demo` dataset:
- **`users`**: `user_id` (INT64), `user_email` (STRING) — 10 rows (user IDs 1–10)
- **`products`**: `product_id` (INT64), `remaining_inventory` (INT64), `price` (FLOAT64) — 5 products
- **`purchases`**: `user_id` (INT64), `order_id` (INT64), `product_id` (INT64) — 30 rows (3 per user)

### Gmail API Credentials
The `send_email` tool uses Application Default Credentials with the `gmail.send` scope. When running locally:
```bash
gcloud auth application-default login \
  --scopes=openid,\
https://www.googleapis.com/auth/cloud-platform,\
https://www.googleapis.com/auth/gmail.send
```
For Dataflow workers, store user-delegated credentials in Secret Manager and load them at runtime, or configure a service account with domain-wide delegation.
