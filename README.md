# Dataflow GenAI Response Pipeline

This project deploys an Apache Beam (Dataflow) Flex Template pipeline that reads incoming messages from a Pub/Sub topic and runs inference models to generate a synthesized response.

It supports two inference backends:
- **Gemma (Local GPU Inference)**: Uses vLLM to run `google/gemma-2b-it` natively on Dataflow worker instances using NVIDIA GPUs (T4 or L4) via Dataflow Rightfitting.
- **Gemini (Cloud API Inference)**: Uses the Vertex AI Gemini REST API to process requests without requiring local GPUs.

## Prerequisites

- **Google Cloud SDK (`gcloud`)** installed and authenticated.
- **Docker** installed for local testing or custom image modification.
- A **Hugging Face Token** with access to gated models (e.g., Gemma).

## Building the Flex Templates

The worker environment requires a custom Docker image that ships the CUDA drivers and Dataflow dependencies. This project uses Cloud Build to construct the images and push them to your Artifact Registry.

Because `gemma-2b-it` requires authorization, you must provide your Hugging Face Token as a build argument so that the layer cache can bake the weights into your Docker image. This dramatically improves worker startup times.

To build manually:
```bash
export HF_TOKEN="your_hf_token_here"
docker build --build-arg HF_TOKEN=${HF_TOKEN} -t your-registry/your-image:latest .
docker push your-registry/your-image:latest
```

## Deployment

The `run.sh` script automates the full deployment flow:
1. **Set up the Pub/Sub topic:** Creates the necessary topic to stream the raw data in.
2. **Build and stage the template:** Submits the Docker build process to Cloud Build (using `gcloud builds submit`), and stages the `metadata.json` Flex Template spec to Google Cloud Storage.
3. **Launch the jobs:** Uses the Dataflow REST API to trigger parallel pipeline jobs using both inference engines. The GPU pipeline will dynamically ask Dataflow to provision GPU workers at runtime using Rightfitting.

To run the script:
```bash
# Make sure you are authenticated with application default credentials so the REST API calls succeed
gcloud auth application-default login

# Execute the run script
./run.sh
```

## Architecture Notes
- The Dataflow pipeline relies exclusively on the **Flex Templates** feature.
- **Rightfitting:** The Gemma execution graph requests a specific hardware accelerator via `with_resource_hints(accelerator="type:nvidia-tesla-t4;...")`. Dataflow will automatically allocate workers with GPUs for this step independently of the main pipeline. Ensure that GCP Compute quotas are sufficient in your selected region.
