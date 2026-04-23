# Use an NVIDIA CUDA 12.1 devel image - required by vLLM/triton which need CUDA 12.1+
# The "devel" variant includes libcuda stubs needed for triton's JIT kernel compilation
FROM nvidia/cuda:12.1.0-cudnn8-devel-ubuntu22.04

# The Python version of the Dockerfile must match the Python version you use
ARG PYTHON_VERSION=3.11

# Avoid interactive timezone prompts during apt-get installations
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Install wget, software-properties-common, and add deadsnakes PPA to ensure python3.11 is available on ubuntu22.04
RUN apt-get update \
    && apt-get install -y wget software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-dev python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-distutils python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 10 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 10 \
    && wget https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY requirements.txt .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"
ARG HF_TOKEN

# Upgrade pip and install the requirements
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# 3. Pre-download the model weights
# We exclude .gguf to save 10GB of unnecessary space
RUN HF_HUB_ENABLE_HF_TRANSFER=1 huggingface-cli download \
    google/gemma-2b-it \
    --exclude "*.gguf" \
    --token $HF_TOKEN

COPY pipeline.py .

# Copy the Apache Beam worker dependencies from the Beam Python 3.11 SDK image
# This sets up the worker entrypoint at /opt/apache/beam/boot
COPY --from=apache/beam_python3.11_sdk:2.72.0 /opt/apache/beam /opt/apache/beam

# Copy the Flex Template launcher from the Dataflow launcher base image.
# This is used when the image is run as a Flex Template launcher.
# The entrypoint below (/opt/apache/beam/boot) is what Dataflow workers use;
# the flex-template spec will override the entrypoint with this launcher binary.
COPY --from=gcr.io/dataflow-templates-base/python311-template-launcher-base /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Default entrypoint for SDK/worker containers
ENTRYPOINT ["/opt/apache/beam/boot"]
