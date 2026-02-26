# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# (Optional but common) tools you may need during deploy/pull
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install Prefect (CLI + SDK)
RUN pip install --no-cache-dir -U prefect

# Copy your project (prefect.yaml, flow code, etc.)
COPY . /app

# Default: show Prefect version (override in `docker run ...`)
CMD ["prefect", "version"]