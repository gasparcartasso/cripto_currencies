# Stage 1: Build with uv and Python 3.12
FROM python:3.12-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | bash

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Create virtual environment and sync dependencies
RUN uv venv && \
    .venv/bin/uv pip install apache-airflow==2.11.0 && \
    .venv/bin/uv pip install -r <(.venv/bin/uv pip freeze)

# Stage 2: Runtime image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy project files (DAGs, configs, etc.)
COPY . .

# Set Airflow environment variables
ENV AIRFLOW_HOME=/app
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# Default command
CMD ["airflow", "webserver"]