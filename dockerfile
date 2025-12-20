FROM apache/airflow:3.1.0-python3.10

# Switch to root to install system deps
USER root

# Install OS-level dependencies (lightweight & safe)
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements first (Docker layer caching)
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code into image
COPY dags /opt/airflow/dags
COPY src /opt/airflow/src
COPY dbt /opt/airflow/dbt
COPY snowflake /opt/airflow/snowflake
COPY terraform-infra /opt/airflow/terraform-infra

# Ensure permissions
RUN chmod -R 755 /opt/airflow

# Default Airflow entrypoint is already defined in base image

