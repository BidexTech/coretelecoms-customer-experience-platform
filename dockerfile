# FROM apache/airflow:3.1.0

# USER root

# RUN apt-get update && apt-get install -y \
#     git \
#     curl \
#     build-essential \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# USER airflow

# WORKDIR /opt/airflow/capstone

# COPY requirements.txt .

# RUN pip install --upgrade pip \
#  && pip install \
#     -r requirements.txt

# COPY . .

# EXPOSE 8080

FROM apache/airflow:3.1.0-python3.10

USER root

# System deps (optional but safe)
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean

USER airflow
# Python deps
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt
RUN pip install --no-cache-dir dbt-snowflake==1.10.3



# Copy Airflow artifacts
COPY dags /opt/airflow/dags
COPY src /opt/airflow/src
COPY dbt /opt/airflow/dbt


