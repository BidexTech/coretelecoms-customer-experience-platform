FROM apache/airflow:3.1.0-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
#  Update PATH so the terminal can find dbt and python code in /opt/airflow/src
ENV PATH="${PATH}:/home/airflow/.local/bin"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"

COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
   

WORKDIR /opt/airflow

COPY --chown=airflow:0 dags /opt/airflow/dags
COPY --chown=airflow:0 src /opt/airflow/src
COPY --chown=airflow:0 dbt /opt/airflow/dbt

USER airflow
