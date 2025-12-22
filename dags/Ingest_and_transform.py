
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
from datetime import timedelta
import sys

# Add src folder so Airflow can import ingestion functions
sys.path.append("/opt/airflow/src")

from ingestion.customers_ingest import ingest_customers
from ingestion.call_center_ingest import ingest_call_center_logs
from ingestion.social_media_ingest import ingest_social_media
from ingestion.web_complaints_ingest import ingest_website_complaints
from ingestion.agents_ingest import ingest_agents


# Airflow connection IDs
SOURCE_READ_CONN = "aws_default"
RAW_WRITE_CONN = "aws_personal"


default_args = {
    "owner": "coretelecoms",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["olasunkanmiabidemia@gmail.com"],
}


# -------------------------------------------------------------------
# AWS helper functions (explicit & safe)
# -------------------------------------------------------------------
def get_s3_read_client():
    """Standard boto3 S3 client for source reads."""
    return S3Hook(aws_conn_id=SOURCE_READ_CONN).get_conn()


def get_s3_write_client():
    """Standard boto3 S3 client for raw writes."""
    return S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()


def get_s3_read_session():
    """
    Returns boto3 Session.
    Used ONLY for website complaints ingestion.
    """
    return S3Hook(aws_conn_id=SOURCE_READ_CONN).get_session()


# -------------------------------------------------------------------
# Ingestion task callables (no logic changes)
# -------------------------------------------------------------------
def run_customers_ingestion():
    ingest_customers(
        get_s3_read_client(),
        get_s3_write_client()
    )


def run_call_center_ingestion():
    ingest_call_center_logs(
        get_s3_read_client(),
        get_s3_write_client()
    )


def run_social_media_ingestion():
    ingest_social_media(
        get_s3_read_client(),
        get_s3_write_client()
    )


def run_web_complaints_ingestion():
    """
    Intentionally uses boto3 Session.
    This is isolated to avoid breaking other ingestion contracts.
    """
    ingest_website_complaints(
        get_s3_read_session(),
        get_s3_write_client()
    )


def run_agents_ingestion():
    ingest_agents(
        get_s3_write_client()
    )


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
with DAG(
    dag_id="coretelecoms_ingest_source_data",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["coretelecoms", "ingestion", "dbt"],
) as dag:

    # -------------------------
    # Ingestion Layer (Parallel)
    # -------------------------
    with TaskGroup(
        group_id="source_ingestion",
        tooltip="Parallel ingestion of source datasets"
    ) as ingest_raw_data:

        customers_ingestion = PythonOperator(
            task_id="customers",
            python_callable=run_customers_ingestion,
        )

        call_center_logs_ingestion = PythonOperator(
            task_id="call_center_logs",
            python_callable=run_call_center_ingestion,
        )

        social_media_ingestion = PythonOperator(
            task_id="social_media",
            python_callable=run_social_media_ingestion,
        )

        web_complaints_ingestion = PythonOperator(
            task_id="web_complaints",
            python_callable=run_web_complaints_ingestion,
        )

        agents_ingestion = PythonOperator(
            task_id="agents",
            python_callable=run_agents_ingestion,
        )

    # -------------------------
    # dbt Transformation Layer
    # -------------------------
    run_dbt_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command="cd /opt/airflow/capstone && dbt run --models curated",
    )

    run_dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/airflow/capstone && dbt run --models gold",
    )

    run_dbt_tests = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/capstone && dbt test",
    )

    # -------------------------
    # Dependencies
    # -------------------------
    ingest_raw_data >> run_dbt_curated >> run_dbt_gold >> run_dbt_tests
