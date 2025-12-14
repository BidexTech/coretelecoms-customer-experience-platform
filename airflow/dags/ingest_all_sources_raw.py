from airflow.sdk import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
from datetime import timedelta
import sys
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
# Add your src folder so Airflow can import ingestion functions
sys.path.append("/opt/airflow/capstone/src")

from ingestion.customers_ingest import ingest_customers
from ingestion.call_center_ingest import ingest_call_center_logs
from ingestion.social_media_ingest import ingest_social_media
from ingestion.web_complaints_ingest import ingest_website_complaints
from ingestion.agents_ingest import ingest_agents


# Airflow connection IDs
SOURCE_READ_CONN = "aws_default"
RAW_WRITE_CONN = "aws_personal"

default_args = {
    'owner': 'coretelecoms',
    'depends_on_past': False,
    'retries': 3,  
    'retry_delay': timedelta(minutes=5), 
    'email_on_failure': True,  
    'email_on_retry': False,
    'email': ['olasunkanmiabidemia@gmail.com'],
}  


with DAG(
    dag_id='coretelecoms_ingest_source_data', 
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=['coretelecoms', 'ingestion'],
):

    
    def run_customers_ingestion():
        # Acquire S3 clients from Airflow connections
        s3_client_read = S3Hook(aws_conn_id=SOURCE_READ_CONN).get_conn()
        s3_client_write = S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()

        # Call your ingestion function with these clients
        ingest_customers(s3_client_read, s3_client_write)

    def run_call_center_ingestion():
        s3_client_read = S3Hook(aws_conn_id=SOURCE_READ_CONN).get_conn()
        s3_client_write = S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()
        ingest_call_center_logs(s3_client_read, s3_client_write)

    def run_social_media_ingestion():
        s3_client_read = S3Hook(aws_conn_id=SOURCE_READ_CONN).get_conn()
        s3_client_write = S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()
        ingest_social_media(s3_client_read, s3_client_write)

    def run_web_complaints_ingestion():
        s3_client_read = S3Hook(aws_conn_id=SOURCE_READ_CONN).get_session()  # use get_session() not get_conn()
        s3_client_write = S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()
        ingest_website_complaints(s3_client_read, s3_client_write)
    
    def run_agents_ingestion():
        s3_client_write = S3Hook(aws_conn_id=RAW_WRITE_CONN).get_conn()
        ingest_agents(s3_client_write)
        
    customers_ingestion = PythonOperator(
        task_id="customers_ingestion",
        python_callable=run_customers_ingestion,
    )

    call_center_logs_ingestion = PythonOperator(
        task_id="call_center_logs_ingestion",
        python_callable=run_call_center_ingestion,
    )

    social_media_ingestion = PythonOperator(
        task_id="social_media_ingestion",
        python_callable=run_social_media_ingestion,
    )

    web_complaints_ingestion = PythonOperator(
        task_id="web_complaints_ingestion",
        python_callable=run_web_complaints_ingestion,
    )

    agents_ingestion = PythonOperator( 
        task_id="agents_ingestion", 
        python_callable=run_agents_ingestion
    )
    



run_dbt_curated = BashOperator(
    task_id="dbt_run_curated",
    bash_command="cd /opt/airflow/capstone && dbt run --models curated"
)

run_dbt_gold = BashOperator(
    task_id="dbt_run_gold",
    bash_command="cd /opt/airflow/capstone && dbt run --models gold"
)

run_dbt_tests = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/capstone && dbt test"
)

    agents_ingestion >> social_media_ingestion >> call_center_logs_ingestion >> web_complaints_ingestion >>  customers_ingestion

