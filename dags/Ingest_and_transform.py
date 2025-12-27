from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
from datetime import timedelta
from airflow.sdk.bases.hook import BaseHook
import os
from ingestion.customers_ingest import ingest_customers
from ingestion.call_center_ingest import ingest_call_center_logs
from ingestion.social_media_ingest import ingest_social_media
from ingestion.web_complaints_ingest import ingest_website_complaints
from ingestion.agents_ingest import ingest_agents

# Airflow connection IDs
SOURCE_READ_CONN = "aws_default"
RAW_WRITE_CONN = "aws_personal"


def get_snowflake_dbt_env():

    try:
        conn = BaseHook.get_connection('snowflake_conn')
        extras = conn.extra_dejson
        
        print(f" Connection 'snowflake_conn' successfully retrieved from Airflow metadata.")
        print(f" Target Snowflake Account: {extras.get('account')}")
        print(f" Preparing to run dbt models in Database: {extras.get('database')}")

        return {
            "DBT_SNOWFLAKE_ACCOUNT": str(extras.get('account') or ""),
            "DBT_SNOWFLAKE_USER": str(conn.login or ""),
            "DBT_SNOWFLAKE_PW": str(conn.password or ""),
            "DBT_SNOWFLAKE_ROLE": str(extras.get('role') or ""),
            "DBT_SNOWFLAKE_DB": str(extras.get('database') or ""),
            "DBT_SNOWFLAKE_WH": str(extras.get('warehouse') or ""),
            "DBT_SNOWFLAKE_SCHEMA": str(conn.schema or "PUBLIC"),
        }
    except Exception as e:
        print(f" ERROR: Failed to fetch Snowflake connection: {e}")
        return {}
    

def slack_fail_alert(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    log_url = ti.log_url

    error_msg = context.get('exception')
    clean_error = str(error_msg)[:200] if error_msg else "No specific error log found."
    slack_msg_text = f""":red_circle: *Airflow Task Failure*
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Error*: {clean_error}
    *Logs*: <{log_url}|Click here to debug>
    """
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id='slack_conn')
        return hook.send(text=slack_msg_text)       
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

def slack_success_alert(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    
    slack_msg_text = f""":white_check_mark: *Pipeline Success*
    *DAG*: {dag_id}
    *Status*: The dbt Gold Layer is ready for dashboard analysis.
    """  
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id='slack_conn')
        return hook.send(text=slack_msg_text)
    except Exception as e:
        print(f"Failed to send Slack success alert: {e}")


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

# Ingestion task callables

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


default_args = {
    "owner": "coretelecoms",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': slack_fail_alert
}
# DAG definition

with DAG(
    dag_id="coretelecoms_unified_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    tags=["coretelecoms", "ELT",  "s3", "snowflake", "dbt"],
) as dag:

    
    # Ingestion Layer (Parallel from multiple sources into S3 Raw Bucket unified structure and parquet format)
  
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

#  load Raw Data using the Stored Procedure you created
    load_raw_data = SQLExecuteQueryOperator(
        task_id='load_s3_raw_data_to_snowflake',
        conn_id='snowflake_conn',
        sql="CALL RAW.INGEST_TELECOMS_RAW_DATA();",
        autocommit=True,
        parameters=None
    )
    # dbt Transformation Layer
    run_dbt_curated = BashOperator(
        task_id="dbt_run_curated",
        bash_command=(
            "cd /opt/airflow/dbt/telecoms_project && "
            "dbt debug && "
            "dbt run --select curated"
        ),
        env=get_snowflake_dbt_env(),
        append_env=True, 
    )

    test_dbt_curated = BashOperator(
        task_id="test_dbt_curated",
        bash_command="cd /opt/airflow/dbt/telecoms_project && dbt test --select curated",
        env=get_snowflake_dbt_env(),
        append_env=True,
    )

    run_dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/airflow/dbt/telecoms_project && dbt run --select gold",
        env=get_snowflake_dbt_env(),
        append_env=True,
    )


    test_dbt_gold = BashOperator(
        task_id="test_dbt_gold",
        bash_command="cd /opt/airflow/dbt/telecoms_project && dbt test --select gold",
        env=get_snowflake_dbt_env(),
        append_env=True,
    )
    
    send_success_email = EmailOperator(
        task_id="send_success_email",
        to=["abidemi4372@gmail.com",  "olasunkanmiabidemia@gmail.com", "folashadeolasunkanmi449@gmail.com"],
        subject="Coretelecoms Unified Pipeline Completed Successfully",
        html_content="""
        <h3> Unified Pipeline Update</h3>
        <p>Hi Team,</p>
        <p>The Airflow pipeline ran successfully</p>
        <p>The dbt Gold Layer is ready for dashboard analysis.</p>
        <p>Kind Regards,</p>
        <p>Afeez Olasunkanmi</p>
        """,
        conn_id="smtp_conn",
        on_success_callback=slack_success_alert
   )

   
    ingest_raw_data >> load_raw_data >> run_dbt_curated >> test_dbt_curated >> run_dbt_gold >> test_dbt_gold >> send_success_email
