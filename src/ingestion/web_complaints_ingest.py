# import os
# import pandas as pd
# import boto3
# from datetime import datetime
# from sqlalchemy import create_engine
# from s3_ingestion import write_dataframe_to_s3  # your existing s3_ingestion.py

# # -----------------------------
# # AWS / S3 Config
# # -----------------------------
# S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecom-raw-data")
# RAW_FOLDER = "raw/website_complaints/"

# # -----------------------------
# # Postgres Daily Tables to Ingest
# # -----------------------------
# DAILY_TABLES = [
#     "web_form_request_2025_11_20",
#     "web_form_request_2025_11_21",
#     "web_form_request_2025_11_22",
#     "web_form_request_2025_11_23",
# ]

# # -----------------------------
# # Fetch Postgres Credentials from SSM
# # -----------------------------
# def get_postgres_credentials():
#     """Fetch Postgres credentials from AWS SSM Parameter Store"""
#     ssm = boto3.client("ssm", region_name="eu-north-1")  # Stockholm region

#     creds = {}
#     params = ["db_host", "db_port", "db_username", "db_password", "db_name", "table_schema_name"]
#     for param in params:
#         response = ssm.get_parameter(
#             Name=f"/coretelecomms/database/{param}",
#             WithDecryption=True
#         )
#         creds[param] = response["Parameter"]["Value"]

#     # Convert port to integer
#     creds["db_port"] = int(creds["db_port"])
#     return creds

# # -----------------------------
# # Ingest Website Complaints
# # -----------------------------
# def ingest_website_complaints():
#     print("Extracting Website Complaint Forms from Postgres...")

#     creds = get_postgres_credentials()

#     # Use SQLAlchemy engine instead of raw psycopg2 connection
#     engine = create_engine(
#     f"postgresql+psycopg2://{creds['db_username']}:{creds['db_password']}"
#     f"@{creds['db_host']}:{creds['db_port']}/{creds['db_name']}?sslmode=require&connect_timeout=10"
# )

#     schema = creds["table_schema_name"]

#     for table in DAILY_TABLES:
#         print(f"Processing table: {table}")

#         # Read table into Pandas via SQLAlchemy engine (warning-free)
#         df = pd.read_sql(f"SELECT * FROM {schema}.{table}", engine)

#         # Add metadata columns
#         df["source_file"] = table
#         df["ingestion_timestamp"] = datetime.now()

#         print(df.head())

#         # Parquet filename same as table
#         parquet_name = f"{table}.parquet"

#         # Write to S3 RAW layer
#         write_dataframe_to_s3(
#             df=df,
#             source="website_complaints",
#             filename=parquet_name
#         )

#     print("All website complaint tables ingested successfully!")

# # -----------------------------
# if __name__ == "__main__":
#     ingest_website_complaints()



import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import logging

from .s3_ingestion import write_dataframe_to_s3


def get_postgres_credentials(ssm_client):
    """Fetch Postgres credentials from SSM Parameter Store."""
    params = [
        "db_host", "db_port", "db_username",
        "db_password", "db_name", "table_schema_name"
    ]

    creds = {
        p: ssm_client.get_parameter(
            Name=f"/coretelecomms/database/{p}",
            WithDecryption=True
        )["Parameter"]["Value"]
        for p in params
    }

    creds["db_port"] = int(creds["db_port"])
    return creds


def discover_web_form_tables(engine, schema):
    """Automatically discover tables matching web_form_request_* pattern."""
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_name LIKE 'web_form_request_%'
        ORDER BY table_name;
    """)

    logging.info(f"Executing table discovery query for schema '{schema}'")

    df = pd.read_sql(query, engine, params={"schema": schema})

    if df.empty:
        logging.info("No tables found matching pattern")
        return []

    tables = df["table_name"].tolist()
    logging.info(f"Discovered tables: {tables}")
    return tables

    

def ingest_website_complaints(s3_client_read, s3_client_write):
    """
    s3_client_read  → boto3.Session (for SSM + read services)
    s3_client_write → boto3 S3 client (for writing to RAW bucket)
    """
    
    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided")

    if s3_client_read is None:
        raise ValueError("s3_client_read (session) must be provided")

    # s3_client_read is a boto3.Session from AwsBaseHook
    ssm_client = s3_client_read.client("ssm")

    logging.info("Fetching Postgres credentials from SSM...")
    creds = get_postgres_credentials(ssm_client)

    schema = creds["table_schema_name"]

    # Build SQLAlchemy connection
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=creds["db_username"],
        password=creds["db_password"],
        host=creds["db_host"],
        port=creds["db_port"],
        database=creds["db_name"],
        query={"sslmode": "require", "connect_timeout": "10"}
    )

    engine = create_engine(url)

    logging.info("Discovering web_form_request form tables...")
    tables = discover_web_form_tables(engine, schema)

    if not tables:
        logging.warning("No web_form_request_* tables found.")
        return

    # Process each table
    for table in tables:
        logging.info(f"Processing table: {table}")

        try:
            df = pd.read_sql(f"SELECT * FROM {schema}.{table}", engine)

            df["source_table"] = table
            df["ingestion_timestamp"] = datetime.now(timezone.utc)
            
            print(df.head())

            parquet_filename = f"{table}.parquet"

            write_dataframe_to_s3(
                df=df,
                source="website_complaints",
                filename=parquet_filename,
                s3_client_write=s3_client_write,
            )

        except Exception as e:
            logging.error(f"Failed processing table {table}: {e}")

    logging.info("Website complaint ingestion completed.")
