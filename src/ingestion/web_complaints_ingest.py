import os
import pandas as pd
import boto3
from datetime import datetime
from sqlalchemy import create_engine
from s3_ingestion import write_dataframe_to_s3  # your existing s3_ingestion.py

# -----------------------------
# AWS / S3 Config
# -----------------------------
S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecom-raw-data")
RAW_FOLDER = "raw/website_complaints/"

# -----------------------------
# Postgres Daily Tables to Ingest
# -----------------------------
DAILY_TABLES = [
    "web_form_request_2025_11_20",
    "web_form_request_2025_11_21",
    "web_form_request_2025_11_22",
    "web_form_request_2025_11_23",
]

# -----------------------------
# Fetch Postgres Credentials from SSM
# -----------------------------
def get_postgres_credentials():
    """Fetch Postgres credentials from AWS SSM Parameter Store"""
    ssm = boto3.client("ssm", region_name="eu-north-1")  # Stockholm region

    creds = {}
    params = ["db_host", "db_port", "db_username", "db_password", "db_name", "table_schema_name"]
    for param in params:
        response = ssm.get_parameter(
            Name=f"/coretelecomms/database/{param}",
            WithDecryption=True
        )
        creds[param] = response["Parameter"]["Value"]

    # Convert port to integer
    creds["db_port"] = int(creds["db_port"])
    return creds

# -----------------------------
# Ingest Website Complaints
# -----------------------------
def ingest_website_complaints():
    print("Extracting Website Complaint Forms from Postgres...")

    creds = get_postgres_credentials()

    # Use SQLAlchemy engine instead of raw psycopg2 connection
    engine = create_engine(
    f"postgresql+psycopg2://{creds['db_username']}:{creds['db_password']}"
    f"@{creds['db_host']}:{creds['db_port']}/{creds['db_name']}?sslmode=require&connect_timeout=10"
)

    schema = creds["table_schema_name"]

    for table in DAILY_TABLES:
        print(f"Processing table: {table}")

        # Read table into Pandas via SQLAlchemy engine (warning-free)
        df = pd.read_sql(f"SELECT * FROM {schema}.{table}", engine)

        # Add metadata columns
        df["source_file"] = table
        df["ingestion_timestamp"] = datetime.now()

        # Parquet filename same as table
        parquet_name = f"{table}.parquet"

        # Write to S3 RAW layer
        write_dataframe_to_s3(
            df=df,
            source="website_complaints",
            filename=parquet_name
        )

    print("All website complaint tables ingested successfully!")

# -----------------------------
if __name__ == "__main__":
    ingest_website_complaints()
