import pandas as pd
from datetime import datetime, timezone
import logging

from .s3_ingestion import write_dataframe_to_s3

S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "customers/" 


def ingest_customers(s3_client_read, s3_client_write):
    """
    Ingest customer CSVs from S3 into RAW S3.
    
    - Uses Airflow-injected S3 clients.
    - No boto3 instantiation here (Airflow manages auth).
    - Supports idempotent + incremental future upgrades.
    """

    if s3_client_read is None or s3_client_write is None:
        raise ValueError("Both s3_client_read and s3_client_write must be provided.")

    try:
        logging.info("Starting Customers ingestion from S3...")

        
        logging.info(f"Listing customer files in bucket '{S3_SOURCE_BUCKET}', prefix '{S3_SOURCE_PREFIX}'...")
        response = s3_client_read.list_objects_v2(
            Bucket=S3_SOURCE_BUCKET,
            Prefix=S3_SOURCE_PREFIX
        )

        if "Contents" not in response:
            logging.warning("No customer files found in source bucket.")
            return

        
        for obj in response["Contents"]:
            key = obj["Key"]

            # skip folder markers
            if key.endswith("/"):
                continue

            logging.info(f"Processing customer file: {key}")

            try:
                
                file_obj = s3_client_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_csv(file_obj["Body"])

                if df.empty:
                    logging.warning(f"Customer file {key} is empty. Skipping.")
                    continue

                
                df["source_file"] = key
                df["ingestion_timestamp"] = datetime.now(timezone.utc)

                print(df.head())

                logging.info(f"Read {len(df)} rows from {key}")

                
                parquet_name = key.split("/")[-1].replace(".csv", ".parquet")

                
                logging.info(f"Writing {parquet_name} to RAW S3...")
                write_dataframe_to_s3(
                    df=df,
                    source="customers",
                    filename=parquet_name,
                    s3_client_write=s3_client_write
                )

                logging.info(f"Successfully ingested {key}")

            except Exception as file_error:
                logging.error(f"Error processing {key}: {file_error}", exc_info=True)
                continue  

        logging.info("All customer files ingested successfully!")

    except Exception as e:
        logging.error(f"Customer ingestion failed: {e}", exc_info=True)
        raise  



