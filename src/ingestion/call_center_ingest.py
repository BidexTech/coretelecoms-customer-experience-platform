import pandas as pd
from datetime import datetime, timezone
import boto3
import logging

from .s3_ingestion import write_dataframe_to_s3

# S3 source configuration
S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "call logs/"  


def ingest_call_center_logs(s3_client_read=None, s3_client_write=None):
    """
    Ingest call center logs from S3, transform, and load into the RAW S3 zone.

    Parameters:
    - s3_client_read (boto3.client): Optional S3 client for reading from the source bucket.
    - s3_client_write (boto3.client): Required S3 client for writing to the RAW zone.
    """

    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided for writing to RAW S3")

    
    s3_read = s3_client_read or boto3.client("s3")

    try:
        logging.info("Starting Call Center Logs ingestion from S3...")

        
        logging.info(f"Listing objects in bucket '{S3_SOURCE_BUCKET}' prefix '{S3_SOURCE_PREFIX}'...")
        response = s3_read.list_objects_v2(
            Bucket=S3_SOURCE_BUCKET,
            Prefix=S3_SOURCE_PREFIX
        )

        if "Contents" not in response:
            logging.warning("No call center log files found in S3 path.")
            return

        for obj in response["Contents"]:
            key = obj["Key"]

            # Skip directories
            if key.endswith("/"):
                continue

            filename = key.split("/")[-1]
            parquet_filename = filename.replace(".csv", ".parquet")

            logging.info(f"Processing file: {filename}")

            try:
                
                file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_csv(file_obj["Body"])

                if df.empty:
                    logging.warning(f"File {filename} is empty. Skipping.")
                    continue

                
                df["source_file"] = filename
                df["ingestion_timestamp"] = datetime.now(timezone.utc)
                
                print(df.head())

                logging.info(f"Read {len(df)} rows from {filename}")

            
                logging.info(f"Writing {parquet_filename} to RAW S3...")
                write_dataframe_to_s3(
                    df=df,
                    source="call_center_logs",
                    filename=parquet_filename,
                    s3_client_write=s3_client_write
                )

                logging.info(f"Successfully ingested {filename}")

            except Exception as e:
                logging.error(f"Error processing {filename}: {e}", exc_info=True)
                continue  

        logging.info("All call center logs ingested successfully!")

    except Exception as e:
        logging.error(f"Call Center Logs ingestion failed: {e}", exc_info=True)
        raise 




