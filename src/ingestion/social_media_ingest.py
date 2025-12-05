import pandas as pd
from datetime import datetime, timezone
import logging
import boto3

from .s3_ingestion import write_dataframe_to_s3


S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "social_medias/"  


def ingest_social_media(s3_client_read=None, s3_client_write=None):
    """
    Ingest social media JSON complaints from S3 into the RAW S3 layer.

    Parameters:
    - s3_client_read: Optional boto3 client for reading from S3.
    - s3_client_write: Required boto3 client for writing to RAW S3.
    """

    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided for writing to RAW S3")

    s3_read = s3_client_read or boto3.client("s3")

    try:
        logging.info("Starting Social Media Complaints ingestion from S3...")

        logging.info(
            f"Listing social media files in bucket '{S3_SOURCE_BUCKET}', prefix '{S3_SOURCE_PREFIX}'..."
        )

        response = s3_read.list_objects_v2(
            Bucket=S3_SOURCE_BUCKET,
            Prefix=S3_SOURCE_PREFIX
        )

        if "Contents" not in response:
            logging.warning("No social media JSON files found.")
            return

        for obj in response["Contents"]:
            key = obj["Key"]

            if key.endswith("/"):
                continue  

            logging.info(f"Processing social media file: {key}")

            try:
                file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_json(file_obj["Body"])

                if df.empty:
                    logging.warning(f"File {key} is empty. Skipping.")
                    continue

                logging.info(f"Read {len(df)} records from {key}")

                df["source_file"] = key.split("/")[-1]
                df["ingestion_timestamp"] = datetime.now(timezone.utc)

                print(df.head())

                parquet_filename = df["source_file"].iloc[0].replace(".json", ".parquet")

                logging.info(f"Writing {parquet_filename} to RAW S3...")

                write_dataframe_to_s3(
                    df=df,
                    source="social_media",
                    filename=parquet_filename,
                    s3_client_write=s3_client_write
                )

                logging.info(f"Successfully ingested {key}")

            except Exception as file_error:
                logging.error(
                    f"Error processing social media file {key}: {file_error}",
                    exc_info=True
                )
                continue  

        logging.info("All social media JSON files ingested successfully!")

    except Exception as e:
        logging.error(f"Social Media ingestion failed: {e}", exc_info=True)
        raise  

