import pandas as pd
from datetime import datetime, timezone
import logging

from .s3_ingestion import write_dataframe_to_s3

S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "customers/"   # folder containing customer CSVs


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

        # 1. LIST source customer files
        logging.info(f"Listing customer files in bucket '{S3_SOURCE_BUCKET}', prefix '{S3_SOURCE_PREFIX}'...")
        response = s3_client_read.list_objects_v2(
            Bucket=S3_SOURCE_BUCKET,
            Prefix=S3_SOURCE_PREFIX
        )

        if "Contents" not in response:
            logging.warning("No customer files found in source bucket.")
            return

        # 2. Loop through all source files
        for obj in response["Contents"]:
            key = obj["Key"]

            # skip folder markers
            if key.endswith("/"):
                continue

            logging.info(f"Processing customer file: {key}")

            try:
                # Read file from S3
                file_obj = s3_client_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_csv(file_obj["Body"])

                if df.empty:
                    logging.warning(f"Customer file {key} is empty. Skipping.")
                    continue

                # Add metadata
                df["source_file"] = key
                df["ingestion_timestamp"] = datetime.now(timezone.utc)

                print(df.head())

                logging.info(f"Read {len(df)} rows from {key}")

                # Build output filename
                parquet_name = key.split("/")[-1].replace(".csv", ".parquet")

                # Write parquet to RAW zone
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
                continue  # move to next file; do not stop entire ingestion

        logging.info("All customer files ingested successfully!")

    except Exception as e:
        logging.error(f"Customer ingestion failed: {e}", exc_info=True)
        raise  # allow Airflow to mark the task as FAILED




# import pandas as pd
# import boto3
# from datetime import datetime
# from .s3_ingestion import write_dataframe_to_s3

# S3_SOURCE_BUCKET = "core-telecoms-data-lake"
# S3_SOURCE_PREFIX = "customers/"   # folder containing customer CSVs

# def ingest_customers():
#     print(" Extracting Customers Data from S3...")

#     s3_client = boto3.client("s3")

#     # --------------------------------------------
#     # 1. LIST objects in the customers folder
#     # --------------------------------------------
#     response = s3_client.list_objects_v2(
#         Bucket=S3_SOURCE_BUCKET,
#         Prefix=S3_SOURCE_PREFIX
#     )

#     if "Contents" not in response:
#         print(" No customer files found in source bucket.")
#         return

#     for obj in response["Contents"]:
#         key = obj["Key"]

#         # Skip folder markers
#         if key.endswith("/"):
#             continue

#         print(f"Processing file: {key}")

#         # Download file
#         file_obj = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
#         df = pd.read_csv(file_obj["Body"])

#         # Add metadata
#         df["source_file"] = key
#         df["ingestion_timestamp"] = datetime.now()

#         print(df.head())

#         # Generate output file name (same as raw CSV but parquet)
#         parquet_name = key.split("/")[-1].replace(".csv", ".parquet")

#         # Write parquet to the unified RAW layer
#         write_dataframe_to_s3(
#             df=df,
#             source="customers",
#             filename=parquet_name
#         )

#     print(" All customer files ingested successfully!")

# if __name__ == "__main__":
#     ingest_customers()

import pandas as pd
from datetime import datetime, timezone
from .s3_ingestion import write_dataframe_to_s3

S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "customers/"   # folder containing customer CSVs


def ingest_customers(s3_client_read, s3_client_write):
    """
    Airflow-friendly customer ingestion.
    - No boto3 session created inside this file.
    - S3 clients must be passed from the DAG (Airflow S3Hook).
    - Supports idempotent + incremental processing by checking RAW layer.
    """

    print("Extracting Customers Data from S3...")

    # 1. LIST source customer files
    response = s3_client_read.list_objects_v2(
        Bucket=S3_SOURCE_BUCKET,
        Prefix=S3_SOURCE_PREFIX
    )

    if "Contents" not in response:
        print("No customer files found in source bucket.")
        return

    # 2. Loop through all source files
    for obj in response["Contents"]:
        key = obj["Key"]

        if key.endswith("/"):
            continue  # skip S3 folder markers

        print(f"Processing file: {key}")

        # Read file from S3
        file_obj = s3_client_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
        df = pd.read_csv(file_obj["Body"])

        # Add metadata
        df["source_file"] = key
        df["ingestion_timestamp"] = datetime.now(timezone.utc)

        # Build output filename
        parquet_name = key.split("/")[-1].replace(".csv", ".parquet")

        # Write parquet to RAW zone using injected client
        write_dataframe_to_s3(
            df=df,
            source="customers",
            filename=parquet_name,
            s3_client_write=s3_client_write
        )

    print("All customer files ingested successfully!")
