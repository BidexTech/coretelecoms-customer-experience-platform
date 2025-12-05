import pandas as pd
from datetime import datetime, timezone
import boto3
import logging

from .s3_ingestion import write_dataframe_to_s3

# S3 source configuration
S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "call logs/"  # folder containing call center CSVs


def ingest_call_center_logs(s3_client_read=None, s3_client_write=None):
    """
    Ingest call center logs from S3, transform, and load into the RAW S3 zone.

    Parameters:
    - s3_client_read (boto3.client): Optional S3 client for reading from the source bucket.
    - s3_client_write (boto3.client): Required S3 client for writing to the RAW zone.
    """

    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided for writing to RAW S3")

    # Use provided read client or default boto3 client
    s3_read = s3_client_read or boto3.client("s3")

    try:
        logging.info("Starting Call Center Logs ingestion from S3...")

        # List objects in the specified S3 prefix
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
                # Read CSV into DataFrame
                file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_csv(file_obj["Body"])

                if df.empty:
                    logging.warning(f"File {filename} is empty. Skipping.")
                    continue

                # Add metadata columns
                df["source_file"] = filename
                df["ingestion_timestamp"] = datetime.now(timezone.utc)
                
                print(df.head())

                logging.info(f"Read {len(df)} rows from {filename}")

                # Write DataFrame to RAW layer
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
                continue  # move to next file but still log errors

        logging.info("All call center logs ingested successfully!")

    except Exception as e:
        logging.error(f"Call Center Logs ingestion failed: {e}", exc_info=True)
        raise  # allow Airflow to mark task as FAILED





# import pandas as pd
# import boto3
# from io import StringIO
# from datetime import datetime, timezone
# from s3_ingestion import write_dataframe_to_s3

# # -----------------------------
# S3_SOURCE_BUCKET = "core-telecoms-data-lake"
# S3_SOURCE_PREFIX = "call logs/"   # Folder in S3


# def ingest_call_center_logs():
#     print(" Extracting Call Center Logs from S3...")

#     s3_client = boto3.client("s3")

#     # --------------------------------------------
#     # 1. LIST all CSV files in the folder
#     # --------------------------------------------
#     response = s3_client.list_objects_v2(
#         Bucket=S3_SOURCE_BUCKET,
#         Prefix=S3_SOURCE_PREFIX
#     )
#     if "Contents" not in response:
#         print(" No call center log files found")
#         return

#     # --------------------------------------------
#     # 2. Process each CSV file individually
#     # --------------------------------------------
#     for obj in response["Contents"]:
#         key = obj["Key"]

#         # Skip folder markers
#         if key.endswith("/"):
#             continue

#         # Extract only the filename (without folder path)
#         filename = key.split("/")[-1]
#         parquet_name = filename.replace(".csv", ".parquet")

#         print(f"\nProcessing file: {filename}")

#         # --------------------------------------------
#         # 3. Download CSV file
#         # --------------------------------------------
#         file_obj = s3_client.get_object(
#             Bucket=S3_SOURCE_BUCKET,
#             Key=key
#         )
#         df = pd.read_csv(file_obj["Body"])

#         # --------------------------------------------
#         # 4. Add metadata columns
#         # --------------------------------------------
#         df["source_file"] = filename

#         # Use timezone-aware UTC timestamp (recommended)
#         df["ingestion_timestamp"] = datetime.now(timezone.utc)

#         print(df.head())

#         # --------------------------------------------
#         # 5. Send file to write_dataframe_to_s3 with filename override
#         # --------------------------------------------
#         write_dataframe_to_s3(
#             df=df,
#             source="call_center_logs",
#             filename=parquet_name  # <-- ensures separate files
#         )

#         # print(f"[INFO] Uploaded {parquet_name} to RAW Data Lake")

#     print("\n All call center logs ingested successfully!")


# if __name__ == "__main__":
#     ingest_call_center_logs()


# import pandas as pd
# from datetime import datetime, timezone
# import boto3

# from .s3_ingestion import write_dataframe_to_s3

# # S3 source configuration
# S3_SOURCE_BUCKET = "core-telecoms-data-lake"
# S3_SOURCE_PREFIX = "call logs/"  # folder containing call center CSVs


# def ingest_call_center_logs(s3_client_read=None, s3_client_write=None):
#     """
#     Ingest call center logs from S3, transform, and load into the RAW S3 zone.

#     Parameters:
#     - s3_client_read (boto3.client): Optional S3 client for reading from the source bucket.
#     - s3_client_write (boto3.client): Required S3 client for writing to the RAW zone.
#     """

#     if s3_client_write is None:
#         raise ValueError("s3_client_write must be provided for writing to RAW S3")

#     # Use provided read client or default boto3 client
#     s3_read = s3_client_read or boto3.client("s3")

#     print("Extracting Call Center Logs from S3...")

#     # List objects in the specified S3 prefix
#     response = s3_read.list_objects_v2(
#         Bucket=S3_SOURCE_BUCKET,
#         Prefix=S3_SOURCE_PREFIX
#     )

#     if "Contents" not in response:
#         print("No call center log files found in S3 path.")
#         return

#     for obj in response["Contents"]:
#         key = obj["Key"]

#         # Skip directories
#         if key.endswith("/"):
#             continue

#         filename = key.split("/")[-1]
#         parquet_filename = filename.replace(".csv", ".parquet")

#         print(f"Processing file: {filename}")

#         # Read CSV into DataFrame
#         file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
#         df = pd.read_csv(file_obj["Body"])

#         # Add metadata columns
#         df["source_file"] = filename
#         df["ingestion_timestamp"] = datetime.now(timezone.utc)

#         print(df.head())

#         # Write DataFrame to RAW layer
#         write_dataframe_to_s3(
#             df=df,
#             source="call_center_logs",
#             filename=parquet_filename,
#             s3_client_write=s3_client_write
#         )

#     print("All call center logs ingested successfully!")
