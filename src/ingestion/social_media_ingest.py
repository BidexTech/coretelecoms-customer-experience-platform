import pandas as pd
from datetime import datetime, timezone
import logging
import boto3

from .s3_ingestion import write_dataframe_to_s3

# S3 source configuration
S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "social_medias/"  # folder containing JSON complaint files


def ingest_social_media(s3_client_read=None, s3_client_write=None):
    """
    Ingest social media JSON complaints from S3 into the RAW S3 layer.

    Parameters:
    - s3_client_read: Optional boto3 client for reading from S3.
    - s3_client_write: Required boto3 client for writing to RAW S3.
    """

    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided for writing to RAW S3")

    # Use provided read client or fallback to default boto3
    s3_read = s3_client_read or boto3.client("s3")

    try:
        logging.info("Starting Social Media Complaints ingestion from S3...")

        # ----------------------------------------------------
        # 1. List JSON objects in S3
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # 2. Process each object
        # ----------------------------------------------------
        for obj in response["Contents"]:
            key = obj["Key"]

            if key.endswith("/"):
                continue  # skip folder placeholders

            logging.info(f"Processing social media file: {key}")

            try:
                # ----------------------------------------------------
                # 3. Read JSON into DataFrame
                # ----------------------------------------------------
                file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
                df = pd.read_json(file_obj["Body"])

                if df.empty:
                    logging.warning(f"File {key} is empty. Skipping.")
                    continue

                logging.info(f"Read {len(df)} records from {key}")

                # ----------------------------------------------------
                # 4. Add ingestion metadata
                # ----------------------------------------------------
                df["source_file"] = key.split("/")[-1]
                df["ingestion_timestamp"] = datetime.now(timezone.utc)

                print(df.head())

                # ----------------------------------------------------
                # 5. Format output file name
                # ----------------------------------------------------
                parquet_filename = df["source_file"].iloc[0].replace(".json", ".parquet")

                # ----------------------------------------------------
                # 6. Write to RAW S3
                # ----------------------------------------------------
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
                continue  # move on to next file

        logging.info("All social media JSON files ingested successfully!")

    except Exception as e:
        logging.error(f"Social Media ingestion failed: {e}", exc_info=True)
        raise  # allow Airflow to mark the task as FAILED


# import pandas as pd
# import boto3
# from datetime import datetime
# from s3_ingestion import write_dataframe_to_s3

# # -----------------------------------------------------
# # S3 SOURCE CONFIG
# # -----------------------------------------------------
# S3_SOURCE_BUCKET = "core-telecoms-data-lake"
# S3_SOURCE_PREFIX = "social_medias/"   # folder with JSON files


# def ingest_social_media():
#     print(" Extracting Social Medias Complaints Data from S3...")

#     s3_client = boto3.client("s3")

#     # -----------------------------------------------------
#     # 1. LIST all JSON complaint files
#     # -----------------------------------------------------
#     response = s3_client.list_objects_v2(
#         Bucket=S3_SOURCE_BUCKET,
#         Prefix=S3_SOURCE_PREFIX
#     )

#     if "Contents" not in response:
#         print(" No social media JSON files found.")
#         return

#     for obj in response["Contents"]:
#         key = obj["Key"]

#         # Skip empty folder markers
#         if key.endswith("/"):
#             continue

#         print(f"Processing file: {key}")

#         # -------------------------------------------------
#         # 2. Read JSON file
#         # -------------------------------------------------
#         file_obj = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)

#         # Handle JSON → Pandas (works for list JSON or record JSON)
#         df = pd.read_json(file_obj["Body"])

#         # -------------------------------------------------
#         # 3. Add RAW metadata
#         # -------------------------------------------------
#         df["source_file"] = key
#         df["ingestion_timestamp"] = datetime.now()

#         print(df.head())

#         # print(df.head())
#         # -------------------------------------------------
#         # 4. Create parquet filename (same as input)
#         # -------------------------------------------------
#         parquet_filename = key.split("/")[-1].replace(".json", ".parquet")

#         # -------------------------------------------------
#         # 5. Write to unified RAW data lake
#         # -------------------------------------------------
#         write_dataframe_to_s3(
#             df=df,
#             source="social_media",
#             filename=parquet_filename
#         )

#     print(" All social medias JSON files ingested successfully!")


# if __name__ == "__main__":
#     ingest_social_media()


# import pandas as pd
# from datetime import datetime, timezone
# from .s3_ingestion import write_dataframe_to_s3
# import boto3

# # S3 source configuration
# S3_SOURCE_BUCKET = "core-telecoms-data-lake"
# S3_SOURCE_PREFIX = "social_medias/"  # folder containing JSON complaint files


# def ingest_social_media(s3_client_read=None, s3_client_write=None):
#     """
#     Ingests social media complaints from S3, adds metadata, 
#     and writes them to the RAW S3 layer.

#     Parameters:
#     - s3_client_read: boto3 client for reading the source bucket (optional)
#     - s3_client_write: boto3 client for writing raw data (required)
#     """

#     if s3_client_write is None:
#         raise ValueError("s3_client_write must be provided for writing to RAW S3")

#     # Use provided read client or fallback to boto3 default
#     s3_read = s3_client_read or boto3.client("s3")

#     print("Extracting Social Media Complaints from S3...")

#     # ----------------------------------------------------
#     # 1. List JSON objects
#     # ----------------------------------------------------
#     response = s3_read.list_objects_v2(
#         Bucket=S3_SOURCE_BUCKET,
#         Prefix=S3_SOURCE_PREFIX
#     )

#     if "Contents" not in response:
#         print("No social media JSON files found.")
#         return

#     # ----------------------------------------------------
#     # 2. Iterate through all JSON files
#     # ----------------------------------------------------
#     for obj in response["Contents"]:
#         key = obj["Key"]

#         if key.endswith("/"):
#             continue  # skip folder markers

#         print(f"Processing file: {key}")

#         # ----------------------------------------------------
#         # 3. Read JSON → Pandas DataFrame
#         # ----------------------------------------------------
#         file_obj = s3_read.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
#         df = pd.read_json(file_obj["Body"])

#         # ----------------------------------------------------
#         # 4. Add ingestion metadata
#         # ----------------------------------------------------
#         df["source_file"] = key.split("/")[-1]
#         df["ingestion_timestamp"] = datetime.now(timezone.utc)

#         print(df.head())

#         # ----------------------------------------------------
#         # 5. Format parquet filename
#         # ----------------------------------------------------
#         parquet_filename = df["source_file"].iloc[0].replace(".json", ".parquet")

#         # ----------------------------------------------------
#         # 6. Write to RAW S3
#         # ----------------------------------------------------
#         write_dataframe_to_s3(
#             df=df,
#             source="social_media",
#             filename=parquet_filename,
#             s3_client_write=s3_client_write
#         )

#     print("All social media JSON files ingested successfully!")



