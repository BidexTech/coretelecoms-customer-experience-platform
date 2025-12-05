import pandas as pd
from datetime import datetime, timezone
import io
import os
import logging

S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecoms-datalake-raw")
RAW_FOLDER = "raw/"


def write_dataframe_to_s3(df: pd.DataFrame, source: str, filename: str, s3_client_write):
    """
    Writes a DataFrame to S3 as Parquet using Airflow's injected S3 client.

    Adds:
    - Logging for start/end of write
    - Logging row count + columns
    - Logging final S3 path
    - Full error traceback
    """

    try:
        logging.info(f"Preparing to upload DataFrame for source '{source}'...")

        # Add ingestion metadata
        df["load_timestamp"] = datetime.now(timezone.utc)

        row_count = len(df)
        logging.info(f"DataFrame contains {row_count} rows and {len(df.columns)} columns.")

        # Partition folder by date
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        s3_path = f"{RAW_FOLDER}{source}/{date_str}/{filename}"

        logging.info(f"Target S3 path: s3://{S3_BUCKET}/{s3_path}")

        # Convert DF to Parquet buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload to S3
        s3_client_write.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=buffer.getvalue()
        )

        logging.info(
            f"Successfully Ingested {filename} ({row_count} rows) → s3://{S3_BUCKET}/{s3_path}"
        )

    except Exception as e:
        logging.error(
            f"Failed to write DataFrame to S3 for source '{source}' — {e}",
            exc_info=True
        )
        raise  # Let Airflow catch and mark task as FAILED



# import boto3
# import pandas as pd
# from datetime import datetime, timezone
# import io
# import os

# S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecoms-datalake-raw")
# RAW_FOLDER = "raw/"

# # AWS CLI credentials automatically used peronal profile
# session = boto3.Session(profile_name="personal")
# s3_client = session.client("s3")


# def write_dataframe_to_s3(df: pd.DataFrame, source: str, filename: str = None):
#     """
#     Writes a DataFrame to S3 as Parquet with load timestamp.
#     Supports custom filenames so multiple files do NOT overwrite each other.
#     No transformations applied.
#     """

#     # Always use timezone-aware UTC timestamps
#     df["load_timestamp"] = datetime.now(timezone.utc)

#     # Today’s folder name
#     date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

#     # If no custom filename is passed → default to old behavior
#     if filename is None:
#         filename = f"{source}.parquet"

#     # Final S3 path
#     s3_path = f"{RAW_FOLDER}{source}/{date_str}/{filename}"

#     # Write df to Parquet buffer
#     buffer = io.BytesIO()
#     df.to_parquet(buffer, index=False)
#     buffer.seek(0)

#     # Upload to S3
#     s3_client.put_object(
#         Bucket=S3_BUCKET,
#         Key=s3_path,
#         Body=buffer.getvalue()
#     )

#     print(f"[INFO] Uploaded {filename} to s3://{S3_BUCKET}/{s3_path}")


# import pandas as pd
# from datetime import datetime, timezone
# import io
# import os

# S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecoms-datalake-raw")
# RAW_FOLDER = "raw/"


# def write_dataframe_to_s3(df: pd.DataFrame, source: str, filename: str, s3_client_write):
#     """
#     Writes a DataFrame to S3 as Parquet.
#     - No boto3 sessions here (Airflow provides the client)
#     - Supports partitioned date folder
#     """

#     df["load_timestamp"] = datetime.now(timezone.utc)
#     date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

#     # Final S3 path
#     s3_path = f"{RAW_FOLDER}{source}/{date_str}/{filename}"

#     # Convert DF to Parquet buffer
#     buffer = io.BytesIO()
#     df.to_parquet(buffer, index=False)
#     buffer.seek(0)

#     # Upload with s3_client_write
#     s3_client_write.put_object(
#         Bucket=S3_BUCKET,
#         Key=s3_path,
#         Body=buffer.getvalue()
#     )

#     print(f"Uploaded {filename} → s3://{S3_BUCKET}/{s3_path}")

