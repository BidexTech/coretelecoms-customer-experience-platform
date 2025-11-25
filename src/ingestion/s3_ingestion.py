import boto3
import pandas as pd
from datetime import datetime, timezone
import io
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "coretelecom-raw-data")
RAW_FOLDER = "raw/"

# AWS CLI credentials automatically used
s3_client = boto3.client("s3")


def write_dataframe_to_s3(df: pd.DataFrame, source: str, filename: str = None):
    """
    Writes a DataFrame to S3 as Parquet with load timestamp.
    Supports custom filenames so multiple files do NOT overwrite each other.
    No transformations applied.
    """

    # Always use timezone-aware UTC timestamps
    df["load_timestamp"] = datetime.now(timezone.utc)

    # Today’s folder name
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # If no custom filename is passed → default to old behavior
    if filename is None:
        filename = f"{source}.parquet"

    # Final S3 path
    s3_path = f"{RAW_FOLDER}{source}/{date_str}/{filename}"

    # Write df to Parquet buffer
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_path,
        Body=buffer.getvalue()
    )

    print(f"[INFO] Uploaded {filename} to s3://{S3_BUCKET}/{s3_path}")
