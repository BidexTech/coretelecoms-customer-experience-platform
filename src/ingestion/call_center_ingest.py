import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timezone
from s3_ingestion import write_dataframe_to_s3

# -----------------------------
S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "call logs/"   # Folder in S3


def ingest_call_center_logs():
    print(" Extracting Call Center Logs from S3...")

    s3_client = boto3.client("s3")

    # --------------------------------------------
    # 1. LIST all CSV files in the folder
    # --------------------------------------------
    response = s3_client.list_objects_v2(
        Bucket=S3_SOURCE_BUCKET,
        Prefix=S3_SOURCE_PREFIX
    )
    if "Contents" not in response:
        print(" No call center log files found")
        return

    # --------------------------------------------
    # 2. Process each CSV file individually
    # --------------------------------------------
    for obj in response["Contents"]:
        key = obj["Key"]

        # Skip folder markers
        if key.endswith("/"):
            continue

        # Extract only the filename (without folder path)
        filename = key.split("/")[-1]
        parquet_name = filename.replace(".csv", ".parquet")

        print(f"\nProcessing file: {filename}")

        # --------------------------------------------
        # 3. Download CSV file
        # --------------------------------------------
        file_obj = s3_client.get_object(
            Bucket=S3_SOURCE_BUCKET,
            Key=key
        )
        df = pd.read_csv(file_obj["Body"])

        # --------------------------------------------
        # 4. Add metadata columns
        # --------------------------------------------
        df["source_file"] = filename

        # Use timezone-aware UTC timestamp (recommended)
        df["ingestion_timestamp"] = datetime.now(timezone.utc)

        print(df.head())

        # --------------------------------------------
        # 5. Send file to write_dataframe_to_s3 with filename override
        # --------------------------------------------
        write_dataframe_to_s3(
            df=df,
            source="call_center_logs",
            filename=parquet_name  # <-- ensures separate files
        )

        # print(f"[INFO] Uploaded {parquet_name} to RAW Data Lake")

    print("\n All call center logs ingested successfully!")


if __name__ == "__main__":
    ingest_call_center_logs()
