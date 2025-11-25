import pandas as pd
import boto3
from datetime import datetime
from s3_ingestion import write_dataframe_to_s3

# -----------------------------------------------------
# S3 SOURCE CONFIG
# -----------------------------------------------------
S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "social_medias/"   # folder with JSON files


def ingest_social_media():
    print(" Extracting Social Medias Complaints Data from S3...")

    s3_client = boto3.client("s3")

    # -----------------------------------------------------
    # 1. LIST all JSON complaint files
    # -----------------------------------------------------
    response = s3_client.list_objects_v2(
        Bucket=S3_SOURCE_BUCKET,
        Prefix=S3_SOURCE_PREFIX
    )

    if "Contents" not in response:
        print(" No social media JSON files found.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        # Skip empty folder markers
        if key.endswith("/"):
            continue

        print(f"Processing file: {key}")

        # -------------------------------------------------
        # 2. Read JSON file
        # -------------------------------------------------
        file_obj = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)

        # Handle JSON → Pandas (works for list JSON or record JSON)
        df = pd.read_json(file_obj["Body"])

        # -------------------------------------------------
        # 3. Add RAW metadata
        # -------------------------------------------------
        df["source_file"] = key
        df["ingestion_timestamp"] = datetime.now()

        # print(df.head())
        # -------------------------------------------------
        # 4. Create parquet filename (same as input)
        # -------------------------------------------------
        parquet_filename = key.split("/")[-1].replace(".json", ".parquet")

        # -------------------------------------------------
        # 5. Write to unified RAW data lake
        # -------------------------------------------------
        write_dataframe_to_s3(
            df=df,
            source="social_media",
            filename=parquet_filename
        )

    print(" All social medias JSON files ingested successfully!")


if __name__ == "__main__":
    ingest_social_media()
