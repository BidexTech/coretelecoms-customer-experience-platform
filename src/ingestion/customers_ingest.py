
import pandas as pd
import boto3
from datetime import datetime
from s3_ingestion import write_dataframe_to_s3

S3_SOURCE_BUCKET = "core-telecoms-data-lake"
S3_SOURCE_PREFIX = "customers/"   # folder containing customer CSVs

def ingest_customers():
    print(" Extracting Customers Data from S3...")

    s3_client = boto3.client("s3")

    # --------------------------------------------
    # 1. LIST objects in the customers folder
    # --------------------------------------------
    response = s3_client.list_objects_v2(
        Bucket=S3_SOURCE_BUCKET,
        Prefix=S3_SOURCE_PREFIX
    )

    if "Contents" not in response:
        print(" No customer files found in source bucket.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        # Skip folder markers
        if key.endswith("/"):
            continue

        print(f"Processing file: {key}")

        # Download file
        file_obj = s3_client.get_object(Bucket=S3_SOURCE_BUCKET, Key=key)
        df = pd.read_csv(file_obj["Body"])

        # Add metadata
        df["source_file"] = key
        df["ingestion_timestamp"] = datetime.now()

        print(df.head())

        # Generate output file name (same as raw CSV but parquet)
        parquet_name = key.split("/")[-1].replace(".csv", ".parquet")

        # Write parquet to the unified RAW layer
        write_dataframe_to_s3(
            df=df,
            source="customers",
            filename=parquet_name
        )

    print(" All customer files ingested successfully!")

if __name__ == "__main__":
    ingest_customers()
