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

        
        df["load_timestamp"] = datetime.now(timezone.utc)

        row_count = len(df)
        logging.info(f"DataFrame contains {row_count} rows and {len(df.columns)} columns.")

      
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        s3_path = f"{RAW_FOLDER}{source}/{date_str}/{filename}"

        logging.info(f"Target S3 path: s3://{S3_BUCKET}/{s3_path}")

        # Convert DF to Parquet buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        
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
        raise  



