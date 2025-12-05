import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timezone
import os
import logging

from .s3_ingestion import write_dataframe_to_s3


def ingest_agents(s3_client_write=None):
    """
    Ingests agents data from Google Sheets and writes to RAW S3.

    Parameters:
    - s3_client_write: boto3 client for writing raw data (required)
    """
    if s3_client_write is None:
        raise ValueError("s3_client_write must be provided for writing to RAW S3")

    logging.info("Starting Agents ingestion from Google Sheets...")

    try:
        #  Authenticate using JSON key
        creds_path = os.path.join(
            os.path.dirname(__file__),
            "../credentials/gsheet_ingestor.json"
        )

        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Google Sheets credentials file not found: {creds_path}")

        logging.info("Authenticating Google Sheets client...")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)

        
        logging.info("Opening Google Sheet: coretelecoms_agents...")
        sheet = client.open("coretelecoms_agents").sheet1

        
        logging.info("Reading rows from Google Sheets...")
        rows = sheet.get_all_records()
        df = pd.DataFrame(rows)

        if df.empty:
            logging.warning("Agents sheet is empty. Nothing to ingest.")
            return

        
        df["ingestion_timestamp"] = datetime.now(timezone.utc)

        print(df.head())

        logging.info(f"Fetched {len(df)} agent records.")


        logging.info("Writing Agents data to RAW S3...")
        write_dataframe_to_s3(
            df=df,
            source="agents",
            filename="agents.parquet",
            s3_client_write=s3_client_write
        )

        logging.info("Agents ingestion completed successfully.")

    except Exception as e:
        logging.error(f"Error during Agents ingestion: {e}", exc_info=True)
        raise 




