import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timezone
import os

from s3_ingestion import write_dataframe_to_s3

def ingest_agents():
    print("Extracting Agents from Google Sheets...")

    # 1. Authenticate using JSON key
    creds_path = os.path.join(
        os.path.dirname(__file__),
        "../credentials/gsheet_ingestor.json"
    )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)

    client = gspread.authorize(creds)

    # 2. Open the sheet
    sheet = client.open("coretelecoms_agents").sheet1

    # 3. Read all rows
    rows = sheet.get_all_records()

    df = pd.DataFrame(rows)

    # 4. Add metadata
    df["ingestion_timestamp"] = datetime.now(timezone.utc)

    print(df.head())

    # 5. Write to Parquet in S3
    write_dataframe_to_s3(
            df=df,
            source="social_media"
            
        )

    print("Agents ingestion completed successfully.")
    
if __name__ == "__main__":
    ingest_agents()