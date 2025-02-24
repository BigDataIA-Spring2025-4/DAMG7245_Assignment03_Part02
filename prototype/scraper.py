import os
import requests
import pandas as pd
from dotenv import load_dotenv
import boto3
from io import StringIO

# Load environment variables
load_dotenv()

# Constants
FRED_DATASET_API_KEY = os.getenv("FRED_DATASET_API_KEY")  # FRED API key from environment variables
FRED_URL = 'https://api.stlouisfed.org/fred/series/observations'
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = 'raw/t10y2y_data.parquet'  

# Initialize S3 client
s3 = boto3.client('s3')

def fetch_last_updated_date():
    """Fetch the last updated date from the existing S3 file."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
        df_existing = pd.read_parquet(obj['Body'])
        return df_existing['date'].max()
    except s3.exceptions.NoSuchKey:
        return None

def fetch_new_data(last_date):
    """Fetch new data from FRED API starting from the last available date."""
    params = {
        'series_id': 'T10Y2Y',
        'api_key': FRED_DATASET_API_KEY,
        'file_type': 'json',
        'observation_start': last_date if last_date else '2020-01-01'
    }
    response = requests.get(FRED_URL, params=params)
    data = response.json()
    observations = data['observations']
    df_new = pd.DataFrame(observations)
    df_new['date'] = pd.to_datetime(df_new['date'])  # Convert date to datetime format
    return df_new

def merge_and_upload_to_s3(df_new):
    """Merge new data with existing data and upload back to S3."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
        df_existing = pd.read_parquet(obj['Body'])
        df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset='date').sort_values('date')
    except s3.exceptions.NoSuchKey:
        df_combined = df_new
    
    buffer = StringIO()
    df_combined.to_parquet(buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY, Body=buffer.getvalue())
    print("Data successfully uploaded to S3.")

def main():
    
    last_date = fetch_last_updated_date()

    df_new = fetch_new_data(last_date)
    
    if not df_new.empty:
        merge_and_upload_to_s3(df_new)
    else:
        print("No new data to update.")

if __name__ == "__main__":
    main()