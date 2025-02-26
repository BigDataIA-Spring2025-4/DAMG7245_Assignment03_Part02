import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Constants
FRED_API_KEY = os.getenv("FRED_API_KEY")  
FRED_URL = 'https://api.stlouisfed.org/fred/series/observations'
AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")  
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=os.getenv("AWS_REGION")
)

def fetch_and_load_to_s3(FRED_DATA_ID):
    """Fetch new data from FRED API and upload to S3."""
    
    params = {
        'series_id': f'{FRED_DATA_ID}',
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': '2020-01-01'
    }
    
    response = requests.get(FRED_URL, params=params)
    response.raise_for_status()     
    
    data = response.json()
    observations = data.get('observations', [])
    
    if not observations:
        print("No data retrieved from FRED API.")
        return pd.DataFrame()

    df_new = pd.DataFrame(observations)
    df_new['date'] = pd.to_datetime(df_new['date'])
    
    upload_to_s3(df_new,FRED_DATA_ID)
    
    return df_new

def upload_to_s3(df,DATA_ID):
    """Upload the DataFrame to S3 as a CSV file."""
    if df.empty:
        print("No data to upload to S3.")
        return

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_file_key = f"{CURRENT_DATE}/{DATA_ID}_data.csv"
    
    s3.put_object(Bucket=AWS_S3_BUCKET_NAME, Key=s3_file_key, Body=csv_buffer.getvalue())
    
    print(f"Data successfully uploaded to S3 at {s3_file_key}")

def main():
    """Main function to execute the pipeline."""
    try:
        FRED_DATA_ID = 'DGS10,DGS2'
        for i in FRED_DATA_ID.split(','):
            print(f"Fetching data for {i}...")
            df_new = fetch_and_load_to_s3(i)
                   
        if not df_new.empty:
            print(f"Latest Data Loaded into S3 for partition: {CURRENT_DATE}")
        else:
            print("No new data to update.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()