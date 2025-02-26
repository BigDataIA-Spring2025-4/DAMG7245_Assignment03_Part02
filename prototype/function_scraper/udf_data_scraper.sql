CREATE OR REPLACE FUNCTION fetch_fred_data()
RETURNS TABLE(date STRING, value FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'fetch_data'
AS
$$
import requests
import pandas as pd
from datetime import datetime

FRED_API_KEY = '##################'
FRED_URL = 'https://api.stlouisfed.org/fred/series/observations'

def fetch_data():
    """Fetch new data from the FRED API and return as a table."""
    params = {
        'series_id': 'T10Y2Y',
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': '2020-01-01'
    }

    response = requests.get(FRED_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    observations = data.get('observations', [])
    
    if not observations:
        return []
    
    df = pd.DataFrame(observations)
    df['date'] = df['date'].astype(str)  # Convert datetime to string
    df['value'] = df['value'].astype(float)
    
    return df[['date', 'value']].values.tolist()
$$;
