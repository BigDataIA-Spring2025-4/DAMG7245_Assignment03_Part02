/*-----------------------------------------------------------------------------
Script : Data Objects Set-up from our Snowflake Account with Snowpark
Name:       01_setup_snowflake.sql
Last Updated: 23/1/2025
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE FRED_ROLE;
GRANT ROLE FRED_ROLE TO ROLE SYSADMIN;
GRANT ROLE FRED_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE FRED_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE FRED_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE FRED_ROLE;

-- Databases
CREATE OR REPLACE DATABASE FRED_DB;
GRANT OWNERSHIP ON DATABASE FRED_DB TO ROLE FRED_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE FRED_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE FRED_WH TO ROLE FRED_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE FRED_ROLE;
USE WAREHOUSE FRED_WH;
USE DATABASE FRED_DB;

-- Schemas
CREATE OR REPLACE SCHEMA DEV_SCHEMA;
CREATE OR REPLACE SCHEMA PROD_SCHEMA;
CREATE OR REPLACE SCHEMA RAW_PARQUET;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;
4
-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;


CREATE OR REPLACE NETWORK RULE api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('jsonplaceholder.typicode.com');


-- External Access Integration - Unable to create to due to being on a trial account
-- Error : SQL compilation error: External access is not supported for trial accounts.
  CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (api_network_rule)
  ENABLED = true;


CREATE OR REPLACE TABLE SECRET (
    secret_name STRING PRIMARY KEY,
    secret_value STRING
);
INSERT INTO SECRET (secret_name, secret_value)
VALUES ('FRED_API_KEY', '##########');

INSERT INTO SECRET (secret_name, secret_value)
VALUES ('AWS_ACCESS_KEY', '######');


INSERT INTO SECRET (secret_name, secret_value)
VALUES ('AWS_SECRET_KEY', '#########');

INSERT INTO SECRET (secret_name, secret_value)
VALUES ('AWS_REGION', 'us-east-2');


INSERT INTO SECRET (secret_name, secret_value)
VALUES ('AWS_S3_BUCKET_NAME', '#########');

CREATE OR REPLACE FUNCTION get_todos(completed boolean) returns
table (userId number, id number, title varchar, completed boolean)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'ApiData'
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)
PACKAGES = ('requests')
AS
$$
import requests
class ApiData:  
    def process(self, completed):
        data = requests.get("https://jsonplaceholder.typicode.com/todos").json()
        for row in data:
            if row["completed"] == completed:
                yield (row["userId"], row["id"], row["title"], row["completed"])
$$;

CREATE OR REPLACE PROCEDURE test_proc()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests', 'pandas', 'boto3')
HANDLER = 'test_handler'
AS
$$
def test_handler(session):
    return "Hello from test_proc!"
$$;


CREATE OR REPLACE PROCEDURE fetch_and_upload_fred_data_proc()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'fetch_data_proc'
PACKAGES = ('snowflake-snowpark-python','requests', 'pandas', 'boto3')
AS
$$
def fetch_data_proc(session):
    # Retrieve the FRED API key from the SECRET table
    result = session.sql("SELECT secret_value FROM SECRET WHERE secret_name = 'FRED_API_KEY'").collect()
    if not result:
        return "FRED API key not found in SECRET table."
    fred_api_key = result[0][0]
    
    # Retrieve AWS credentials and S3 bucket details from the SECRET table
    aws_access_result = session.sql("SELECT secret_value FROM SECRET WHERE secret_name = 'AWS_ACCESS_KEY'").collect()
    aws_secret_result = session.sql("SELECT secret_value FROM SECRET WHERE secret_name = 'AWS_SECRET_KEY'").collect()
    aws_region_result = session.sql("SELECT secret_value FROM SECRET WHERE secret_name = 'AWS_REGION'").collect()
    bucket_result = session.sql("SELECT secret_value FROM SECRET WHERE secret_name = 'AWS_S3_BUCKET_NAME'").collect()
    
    if not (aws_access_result and aws_secret_result and aws_region_result and bucket_result):
        return "One or more AWS credentials/bucket details not found in SECRET table."
    
    aws_access_key = aws_access_result[0][0]
    aws_secret_key = aws_secret_result[0][0]
    aws_region = aws_region_result[0][0]
    s3_bucket_name = bucket_result[0][0]
    
    import requests
    import pandas as pd
    import boto3
    from io import StringIO
    from datetime import datetime

    FRED_URL = 'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': 'T10Y2Y',
        'api_key': fred_api_key,
        'file_type': 'json',
        'observation_start': '2020-01-01'
    }
    
    # Fetch data from the FRED API
    response = requests.get(FRED_URL, params=params)
    response.raise_for_status()
    data = response.json()
    observations = data.get('observations', [])
    
    if not observations:
        return "No data retrieved from FRED API."
    
    # Convert observations to a DataFrame
    df = pd.DataFrame(observations)
    df['date'] = df['date'].astype(str)  # Ensure date is a string
    df['value'] = df['value'].astype(float)  # Ensure value is a float
    
    # Convert DataFrame to CSV using an in-memory buffer
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Create boto3 S3 client using the retrieved AWS credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Define a file key using the current date for partitioning
    current_date = datetime.now().strftime('%Y-%m-%d')
    s3_file_key = f"{current_date}/t10y2y_data.csv"
    
    # Upload the CSV data to the specified S3 bucket
    s3.put_object(Bucket=s3_bucket_name, Key=s3_file_key, Body=csv_buffer.getvalue())
    
    return f"Data successfully uploaded to S3 at {s3_file_key}."

# The stored procedure entry point expects a function named "main" that receives a session object.
$$;

CALL fetch_and_upload_fred_data_proc();
