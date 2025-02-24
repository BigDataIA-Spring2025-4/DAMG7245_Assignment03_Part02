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

-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;