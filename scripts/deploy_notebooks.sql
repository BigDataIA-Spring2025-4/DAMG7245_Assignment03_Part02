--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       deploy_notebooks.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/

-- See https://docs.snowflake.com/en/LIMITEDACCESS/execute-immediate-from-template

-- Create the Notebooks
--USE SCHEMA {{env}}_SCHEMA;

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema1}}"."{{env}}_01_load_files"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/notebooks/01_load_files/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = '01_load_files.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema1}}"."{{env}}_01_load_files" ADD LIVE VERSION FROM LAST;

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema2}}"."{{env}}_02_raw_to_harmonized"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/notebooks/02_raw_to_harmonized/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = '02_raw_to_harmonized.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema2}}"."{{env}}_02_raw_to_harmonized" ADD LIVE VERSION FROM LAST;
