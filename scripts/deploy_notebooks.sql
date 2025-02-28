--!jinja

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

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema3}}"."{{env}}_03_analytics_table_processing"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/notebooks/03_analytics_table_processing/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = '03_analytics_table_processing.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema3}}"."{{env}}_03_analytics_table_processing" ADD LIVE VERSION FROM LAST;
