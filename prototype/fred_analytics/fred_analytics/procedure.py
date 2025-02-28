import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_fred_10y_table(session):
    SHARED_COLUMNS= [T.StructField("OBS_DATE", T.DateType()),
                                        T.StructField("TOTAL_RECORDS", T.DecimalType()),
                                        T.StructField("MIN_VALUE", T.DecimalType()),
                                        T.StructField("MAX_VALUE", T.DecimalType()),
                                        T.StructField("AVG_VALUE", T.DecimalType()),
                                    ]
    FRED_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    FRED_SCHEMA = T.StructType(FRED_COLUMNS)

    dcm = session.create_dataframe([[None]*len(FRED_SCHEMA.names)], schema=FRED_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('ANALYTICS.FRED_10Y_WEEKLY')
    dcm = session.table('ANALYTICS.FRED_10Y_WEEKLY')

def create_fred_2y_table(session):
    SHARED_COLUMNS= [T.StructField("OBS_DATE", T.DateType()),
                                        T.StructField("TOTAL_RECORDS", T.DecimalType()),
                                        T.StructField("MIN_VALUE", T.DecimalType()),
                                        T.StructField("MAX_VALUE", T.DecimalType()),
                                        T.StructField("AVG_VALUE", T.DecimalType()),
                                    ]
    FRED_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    FRED_SCHEMA = T.StructType(FRED_COLUMNS)

    dcm = session.create_dataframe([[None]*len(FRED_SCHEMA.names)], schema=FRED_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('ANALYTICS.FRED_2Y_WEEKLY')
    dcm = session.table('ANALYTICS.FRED_2Y_WEEKLY')


def aggregate_fred_daily(session):
    fred_10y = session.table("FRED_SCHEMA.FRED_10Y_TABLE")
    fred_2y = session.table("FRED_SCHEMA.FRED_2Y_TABLE")
    fred = fred_10y.join(fred_2y, fred_10y['OBS_DATE'] == fred_2y['OBS_DATE'], rsuffix='_2y')
    fred = fred.select(fred_10y['OBS_DATE'], fred_10y['PERCENT_CHANGE'].alias('PERCENT_CHANGE_10y'), fred_2y['PERCENT_CHANGE'].alias('PERCENT_CHANGE_2y'))
    fred.write.mode('overwrite').save_as_table('ANALYTICS.FRED_DAILY')
    print("FRED_DAILY table aggregated")
    session.table('ANALYTICS.FRED_DAILY').limit(5).show()
    
    
def aggregate_fred_10y_weekly(session):
    fred_10y = session.table("FRED_SCHEMA.FRED_10Y_TABLE")
    fred_10y_agg = fred_10y.group_by(F.date_trunc('WEEK', F.col('OBS_DATE')).alias("WEEK_START")) \
                        .agg( \
                            F.count('PERCENT_CHANGE').alias("TOTAL_RECORDS"), \
                            F.round(F.min('PERCENT_CHANGE'),2).alias("MIN_VALUE"), \
                            F.round(F.max('PERCENT_CHANGE'),2).alias("MAX_VALUE"), \
                            F.round(F.avg('PERCENT_CHANGE'),2).alias("AVG_VALUE") \
                        )
    fred_10y_agg.write.mode('overwrite').save_as_table('ANALYTICS.FRED_10Y_WEEKLY') 

def aggregate_fred_2y_weekly(session):
    fred_2y = session.table("FRED_SCHEMA.FRED_2Y_TABLE")
    fred_2y_agg = fred_2y.group_by(F.date_trunc('WEEK', F.col('OBS_DATE')).alias("WEEK_START")) \
                        .agg( \
                            F.count('PERCENT_CHANGE').alias("TOTAL_RECORDS"), \
                            F.round(F.min('PERCENT_CHANGE'),2).alias("MIN_VALUE"), \
                            F.round(F.max('PERCENT_CHANGE'),2).alias("MAX_VALUE"), \
                            F.round(F.avg('PERCENT_CHANGE'),2).alias("AVG_VALUE") \
                        )
    fred_2y_agg.write.mode('overwrite').save_as_table('ANALYTICS.FRED_2Y_WEEKLY') 

def aggregate_fred_weekly(session):
    # Aggregate the FRED_10Y_WEEKLY and FRED_2Y_WEEKLY tables
    
    aggregate_fred_10y_weekly(session)
    print("FRED_10Y_WEEKLY table aggregated")
    session.table('ANALYTICS.FRED_10Y_WEEKLY').limit(5).show()
    
    aggregate_fred_2y_weekly(session)
    print("FRED_2Y_WEEKLY table aggregated")
    session.table('ANALYTICS.FRED_2Y_WEEKLY').limit(5).show()
    
    # Merge the FRED_10Y_WEEKLY and FRED_2Y_WEEKLY tables
    fred_10y = session.table('ANALYTICS.FRED_10Y_WEEKLY')
    fred_2y = session.table('ANALYTICS.FRED_2Y_WEEKLY')
    fred = fred_10y.join(fred_2y, fred_10y['WEEK_START'] == fred_2y['WEEK_START'], rsuffix='_2y')
    fred = fred.select(fred_10y['WEEK_START'], fred_10y['TOTAL_RECORDS'].alias('TOTAL_RECORDS_10y'), fred_10y['MIN_VALUE'].alias('MIN_VALUE_10y'), fred_10y['MAX_VALUE'].alias('MAX_VALUE_10y'), fred_10y['AVG_VALUE'].alias('AVG_VALUE_10y'), fred_2y['TOTAL_RECORDS'].alias('TOTAL_RECORDS_2y'), fred_2y['MIN_VALUE'].alias('MIN_VALUE_2y'), fred_2y['MAX_VALUE'].alias('MAX_VALUE_2y'), fred_2y['AVG_VALUE'].alias('AVG_VALUE_2y'))
    fred.write.mode('overwrite').save_as_table('ANALYTICS.FRED_WEEKLY')
    print("FRED_WEEKLY table aggregated")
    session.table('ANALYTICS.FRED_WEEKLY').limit(5).show()

def aggregate_fred_10y_monthly(session):
    fred_10y = session.table("FRED_SCHEMA.FRED_10Y_TABLE")
    fred_10y_agg = fred_10y.group_by(F.date_trunc('MONTH', F.col('OBS_DATE')).alias("MONTH_START")) \
                        .agg( \
                            F.count('PERCENT_CHANGE').alias("TOTAL_RECORDS"), \
                            F.round(F.min('PERCENT_CHANGE'),2).alias("MIN_VALUE"), \
                            F.round(F.max('PERCENT_CHANGE'),2).alias("MAX_VALUE"), \
                            F.round(F.avg('PERCENT_CHANGE'),2).alias("AVG_VALUE") \
                        )
    fred_10y_agg.write.mode('overwrite').save_as_table('ANALYTICS.FRED_10Y_MONTHLY') 
    
def aggregate_fred_2y_monthly(session):
    fred_2y = session.table("FRED_SCHEMA.FRED_2Y_TABLE")
    fred_2y_agg = fred_2y.group_by(F.date_trunc('MONTH', F.col('OBS_DATE')).alias("MONTH_START")) \
                        .agg( \
                            F.count('PERCENT_CHANGE').alias("TOTAL_RECORDS"), \
                            F.round(F.min('PERCENT_CHANGE'),2).alias("MIN_VALUE"), \
                            F.round(F.max('PERCENT_CHANGE'),2).alias("MAX_VALUE"), \
                            F.round(F.avg('PERCENT_CHANGE'),2).alias("AVG_VALUE") \
                        )
    fred_2y_agg.write.mode('overwrite').save_as_table('ANALYTICS.FRED_2Y_MONTHLY') 
    
def aggregate_fred_monthly(session):
    # Aggregate the FRED_10Y_WEEKLY and FRED_2Y_WEEKLY tables
    
    aggregate_fred_10y_monthly(session)
    print("FRED_10Y_MONTHLY table aggregated")
    session.table('ANALYTICS.FRED_10Y_MONTHLY').limit(5).show()
    
    aggregate_fred_2y_monthly(session)
    print("FRED_2Y_MONTHLY table aggregated")
    session.table('ANALYTICS.FRED_2Y_MONTHLY').limit(5).show()
    
    # Merge the FRED_10Y_MONTHLY and FRED_2Y_MONTHLY tables
    fred_10y = session.table('ANALYTICS.FRED_10Y_MONTHLY')
    fred_2y = session.table('ANALYTICS.FRED_2Y_MONTHLY')
    fred = fred_10y.join(fred_2y, fred_10y['MONTH_START'] == fred_2y['MONTH_START'], rsuffix='_2y')
    fred = fred.select(fred_10y['MONTH_START'], fred_10y['TOTAL_RECORDS'].alias('TOTAL_RECORDS_10y'), fred_10y['MIN_VALUE'].alias('MIN_VALUE_10y'), fred_10y['MAX_VALUE'].alias('MAX_VALUE_10y'), fred_10y['AVG_VALUE'].alias('AVG_VALUE_10y'), fred_2y['TOTAL_RECORDS'].alias('TOTAL_RECORDS_2y'), fred_2y['MIN_VALUE'].alias('MIN_VALUE_2y'), fred_2y['MAX_VALUE'].alias('MAX_VALUE_2y'), fred_2y['AVG_VALUE'].alias('AVG_VALUE_2y'))
    fred.write.mode('overwrite').save_as_table('ANALYTICS.FRED_MONTHLY')
    print("FRED_MONTHLY table aggregated")
    session.table('ANALYTICS.FRED_MONTHLY').limit(5).show()
        
def main(session: Session) -> str:
    # Create the DAILY_CITY_METRICS table if it doesn't exist
    if not table_exists(session, schema='ANALYTICS', name='FRED_10Y_WEEKLY'):
        create_fred_10y_table(session)
        print("FRED_10Y_WEEKLY table created")
    if not table_exists(session, schema='ANALYTICS', name='FRED_2Y_WEEKLY'):
        create_fred_2y_table(session)
        print("FRED_2Y_WEEKLY table created")
        
    aggregate_fred_daily(session)
    aggregate_fred_weekly(session)
    aggregate_fred_monthly(session)

    return f"Successfully aggregated analytics tables"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        import sys
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
