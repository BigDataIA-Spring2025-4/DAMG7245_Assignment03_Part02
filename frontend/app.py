# Import required packages
import streamlit as st
import snowflake.connector
import pandas as pd

# Snowflake connection function
@st.cache_resource
def create_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )


# Function to load data from Snowflake
# @st.cache_data()
def load_data(conn):
    daily_query = "SELECT * FROM FRED_DB.PROD_ANALYTICS.FRED_COMBINED_DAILY"
    weekly_query = "SELECT * FROM FRED_DB.PROD_ANALYTICS.FRED_COMBINED_WEEKLY"
    monthly_query =  "SELECT * FROM FRED_DB.PROD_ANALYTICS.FRED_COMBINED_MONTHLY"

    daily_agg = pd.read_sql(daily_query, conn)
    weekly_agg = pd.read_sql(weekly_query, conn)
    monthly_agg = pd.read_sql(monthly_query, conn)

    return daily_agg, weekly_agg, monthly_agg

def daily_data_analytics(daily_agg):
    st.subheader('Daily Yield Analytics')

    # Ensure OBS_DATE is in datetime format
    daily_agg['OBS_DATE'] = pd.to_datetime(daily_agg['OBS_DATE'])

    # Get min and max date
    min_date = daily_agg['OBS_DATE'].min()
    max_date = daily_agg['OBS_DATE'].max()

    # Date range input
    date_range = st.date_input(
        "Date range:", 
        [min_date.date(), max_date.date()],  # Convert min/max to .date()
        min_value=min_date.date(), 
        max_value=max_date.date(), 
        key='date_range'
    )

    # Ensure date_range returns a tuple of two dates
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Apply filtering
        df_filter = daily_agg[(daily_agg['OBS_DATE'] >= start_date) & (daily_agg['OBS_DATE'] <= end_date)]
        # Remove rows where 10Y_YIELD or 2Y_YIELD is 0
        df_filter = df_filter[(df_filter['10Y_YIELD'] > 0) & (df_filter['2Y_YIELD'] > 0)]

        st.line_chart(df_filter, x = 'OBS_DATE', y = ['10Y_YIELD', '2Y_YIELD'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        

        data = df_filter[['OBS_DATE', 'SPREAD']]

        data['Positive Change'] = data['SPREAD'].apply(lambda x: x if x >= 0 else None)
        data['Negative Change'] = data['SPREAD'].apply(lambda x: x if x < 0 else None)

        st.line_chart(
            data.set_index('OBS_DATE')[['Positive Change', 'Negative Change']], 
            use_container_width=True
        )

    else:
        st.warning("Please select a valid date range.")
        
def weekly_data_analytics(weekly_agg):
    st.subheader('Weekly Yield Spread Analytics')

    # Ensure WEEK_START is in datetime format
    weekly_agg['WEEK_START'] = pd.to_datetime(weekly_agg['WEEK_START'])

    # Get min and max date
    min_date = weekly_agg['WEEK_START'].min()
    max_date = weekly_agg['WEEK_START'].max()

    # Date range selection
    date_range = st.date_input(
        "Select Date Range:",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date(),
        key='weekly_date_range'
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Filter data based on selected date range
        df_filter = weekly_agg[(weekly_agg['WEEK_START'] >= start_date) & (weekly_agg['WEEK_START'] <= end_date)]

        st.line_chart(df_filter, x = 'WEEK_START', y = ['AVG_10Y', 'AVG_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        
        st.line_chart(df_filter, x = 'WEEK_START', y = ['AVG_10Y', 'MIN_10Y', 'MAX_10Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        
        st.line_chart(df_filter, x = 'WEEK_START', y = ['AVG_2Y', 'MIN_2Y', 'MAX_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        

        st.line_chart(df_filter, x = 'WEEK_START', y = ['AVG_10Y', 'AVG_2Y', 
                                                       'MIN_10Y', 'MAX_10Y',
                                                       'MIN_2Y', 'MAX_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        

        data = df_filter[['WEEK_START', 'AVG_SPREAD']]

        data['Positive Change'] = data['AVG_SPREAD'].apply(lambda x: x if x >= 0 else None)
        data['Negative Change'] = data['AVG_SPREAD'].apply(lambda x: x if x < 0 else None)

        st.line_chart(
            data.set_index('WEEK_START')[['Positive Change', 'Negative Change']], 
            use_container_width=True
        )

    else:
        st.warning("Please select a valid date range.")

def monthly_data_analytics(monthly_agg):
    st.subheader('Monthly Yield Spread Analytics')

    # Ensure MONTH_START is in datetime format
    monthly_agg['MONTH_START'] = pd.to_datetime(monthly_agg['MONTH_START'])

    # Get min and max date
    min_date = monthly_agg['MONTH_START'].min()
    max_date = monthly_agg['MONTH_START'].max()

    # Date range selection
    date_range = st.date_input(
        "Select Date Range:",
        [min_date.date(), max_date.date()],
        min_value=min_date.date(),
        max_value=max_date.date(),
        key='monthly_date_range'
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Filter data based on selected date range
        df_filter = monthly_agg[(monthly_agg['MONTH_START'] >= start_date) & (monthly_agg['MONTH_START'] <= end_date)]

        st.line_chart(df_filter, x = 'MONTH_START', y = ['AVG_10Y', 'AVG_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        
        st.line_chart(df_filter, x = 'MONTH_START', y = ['AVG_10Y', 'MIN_10Y', 'MAX_10Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        
        st.line_chart(df_filter, x = 'MONTH_START', y = ['AVG_2Y', 'MIN_2Y', 'MAX_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        

        st.line_chart(df_filter, x = 'MONTH_START', y = ['AVG_10Y', 'AVG_2Y', 
                                                       'MIN_10Y', 'MAX_10Y',
                                                       'MIN_2Y', 'MAX_2Y'],
                      height = 500,
                      x_label= "Financial Period",
                      y_label= "Yield")        

        data = df_filter[['MONTH_START', 'AVG_SPREAD']]

        data['Positive Change'] = data['AVG_SPREAD'].apply(lambda x: x if x >= 0 else None)
        data['Negative Change'] = data['AVG_SPREAD'].apply(lambda x: x if x < 0 else None)

        st.line_chart(
            data.set_index('MONTH_START')[['Positive Change', 'Negative Change']], 
            use_container_width=True
        )

    else:
        st.warning("Please select a valid date range.")


def main():
    # Set the title of the app
    st.title("FRED Yield Curve Analysis")
    st.header("Analysis of the Yield Curve Spread between 10Y and 2Y Treasury Bonds")
    
    # Establish Snowflake session
    conn = create_snowflake_connection()
            
    # Load data
    daily_agg, weekly_agg, monthly_agg = load_data(conn)

    # Select period
    period = st.selectbox("Select Period", ["DAILY", "WEEKLY", "MONTHLY"])

    if period == "DAILY":
        daily_data_analytics(daily_agg)
    elif period == "WEEKLY":
        weekly_data_analytics(weekly_agg)
    elif period == "MONTHLY":
        monthly_data_analytics(monthly_agg)

if __name__ == "__main__":
# Set page configuration
    st.set_page_config(
        page_title="FRED - Snowflake Data Pipeline",  # Name of the app
        layout="wide",              # Layout: "centered" or "wide"
        initial_sidebar_state="expanded"  # Sidebar: "expanded" or "collapsed"
    )    
    main()

