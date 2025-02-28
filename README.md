# FRED (Federal Reserve Economic Data) - Snowflake Pipelines 

### Project Overview :
This repository contains the implementation of a robust data engineering pipeline designed to process financial data from the Federal Reserve Economic Data (FRED) platform, specifically focusing on U.S. Treasury yields for 10-Year and 2-Year bonds. Leveraging Snowflake's Snowpark for Python, the project provides an efficient system for extracting, transforming, and validating financial data to enable advanced analysis and reporting.


### Team Members :
- Vedant Mane
- Abhinav Gangurde
- Yohan Markose

### Resources : 
- **Streamlit Application** : [Streamlit App]()

- **Google Codelab**: [Codelab]()

- **Google Docs**: [Project Document]()

- **Video Walkthrough**: [Video]()

### Technologies Used :
- **Streamlit** : Frontend Visual Dataset for analytics
- **AWS S3**: External Cloud Storage
- **Cloud & Storage**: Snowflake, AWS S3
- **ELT & Pipeline**: Snowflake Tasks
- **Snowflake** : Snowpark, UDF, Stored Procedures, Streams, Notebook

### Architecture Diagram :



### Workflow :

1. **Initial Account Creation and Setup** - 
To get started with the project, you need to set up the necessary accounts and configurations for both Snowflake and FRED. These accounts form the foundation for creating the data pipelines, storing data, and accessing the required datasets.

- **Snowflake** : The pipelines heavily rely on Snowflake for data storage, transformation, and orchestration.
- **FRED** : The project uses publicly available APIs from FRED (Federal Reserve Economic Data) to fetch U.S. Treasury yield data. To access these APIs, you need an API key from FRED.

2.  **Automation of data extraction** -  
Github's actions for scheduling the data extraction daily and loading data into secondary data storage in this case AWS S3 bucket.

3. **Snowflake Account Setup** - 
Upload the provided FRED_0_START.ipynb from the git in your snowflake account and run for setting up with required database level objects.

4. **Deploy Notebooks** -
Deploy the external git notebook elements in the snowflake and use the for data processing and updation in created snowflake dags.

5. **Schedule the Snowflake DAGs** -
Run the created snowflake tasks and observed the run for the data processing.

6. **Streamlit**
The processed data is observable in the rendered streamlit application

For detailed guide and steps to run the data pipelines following through the offical quickstarter guide. -
[FRED (Federal Reserve Economic Data) - Snowflake Data Pipelines Quickstarter Guide](https://docs.google.com/document/d/1jTG4u1Wsswd29oEoYj2Cy0oAIexVLM-iuCtUTEH-1QU/edit?tab=t.0) 

### Attestation :
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK