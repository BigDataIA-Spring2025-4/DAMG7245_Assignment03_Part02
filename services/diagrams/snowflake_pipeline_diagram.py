from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.aws.storage import S3
from diagrams.saas.analytics import Snowflake  # Correct import for Snowflake
from diagrams.programming.flowchart import PredefinedProcess  # Use for GitHub Actions

# Create the diagram
with Diagram("Snowflake Data Pipeline", show=True, direction="LR"):
    # GitHub Actions for scheduling
    github_actions = PredefinedProcess("GitHub Actions (Scheduler)")

    # Fred Website Cluster (Frontend)
    with Cluster("Fred Website"):
        frontend = Custom("Fred Website", "./services/diagrams/src/fred-logo.png")
        
    # AWS S3 Bucket for storing data
    with Cluster("AWS"):
        s3_bucket = S3("AWS S3 Bucket")

    # Snowflake environment
    with Cluster("Snowflake"):
        raw_table = Snowflake("Raw Table")
        harmonized_table = Snowflake("Harmonized Table")
        snowflake_task = Snowflake("Snowflake Task (ETL Processing)")
        analytics_table = Snowflake("Analytics Table")

    # Streamlit Dashboard Cluster (Frontend)
    with Cluster("Frontend (Streamlit)") as frontend_cluster:
        streamlit_app = Custom("Streamlit UI", "./services/diagrams/src/streamlit.png")

    # Data pipeline flow
    github_actions >> Edge(label="Extract FRED API Data (Daily)") >> s3_bucket
    s3_bucket >> Edge(label="Load to Raw Tables") >> raw_table
    raw_table >> Edge(label="Transform to Harmonized Schema") >> harmonized_table
    harmonized_table >> Edge(label="Trigger Snowflake Task") >> snowflake_task
    snowflake_task >> Edge(label="Load to Analytics") >> analytics_table
    analytics_table >> Edge(label="Visualize Data") >> streamlit_app

