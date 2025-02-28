from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.aws.storage import S3
from diagrams.saas.analytics import Snowflake  # Correct import for Snowflake
from diagrams.programming.flowchart import PredefinedProcess  # Use for GitHub Actions

# Create the diagram
with Diagram("Snowflake Data Pipeline", show=True, direction="LR"):
    # GitHub Actions for scheduling
    github_actions = Custom("GitHub Actions (Scheduler)", "./services/diagrams/src/github-actions.png")

    # Fred Website Cluster (Frontend)
    with Cluster("Fred Website"):
        fred = Custom("Fred Website", "./services/diagrams/src/fred-logo.png")
        
    # AWS S3 Bucket for storing data
    with Cluster("AWS"):
        s3_bucket = S3("AWS S3 Bucket")

    # Snowflake environment
    with Cluster("Snowflake", direction="TB"):
        raw_table = Snowflake("Raw Table")
        harmonized_table = Snowflake("Harmonized Table")
        snowflake_task = Snowflake("Snowflake Task \n(ETL Processing)")
        analytics_table = Snowflake("Analytics Table")

    # Streamlit Dashboard Cluster (Frontend)
    with Cluster("Frontend (Streamlit)") as frontend_cluster:
        streamlit_app = Custom("Streamlit UI", "./services/diagrams/src/streamlit.png")

    # Data pipeline flow
    fred >> Edge(label="Extract Data from Fred API")>> github_actions 
    github_actions >> Edge(label="Stage Data in S3") >> s3_bucket
    s3_bucket >> Edge(label="Load Raw Tables") >> raw_table
    raw_table >> Edge(label="Harmonized Schema") >> harmonized_table
    analytics_table >> Edge(label="Orchestrate Jobs") >> snowflake_task
    harmonized_table >> Edge(label="Load to Analytics") >> analytics_table
    analytics_table >> streamlit_app
    streamlit_app >> analytics_table

