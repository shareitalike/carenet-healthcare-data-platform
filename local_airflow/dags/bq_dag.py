import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Fetch Dynamic Configs
PROJECT_ID = os.environ.get("GCP_PROJECT", "carenet-rcm-data-platform")
COMPOSER_BUCKET = Variable.get("gcs_bucket", "carenet-rcm-data-bucket")
LOCATION = "US"

# In Composer, DAG folder lies under /home/airflow/gcs/. Locally, it is in include/
if os.path.exists("/home/airflow/gcs/data/BQ/bronze.sql"):
    SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
    SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
    SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"
else:
    SQL_FILE_PATH_1 = "/usr/local/airflow/include/BQ/bronze.sql"
    SQL_FILE_PATH_2 = "/usr/local/airflow/include/BQ/silver.sql"
    SQL_FILE_PATH_3 = "/usr/local/airflow/include/BQ/gold.sql"

# Read SQL query from file and inject runtime configurations
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        query = file.read()
    # Dynamically replace project and bucket strings
    query = query.replace("avd-databricks-demo", PROJECT_ID)
    query = query.replace("healthcare-bucket-22032025", COMPOSER_BUCKET)
    return query

# Fetch processed queries
BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
ARGS = {
    "owner": "data-engineering-team",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["de-alerts@carenethealth.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule=None,
    description="Orchestrates BigQuery ELT transformations from Bronze to Silver and Gold layers",
    default_args=ARGS,
    catchup=False,
    tags=["gcs", "bq", "elt", "production"]
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables
