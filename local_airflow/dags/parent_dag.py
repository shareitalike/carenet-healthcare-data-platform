from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

# Define the parent DAG
with DAG(
    dag_id="parent_dag",
    schedule="0 5 * * *", # Run daily at 05:00 UTC
    description="Parent orchestrator to trigger PySpark ingestion and BigQuery transformation DAGs sequentially",
    default_args=ARGS,
    catchup=False,
    tags=["parent", "orchestration", "production"]
) as dag:

    # Task to trigger PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
    )

    # Task to trigger BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
    )

# Define dependencies
trigger_pyspark_dag >> trigger_bigquery_dag