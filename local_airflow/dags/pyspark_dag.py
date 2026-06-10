import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# Fetch Dynamic Configs
PROJECT_ID = os.environ.get("GCP_PROJECT", "carenet-rcm-data-platform")
REGION = "us-central1"
# Cluster name must be lowercase and unique per day to avoid reuse conflicts
CLUSTER_NAME = "carenet-dataproc-cluster-{{ ds_nodash }}"
COMPOSER_BUCKET = Variable.get("gcs_bucket", "carenet-rcm-data-bucket")

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/epic_clarity_to_landing.py"
GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cerner_millennium_to_landing.py"
GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"

# Define PySpark Job Configs with Dynamic Arguments
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "args": [
            "--bucket", COMPOSER_BUCKET,
            "--project_id", PROJECT_ID,
            "--execution_date", "{{ ds }}"
        ]
    },
}

PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_2,
        "args": [
            "--bucket", COMPOSER_BUCKET,
            "--project_id", PROJECT_ID,
            "--execution_date", "{{ ds }}"
        ]
    },
}

PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_3,
        "args": [
            "--bucket", COMPOSER_BUCKET,
            "--project_id", PROJECT_ID
        ]
    },
}

PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_4,
        "args": [
            "--bucket", COMPOSER_BUCKET,
            "--project_id", PROJECT_ID
        ]
    },
}

# Cluster Config for Ephemeral Sizing & Preemptible/Spot workers
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-1",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-1",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "secondary_worker_config": {
        "num_instances": 0,
        "machine_type_uri": "n1-standard-1",
        "is_preemptible": True,
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "gce_cluster_config": {
        "zone_uri": "us-central1-a"
    },
    "software_config": {
        "properties": {
            "dataproc:pip.packages": "google-cloud-secret-manager==2.22.0"
        }
    }
}

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

with DAG(
    dag_id="pyspark_dag",
    schedule=None,
    description="Orchestrates ephemeral Dataproc cluster creation, parallel PySpark ingestion, and cluster termination",
    default_args=ARGS,
    catchup=False,
    tags=["dataproc", "pyspark", "ingestion", "production"]
) as dag:
    
    # Task 1: Create ephemeral cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # Ingestion Tasks
    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="extract_hospital_a", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="extract_hospital_b", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="extract_claims", 
        job=PYSPARK_JOB_3, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id="extract_cpt_codes", 
        job=PYSPARK_JOB_4, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # Task 6: Delete cluster (even if jobs fail)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done" # Trigger cluster deletion even if ingestion jobs fail to save costs
    )

# Execution Flow
create_cluster >> [pyspark_task_1, pyspark_task_2, pyspark_task_3, pyspark_task_4] >> delete_cluster