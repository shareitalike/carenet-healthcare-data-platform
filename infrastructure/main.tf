terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. Enable Required APIs
resource "google_project_service" "gcp_services" {
  for_each = toset([
    "compute.googleapis.com",
    "secretmanager.googleapis.com",
    "dataproc.googleapis.com",
    "composer.googleapis.com",
    "bigquery.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 2. GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  depends_on    = [google_project_service.gcp_services]
}

# Create folder structure in GCS
resource "google_storage_bucket_object" "folders" {
  for_each = toset([
    "landing/",
    "landing/claims/",
    "landing/cptcodes/",
    "configs/",
    "temp/"
  ])
  name    = each.key
  content = " "
  bucket  = google_storage_bucket.data_lake.name
}

# 3. Cloud SQL (MySQL) for EMR Simulation
resource "google_sql_database_instance" "mysql_instance" {
  name             = "carenet-mysql-instance"
  database_version = "MYSQL_8_0"
  region           = var.region
  depends_on       = [google_project_service.gcp_services]

  settings {
    tier = "db-f1-micro"
    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "all"
        value = "0.0.0.0/0" # For testing/Dataproc access
      }
    }
  }
  deletion_protection = false
}

resource "google_sql_database" "hospital_a_db" {
  name     = "hospital_a_db"
  instance = google_sql_database_instance.mysql_instance.name
}

resource "google_sql_database" "hospital_b_db" {
  name     = "hospital_b_db"
  instance = google_sql_database_instance.mysql_instance.name
}

resource "google_sql_user" "db_user" {
  name     = "myuser"
  instance = google_sql_database_instance.mysql_instance.name
  password = var.db_password
}

# 4. Secret Manager for DB Credentials
resource "google_secret_manager_secret" "db_secret" {
  secret_id = "mysql-emr-credentials"
  replication {
    automatic = true
  }
  depends_on = [google_project_service.gcp_services]
}

resource "google_secret_manager_secret_version" "db_secret_version" {
  secret      = google_secret_manager_secret.db_secret.id
  secret_data = jsonencode({
    username = google_sql_user.db_user.name
    password = google_sql_user.db_user.password
    host     = google_sql_database_instance.mysql_instance.public_ip_address
    database = google_sql_database.hospital_a_db.name
  })
}

# 5. BigQuery Temp Dataset
resource "google_bigquery_dataset" "temp_dataset" {
  dataset_id  = "temp_dataset"
  description = "Temporary dataset for PySpark auditing and intermediate tables"
  location    = "US"
  depends_on  = [google_project_service.gcp_services]
}

# 6. Pub/Sub for Streaming
resource "google_pubsub_topic" "transactions_topic" {
  name       = "carenet-rcm-transactions-topic"
  depends_on = [google_project_service.gcp_services]
}

resource "google_pubsub_subscription" "transactions_sub" {
  name  = "carenet-rcm-transactions-sub"
  topic = google_pubsub_topic.transactions_topic.name
}

# 7. Cloud Composer (Airflow)
resource "google_composer_environment" "airflow" {
  name   = "carenet-composer"
  region = var.region
  depends_on = [google_project_service.gcp_services]
  
  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }
}
