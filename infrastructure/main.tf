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

resource "google_sql_database" "epic_clarity_db" {
  name     = "epic_clarity_db"
  instance = google_sql_database_instance.mysql_instance.name
}

resource "google_sql_database" "cerner_millennium_db" {
  name     = "cerner_millennium_db"
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
    database = google_sql_database.epic_clarity_db.name
  })
}

# 5. BigQuery Temp Dataset
resource "google_bigquery_dataset" "temp_dataset" {
  dataset_id  = "temp_dataset"
  description = "Temporary dataset for PySpark auditing and intermediate tables"
  location    = "US"
  depends_on  = [google_project_service.gcp_services]
}

# 6. Pub/Sub for Streaming (Domain-Specific Topics)
# Enterprise Architecture: Each clinical domain has its own topic
# for independent scaling, monitoring, and error handling.

# 6a. ADT Topic (Admit/Discharge/Transfer - HL7v2 ADT equivalent)
resource "google_pubsub_topic" "topic_adt" {
  name       = "carenet-rcm-topic-adt"
  depends_on = [google_project_service.gcp_services]
}

resource "google_pubsub_subscription" "sub_adt" {
  name  = "carenet-rcm-sub-adt"
  topic = google_pubsub_topic.topic_adt.name

  # Enable message ordering for FIFO guarantees per patient
  enable_message_ordering = true

  # Retry policy for transient failures
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Dead letter policy: after 5 failed delivery attempts, send to DLQ topic
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.topic_dlq.id
    max_delivery_attempts = 5
  }
}

# 6b. Claims Topic (837/835 transaction equivalent)
resource "google_pubsub_topic" "topic_claims" {
  name       = "carenet-rcm-topic-claims"
  depends_on = [google_project_service.gcp_services]
}

resource "google_pubsub_subscription" "sub_claims" {
  name  = "carenet-rcm-sub-claims"
  topic = google_pubsub_topic.topic_claims.name

  enable_message_ordering = true

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.topic_dlq.id
    max_delivery_attempts = 5
  }
}

# 6c. Orders Topic (ORM/ORU - Lab/Pharmacy Orders)
resource "google_pubsub_topic" "topic_orders" {
  name       = "carenet-rcm-topic-orders"
  depends_on = [google_project_service.gcp_services]
}

resource "google_pubsub_subscription" "sub_orders" {
  name  = "carenet-rcm-sub-orders"
  topic = google_pubsub_topic.topic_orders.name

  enable_message_ordering = true

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.topic_dlq.id
    max_delivery_attempts = 5
  }
}

# 6d. Infrastructure-Level Dead Letter Queue Topic
# Messages that fail delivery after max_delivery_attempts land here
# for manual inspection and replay by the Data Engineering team.
resource "google_pubsub_topic" "topic_dlq" {
  name       = "carenet-rcm-topic-dead-letter-queue"
  depends_on = [google_project_service.gcp_services]
}

resource "google_pubsub_subscription" "sub_dlq" {
  name  = "carenet-rcm-sub-dead-letter-queue"
  topic = google_pubsub_topic.topic_dlq.name
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
