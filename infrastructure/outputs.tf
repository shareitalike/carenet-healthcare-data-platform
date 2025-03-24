output "gcs_bucket_name" {
  description = "The name of the GCS Data Lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "mysql_public_ip" {
  description = "The Public IP of the Cloud SQL MySQL instance"
  value       = google_sql_database_instance.mysql_instance.public_ip_address
}

output "composer_dags_folder" {
  description = "The GCS URI for the Composer DAGs folder"
  value       = google_composer_environment.airflow.config[0].dag_gcs_prefix
}
