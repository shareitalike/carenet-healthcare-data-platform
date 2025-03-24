variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources to"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The globally unique name for the GCS data lake bucket"
  type        = string
}

variable "db_password" {
  description = "The password for the MySQL database user"
  type        = string
  sensitive   = true
}
