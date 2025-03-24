# Terraform Deployment Guide for CareNet RCM Platform

This project includes **Infrastructure as Code (IaC)** using Terraform to automate the provisioning of all Google Cloud Platform (GCP) resources required for the CareNet Revenue Cycle Management (RCM) Data Platform.

This demonstrates enterprise-grade DevOps and automated deployment practices.

---

## 🏗️ Architecture Provisioned via Terraform
The Terraform scripts in the `infrastructure/` directory will automatically create:
1. **Cloud Storage (GCS)**: Creates the primary data lake bucket and initializes the folder structure (`landing/`, `configs/`, `temp/`).
2. **Cloud SQL (MySQL)**: Provisions a MySQL 8.0 instance and creates the `hospital_a_db` and `hospital_b_db` databases to simulate EMR sources.
3. **Secret Manager**: Creates the secret to securely store database credentials.
4. **Cloud Composer (Airflow)**: Provisions a Composer 2 environment for orchestration.
5. **BigQuery**: Creates the `temp_dataset` (other datasets are dynamically created by the DAGs).
6. **Cloud Pub/Sub**: Provisions the real-time topic (`carenet-rcm-transactions-topic`) and subscription for the streaming pipeline.

---

## 🚀 How to Deploy Using Terraform

### Prerequisites
1. Install [Terraform](https://developer.hashicorp.com/terraform/downloads).
2. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).
3. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   gcloud config set project carenet-rcm-data-platform
   ```
4. Enable the required Resource Manager API (needed by Terraform):
   ```bash
   gcloud services enable cloudresourcemanager.googleapis.com
   ```

### Step 1: Initialize Terraform
Navigate to the `infrastructure` directory and initialize the Terraform workspace:
```bash
cd infrastructure
terraform init
```

### Step 2: Configure Variables
Create a file named `terraform.tfvars` in the `infrastructure/` directory. (This file is ignored by Git to keep your secrets safe).
```hcl
project_id      = "carenet-rcm-data-platform"
region          = "us-central1"
bucket_name     = "carenet-rcm-data-bucket-99" # Must be globally unique
db_password     = "SuperSecretPassword123!"    # Cloud SQL root password
```

### Step 3: Plan the Deployment
Run the plan command to see exactly what Terraform will create:
```bash
terraform plan
```
*Review the output to ensure 13+ resources will be created.*

### Step 4: Apply the Infrastructure
Deploy the resources to GCP (this step takes about 20-25 minutes, mostly due to Cloud Composer and Cloud SQL provisioning):
```bash
terraform apply
```
Type `yes` when prompted.

### Step 5: Post-Deployment Steps
Once Terraform finishes, it will output the GCS bucket URI for your Composer environment (e.g., `gs://us-central1-carenet-compos-xxxx-bucket/dags`).
1. **Upload DAGs & Data**: Sync your `workflows/` and `data/` directories to the Composer bucket.
2. **Set Airflow Variables**: Go to the Airflow UI and add `gcs_bucket` = `<your-bucket-name>`.
3. **Seed Database**: Connect to the Cloud SQL instance and run the DDL/DML script from the manual deployment guide.

---

## 🧹 Teardown (Clean Up)
To avoid incurring unnecessary GCP charges, you can destroy all provisioned resources with a single command when you are done testing:
```bash
terraform destroy
```
Type `yes` when prompted.
