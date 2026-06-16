# CareNet Healthcare Revenue Cycle Management (RCM) Data Platform

An enterprise-grade, HIPAA-compliant batch and real-time streaming data platform built on Google Cloud Platform (GCP) to ingest, transform, and analyze healthcare EMR systems and payer claims responses.

This repository serves as a showcase of senior-level data engineering architectural designs, production hardening strategies, and business-focused KPI reporting for Healthcare Revenue Cycle Management (RCM).

---

## 1. Business Scenario & Objectives
**CareNet Health Systems** (a regional network with 2 major hospitals and 15 clinics) was experiencing substantial revenue leakage due to high claim denial rates and delayed payer reimbursements. The clinical EMR data (MySQL databases) and payer claim responses (FTP CSV logs) were siloed in completely separate systems.

### Business Metrics Delivered:
* **Clean Claim Rate (CCR)**: Tracks billing accuracy and documentation quality to ensure claims are paid on first submission.
* **Days in Accounts Receivable (Days in A/R)**: Measures payment latency, helping RCM teams collect payment faster.
* **Claim Denial Analysis**: Allows granular analysis of claims rejections by physician NPI, department, and ICD-10 diagnosis code to prevent future denials.

---

## 2. Architecture & Data Flow
The platform is designed using a decoupled, hybrid Medallion (Bronze, Silver, Gold) architecture:

```
[ Ingestion Layer ]                 [ Data Lake / DWH ]                 [ BI Reporting ]
EMR Cloud SQL (MySQL) ──┐
Provider/Dept APIs ─────┼──► [ PySpark / Dataproc ] ──► [ GCS Landing ] 
claims.csv (FTP Payer) ─┘                                     │
                                                              ▼
                                                    [ BQ Bronze (External) ]
                                                              │
                                                              ▼
                                                    [ BQ Silver (SCD Type 2) ]
                                                              │
                                                              ▼
                                                    [ BQ Gold (Star Schema) ] ──► [ Looker Studio ]
```

### Technical Component Summary:
* **Orchestration**: **Cloud Composer (Apache Airflow)** triggers sequential dependency flows.
* **Compute**: Ephemeral **Dataproc (PySpark)** clusters are created/deleted dynamically to ingest batch feeds, utilising **Preemptible/Spot VMs** to reduce compute budgets.
* **Streaming Extension**: **Google Cloud Pub/Sub** and **GCP Dataflow (Apache Beam)** process live transactional check-ins and claims logs in real-time, providing sub-second denial alerts.
* **Warehouse**: **BigQuery** stores Bronze, Silver, and Gold datasets, optimized with table partitioning (by date) and clustering (by source and payer).

---

## 3. Production Hardening Features

* **Secrets Isolation**: Database and API credentials are dynamically fetched at runtime from **GCP Secret Manager**, removing passwords and keys from the source code.
* **Data Quality & Quarantine Gates**: The Silver SQL layer checks incoming rows for critical constraints, writing invalid rows to a quarantine dataset while clean data continues downstream.
* **Schema Drift Handling**: PySpark compares incoming JDBC schemas against a JSON-based schema registry stored in GCS. New columns trigger notifications and update the registry automatically without failing the job, while BigQuery uses dynamic `ALTER TABLE` DDLs to absorb the drift.
* **HIPAA Compliance & Governance**: BigQuery **Column-Level Policy Tags** enforce dynamic data masking on sensitive patient fields (e.g., SSN masked as `XXX-XX-XXXX`), while Cloud Audit Logs are exported to a locked down GCS bucket for compliance audits.

---

## 4. Repository Structure

* [workflows/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/workflows/): Production Apache Airflow DAGs built for deployment to **GCP Cloud Composer**. Orchestrates Dataproc creation, PySpark runs, and BigQuery SQL runs.
* [local_airflow/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/local_airflow/): Dockerized local Airflow environment used exclusively for zero-cost local DAG development and testing before pushing to Cloud Composer.
* [data/INGESTION/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/INGESTION/): PySpark batch extraction scripts for EMR and ICD/CPT reference datasets.
* [data/STREAMING/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/STREAMING/): Streaming event generator and consumers (in both **Spark Structured Streaming** and **Apache Beam/Dataflow**).
* [data/BQ/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/BQ/): BigQuery SQL transformations for Bronze/Silver/Gold layouts, and real-time streaming views.
