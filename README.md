# CareNet Healthcare RCM Data Platform

An enterprise-grade, HIPAA-compliant batch and real-time streaming data platform built on Google Cloud Platform (GCP). This platform ingests, transforms, and analyzes clinical events from disparate EMR systems (Epic Clarity and Cerner Millennium) to deliver real-time operational insights and Revenue Cycle Management (RCM) analytics.

This repository serves as a showcase of senior-level data engineering architectural designs, production hardening strategies, and business-focused KPI reporting for the US Healthcare domain.

---

## 1. Business Scenario & Objectives
**CareNet Health Systems** (a regional network with 3 hospital campuses and 12 outpatient clinics, 1,800 total beds) experienced an M&A that left their data siloed. The clinical EMR data lived in two separate systems (Epic and Cerner), leading to a lack of unified reporting. Furthermore, the hospital faced an 18% claim denial rate (well above the 5-7% industry benchmark) and delayed patient flow in the ED due to batch-only reporting.

### Business Value Delivered:
* **Real-Time ADT Census**: Streaming patient flow (Admit, Discharge, Transfer) to the ED Charge Nurses with a <5 minute lag.
* **Claim Denial Analysis**: Granular analysis of claims rejections by payer, physician NPI, and ICD-10 diagnosis code to prevent future denials, identifying millions in recoverable revenue.
* **Length of Stay (LOS) & Charge Summaries**: Automated clinical and financial metrics aggregated in the Gold layer.

---

## 2. Architecture & Data Flow
The platform is designed using a decoupled, hybrid Medallion (Bronze, Silver, Gold) architecture with parallel Batch and Streaming paths.

### 🔄 Batch Pipeline (Nightly ETL)
```text
Epic Clarity DB ────┐ (High-Watermark JDBC)
                    ├──► [ PySpark on Dataproc ] ──► [ GCS Landing (Parquet) ]
Cerner Millennium ──┘
                                                            │
                                                            ▼
                                                 [ BQ Bronze (External Tables) ]
                                                            │
                                                            ▼
                                                 [ BQ Silver (SCD Type 2 MERGE) ]
                                                            │
                                                            ▼
                                                 [ BQ Gold (Star Schema / Views) ]
```

### ⚡ Streaming Pipeline (Real-Time Clinical Events)
```text
Epic EMR ──► [ MLLP Gateway ] ──► [ Pub/Sub Topics ] (ADT, Claims, Orders)
                                            │
                                            ▼
                                  [ Dataflow (Apache Beam) ]
                                   - Parse & Validate
                                   - 5-min Tumbling Window
                                   - PHI Hashing
                                            │
                                 ┌──────────┴──────────┐
                                 ▼                     ▼
                       [ BQ Bronze Streaming ]    [ BQ Dead Letter Queue (DLQ) ]
```

---

## 3. Production Hardening Features

* **Distributed PySpark Parquet Writer**: Resolved initial driver Out-Of-Memory (OOM) crashes by replacing Pandas conversion with native PySpark Parquet chunking, reducing storage and query costs by ~65%.
* **Dead Letter Queue (DLQ)**: In healthcare, dropping events is unacceptable. The streaming pipeline validates all payloads and routes malformed events (missing patient IDs, negative charges) to a BigQuery DLQ for replay.
* **Pub/Sub Message Ordering**: Utilizes ordering keys (`patient_id`) to ensure chronological delivery of ADT events (Admit → Transfer → Discharge), preventing corrupt patient census state.
* **Schema Drift Detection**: PySpark compares incoming JDBC schemas against a JSON-based schema registry stored in GCS. New columns trigger warnings and automatic registry updates; missing columns are backfilled with NULLs.
* **HIPAA Compliance & Security**:
    * **3-Layer Defense**: PHI (names, addresses) is SHA-256 hashed at the streaming producer, SSNs are hashed in the Silver layer, and fallback hashing exists in the consumer.
    * **Access Control**: BigQuery Column-Level Policy Tags enforce dynamic data masking on sensitive patient fields.
    * **Auditability**: Automated Cloud Audit Logs combined with a custom pipeline audit table track exactly who and what touched the data.

---

## 4. Repository Structure

* [workflows/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/workflows/): Production Apache Airflow DAGs built for deployment to **GCP Cloud Composer**. Orchestrates Dataproc creation, PySpark runs, and BigQuery SQL runs.
* [local_airflow/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/local_airflow/): Dockerized local Airflow environment used exclusively for zero-cost local DAG development and testing before pushing to Cloud Composer.
* [data/INGESTION/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/INGESTION/): PySpark batch extraction scripts (`epic_clarity_to_landing.py`, `cerner_millennium_to_landing.py`) and reference datasets (ICD-10, CPT, NPI).
* [data/STREAMING/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/STREAMING/): Streaming event generator (`streaming_producer.py`) and consumers (`dataflow_streaming_consumer.py`, `pyspark_streaming_consumer.py`).
* [data/BQ/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/BQ/): BigQuery SQL transformations for Bronze, Silver, Gold layers, and real-time streaming views.
* [infrastructure/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/infrastructure/): Terraform IaC configurations provisioning all GCP resources (GCS, BigQuery, Pub/Sub, Service Accounts).
* [data/configs/](file:///f:/pyspark_study/project_hospital/Project_hospital_Prod/data/configs/): Metadata-driven pipeline configuration files (`load_config.csv`) for onboarding new tables without code changes.
