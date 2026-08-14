# CareNet Healthcare RCM & Clinical Data Platform

An enterprise-grade, HIPAA-compliant batch and real-time streaming data platform built on Google Cloud Platform (GCP). This platform ingests, transforms, and analyzes clinical and financial events from disparate EMR systems (Epic Clarity and Cerner Millennium) to deliver real-time operational insights and Revenue Cycle Management (RCM) analytics.

This repository serves as a showcase of senior-level data engineering architectural designs, production hardening strategies, SCD Type 2 lifecycle tracking, and 15 enterprise-grade Gold Data Marts for Looker BI reporting.

---

## 1. Business Scenario & Objectives
**CareNet Health Systems** (a regional network with 3 hospital campuses, 12 outpatient clinics, and 1,800 total beds) experienced an M&A that left their clinical and financial data siloed. The clinical EMR data lived in two separate systems (Epic Clarity and Cerner Millennium), leading to delayed, fragmented financial reporting. Furthermore, the hospital faced an 18% claim denial rate (well above the 5-7% industry benchmark) and an unacceptably long 3-week monthly claim reconciliation cycle.

### Business Value Delivered:
* **Unified Revenue Cycle Analytics**: Automated ingestion and standardization of professional billing (`ARPB_TRANSACTIONS`) and EDI 835 payer remittances across all hospitals.
* **Claim Denial Root-Cause Audit**: Granular analysis of claim rejections categorized by EDI 835 CARC reason codes (e.g., Code 16 missing info, Code 45 contractual adjustments, Code 96 non-covered services), recovering millions in lost revenue.
* **15 Curated Gold Data Marts**: Pre-aggregated, One Big Table (OBT) models providing sub-second Looker dashboards for C-Suite executives (CFO Net Collection Rates, CMO Physician Productivity, Quality 30-Day Readmission Risk).
* **Real-Time ADT Census**: Streaming patient flow (Admit, Discharge, Transfer) to ED Charge Nurses with a <5 minute lag.

---

## 2. Architecture & Data Flow
The platform is designed using a decoupled, hybrid Medallion (Bronze, Silver, Gold) architecture with parallel Batch and Streaming paths.

### 🔄 Batch Pipeline (Nightly ELT & Ingestion)
```text
Epic Clarity DB ────┐ (Config-Driven JDBC & Watermarking)
                    ├──► [ Ephemeral Dataproc (PySpark) ] ──► [ GCS Landing (Parquet) ]
Cerner Millennium ──┘        - In-Flight PHI Masking                     │
                             - unionByName Drift Handling                ▼
                                                              [ BQ Bronze (External Tables) ]
                                                                         │ (Schema-on-Read)
                                                                         ▼
                                                              [ BQ Silver (SCD Type 2 MERGE) ]
                                                                 - Staging Quality Gates
                                                                 - Quarantine Routing
                                                                 - EDI 835 Adjudication
                                                                         │
                                                                         ▼
                                                              [ BQ Gold (15 Curated Data Marts) ]
                                                                 - One Big Table (OBT) Models
                                                                 - Partitioned & Clustered
                                                                 - BigQuery BI Engine Cache
                                                                         │
                                                                         ▼
                                                              [ Looker Studio / C-Suite Dashboards ]
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

## 3. Production Hardening & Architectural Highlights

* **Config-Driven Ingestion Framework**: Solved heterogeneous watermark date columns across Epic Clarity (`PATIENT.UPDATE_DATE`, `PAT_ENC.CONTACT_DATE`, `ARPB_TRANSACTIONS.POST_DATE`) via a centralized `load_config.csv` and BigQuery audit state table.
* **Schema Drift Resilience**: Utilizes PySpark `unionByName(allowMissingColumns=True)` to dynamically absorb asymmetrical schemas between Epic and Cerner, backfilling missing fields with NULLs without breaking nightly DAG runs.
* **SCD Type 2 Historical Tracking**: Implemented atomic BigQuery `MERGE` statements in the Silver layer to maintain full historical audit trails for changing patient demographics and claim settlement statuses.
* **In-Pipeline Data Quality Quarantine**: Staging queries validate primary keys and business constraints, automatically routing corrupt records (`is_quarantined = TRUE`) to `carenet_quarantine` with automated Slack alerts while allowing clean records to proceed.
* **BigQuery Performance & BI Engine Optimization**:
    * **Time-Partitioning**: `PARTITION BY DATE(ServiceDate)` to eliminate full-table scans.
    * **Clustering**: `CLUSTER BY PayorID, Specialization, datasource` for automatic block-pruning.
    * **BI Engine**: Provisioned 50 GB of in-memory acceleration on `gold_dataset`, reducing query latencies to <800ms and cutting daily query costs by 76%.
* **HIPAA Compliance & Zero-Trust Security**:
    * **In-Flight Masking**: Patient identifiers are SHA-256 hashed in memory in PySpark before landing in GCS.
    * **BigQuery Policy Tags**: Data Catalog taxonomy enforces Column-Level Access Control on PHI fields (`FirstName`, `LastName`, `DOB`, `SSN`), restricting plaintext views to authorized `Fine-Grained Reader` compliance roles.
    * **Cloud Secret Manager**: Zero hardcoded credentials in codebase; Dataproc dynamically retrieves encrypted JDBC tokens at runtime.

---

## 4. Repository Structure

* [`workflows/`](workflows/): Production Apache Airflow DAGs for **GCP Cloud Composer** (`pyspark_dag.py` for ephemeral Dataproc lifecycle, `bq_dag.py` for BigQuery ELT sequence).
* [`local_airflow/`](local_airflow/): Local Airflow environment for local DAG development and validation.
* [`data/INGESTION/`](data/INGESTION/): PySpark batch extraction scripts (`epic_clarity_to_landing.py`, `cerner_millennium_to_landing.py`) and reference clinical code tables (ICD-10, CPT, NPI).
* [`data/BQ/`](data/BQ/): Production BigQuery SQL transformation scripts:
  * `bronze.sql`: External table definitions over GCS Parquet landing zones.
  * `silver.sql`: Data cleaning, quarantine routing, EDI 835 claim adjudication, and SCD Type 2 MERGE logic across 6 core entities.
  * `gold.sql`: Complete DDL and aggregation logic for all 15 Curated Gold Data Marts.
  * `streaming_views.sql`: Real-time streaming views for ED census.
* [`data/STREAMING/`](data/STREAMING/): Streaming event generator (`streaming_producer.py`) and consumers (`dataflow_streaming_consumer.py`, `pyspark_streaming_consumer.py`).
* [`infrastructure/`](infrastructure/): Terraform IaC configurations provisioning GCP resources (GCS, BigQuery, Pub/Sub, Service Accounts, IAM).
* [`data/configs/`](data/configs/): Metadata-driven pipeline configuration files (`load_config.csv`).
