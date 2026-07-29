--------------------------------------------------------------------------------------------------
-- ENTERPRISE HEALTHCARE BIGQUERY BRONZE LAYER (DATA LAKE LANDING)
-- Description: Creates External Tables over raw Parquet files ingested from Multi-Instance Epic Clarity
-- Note: Raw ingestion is performed by PySpark/Airflow directly from Epic Clarity JDBC instances.
--------------------------------------------------------------------------------------------------

-- ===============================================================================================
-- SOURCE 1: EPIC CLARITY NORTH CAMPUS (HOSPITAL NORTH - epic_clarity_north)
-- Mappings: PATIENT, CLARITY_SER, CLARITY_DEP, PAT_ENC, ARPB_TRANSACTIONS (Epic v2024)
-- ===============================================================================================

-- 1. Epic North PATIENT Master (Demographics)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_epic_north`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-north/patients/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-north/PATIENT/*.parquet']
);

-- 2. Epic North CLARITY_SER (Provider/Doctor Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_epic_north`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-north/providers/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-north/CLARITY_SER/*.parquet']
);

-- 3. Epic North CLARITY_DEP (Department/Location Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_epic_north`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-north/departments/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-north/CLARITY_DEP/*.parquet']
);

-- 4. Epic North PAT_ENC (Patient Encounters / Visits)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_epic_north`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-north/encounters/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-north/PAT_ENC/*.parquet']
);

-- 5. Epic North ARPB_TRANSACTIONS (Professional Billing & Charges)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_epic_north`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-north/transactions/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-north/ARPB_TRANSACTIONS/*.parquet']
);

-- ===============================================================================================
-- SOURCE 2: EPIC CLARITY SOUTH CAMPUS (HOSPITAL SOUTH - epic_clarity_south)
-- Mappings: PATIENT, CLARITY_SER, CLARITY_DEP, PAT_ENC, ARPB_TRANSACTIONS (Epic v2022 - Acquired Campus)
-- ===============================================================================================

-- 1. Epic South PATIENT Master
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_epic_south`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-south/patients/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-south/PATIENT/*.parquet']
);

-- 2. Epic South CLARITY_SER (Provider Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_epic_south`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-south/providers/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-south/CLARITY_SER/*.parquet']
);

-- 3. Epic South CLARITY_DEP (Department Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_epic_south`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-south/departments/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-south/CLARITY_DEP/*.parquet']
);

-- 4. Epic South PAT_ENC (Patient Encounters)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_epic_south`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-south/encounters/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-south/PAT_ENC/*.parquet']
);

-- 5. Epic South ARPB_TRANSACTIONS (Charges)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_epic_south`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic-clarity-south/transactions/*.parquet', 'gs://healthcare-data-lake-prod/landing/epic-clarity-south/ARPB_TRANSACTIONS/*.parquet']
);

-- ===============================================================================================
-- SOURCE 3: PAYER REMITTANCE & CLAIMS (EDI 835 / CLEARINGHOUSE ERA)
-- ===============================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.claims_remittance`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/clearinghouse/edi_835_parsed/*.parquet']
);
