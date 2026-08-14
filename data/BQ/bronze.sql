--------------------------------------------------------------------------------------------------
-- ENTERPRISE HEALTHCARE BIGQUERY BRONZE LAYER (DATA LAKE LANDING)
-- Description: Creates External Tables over raw Parquet files ingested from Epic Clarity & Cerner
-- Note: Raw ingestion is performed by PySpark/Airflow directly from Epic Clarity JDBC.
--------------------------------------------------------------------------------------------------

-- ===============================================================================================
-- SOURCE 1: EPIC CLARITY (HOSPITAL A - hosa)
-- Mappings: PATIENT, CLARITY_SER, CLARITY_DEP, PAT_ENC, ARPB_TRANSACTIONS
-- ===============================================================================================

-- 1. Epic PATIENT Master (Demographics)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_epic`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic_clarity/PATIENT/*.parquet']
);

-- 2. Epic CLARITY_SER (Provider/Doctor Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_epic`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic_clarity/CLARITY_SER/*.parquet']
);

-- 3. Epic CLARITY_DEP (Department/Location Master)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_epic`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic_clarity/CLARITY_DEP/*.parquet']
);

-- 4. Epic PAT_ENC (Patient Encounters / Visits)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_epic`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic_clarity/PAT_ENC/*.parquet']
);

-- 5. Epic ARPB_TRANSACTIONS (Professional Billing & Charges)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_epic`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/epic_clarity/ARPB_TRANSACTIONS/*.parquet']
);

-- ===============================================================================================
-- SOURCE 2: CERNER MILLENNIUM (HOSPITAL B - hosb)
-- Mappings: Cerner Person, Prsnl, Encounter, Charge Event tables
-- ===============================================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_cerner`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/cerner_millennium/person/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_cerner`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/cerner_millennium/prsnl/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_cerner`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/cerner_millennium/location/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_cerner`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/cerner_millennium/encounter/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_cerner`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/cerner_millennium/charge_event/*.parquet']
);

-- ===============================================================================================
-- SOURCE 3: PAYER REMITTANCE & CLAIMS (EDI 835 / FHIR JSON / CLARITY_CLM)
-- ===============================================================================================

-- Claims Remittance (Generated from Clearinghouses or Epic CLARITY_CLM / TDL_TRAN)
CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.claims_remittance`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-lake-prod/landing/clearinghouse/edi_835_parsed/*.parquet']
);
