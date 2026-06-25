-- Description: Create external tables for bronze dataset in BigQuery
-- please do not forget to replace the bucket path

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_ha` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/epic-clarity/departments/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_ha` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/epic-clarity/encounters/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_ha` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/epic-clarity/patients/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_ha` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/epic-clarity/providers/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_ha` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/epic-clarity/transactions/*.parquet']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.departments_hb` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/cerner-millennium/departments/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.encounters_hb` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/cerner-millennium/encounters/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.patients_hb` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/cerner-millennium/patients/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.providers_hb` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/cerner-millennium/providers/*.parquet']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_hb` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-bucket-22032025/landing/cerner-millennium/transactions/*.parquet']
);

---------------------------------------------------------------------------------------------------------------------------