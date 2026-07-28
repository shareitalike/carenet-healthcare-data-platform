-- BigQuery Script to Setup Enterprise Streaming Target and Real-Time Reporting Views
-- Updated to support domain-specific event types (ADT, Claims, Orders)
-- with PHI hashing and Dead Letter Queue support.

-- 1. Create Streaming Target Table in Bronze (Unified Event Store)
-- All event types (ADT, Claims, Orders) land in a single wide table
-- for cross-domain analytics. Sparse columns are NULL for non-applicable event types.
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.transactions_streaming` (
    event_type STRING,
    event_timestamp STRING,
    message_control_id STRING,
    transaction_id STRING,
    encounter_id STRING,
    order_id STRING,
    claim_id STRING,
    patient_id STRING,
    patient_first_name_hash STRING,
    patient_last_name_hash STRING,
    patient_gender STRING,
    patient_address_hash STRING,
    provider_id STRING,
    dept_id STRING,
    encounter_type STRING,
    admit_source STRING,
    discharge_disposition STRING,
    drg_code STRING,
    primary_icd_code STRING,
    procedure_code INT64,
    icd_code STRING,
    payor_id STRING,
    payor_type STRING,
    billed_amount FLOAT64,
    paid_amount FLOAT64,
    claim_status STRING,
    claim_type STRING,
    revenue_code STRING,
    place_of_service STRING,
    order_type STRING,
    order_status STRING,
    priority STRING
);

--------------------------------------------------------------------------------------------------
-- 2. Dead Letter Queue Table
-- Stores messages that failed parsing or validation in the Dataflow/PySpark consumers.
-- Data Engineers review this table to identify upstream data quality issues and replay fixed records.
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.bronze_dataset.streaming_dead_letter_queue` (
    raw_payload STRING,
    error_type STRING,
    error_message STRING,
    pipeline_stage STRING
);

--------------------------------------------------------------------------------------------------
-- 3. Real-Time Provider Charge View (Streaming + Batch Joins)
-- Joins the real-time transactions stream with the batch-loaded providers/departments
CREATE OR REPLACE VIEW `carenet-rcm-data-platform.gold_dataset.v_realtime_provider_charge_summary` AS
SELECT 
    CONCAT(p.firstname, ' ', p.LastName) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(t.billed_amount) AS Billed_Amount,
    COUNT(t.transaction_id) AS Total_Transactions
FROM `carenet-rcm-data-platform.bronze_dataset.transactions_streaming` t
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.providers` p 
    ON p.ProviderID = CONCAT(t.provider_id, '-epic-clarity')
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.departments` d 
    ON d.Dept_Id = CONCAT(t.dept_id, '-epic-clarity')
WHERE t.event_type IN ('CLAIM', 'ADT')
GROUP BY Provider_Name, Dept_Name;

--------------------------------------------------------------------------------------------------
-- 4. Real-Time Claim Denial Rate View
-- Dynamically monitors the denial rate from streaming claims events
CREATE OR REPLACE VIEW `carenet-rcm-data-platform.gold_dataset.v_realtime_claim_denials` AS
SELECT 
    payor_type AS Payor_Type,
    claim_status AS Claim_Status,
    COUNT(claim_id) AS Total_Claims,
    SUM(billed_amount) AS Total_Billed,
    ROUND((COUNT(CASE WHEN claim_status = 'Denied' THEN 1 END) / NULLIF(COUNT(claim_id), 0)) * 100, 2) AS Realtime_Denial_Rate
FROM `carenet-rcm-data-platform.bronze_dataset.transactions_streaming`
WHERE event_type = 'CLAIM'
GROUP BY Payor_Type, Claim_Status;

--------------------------------------------------------------------------------------------------
-- 5. Real-Time ADT Census View
-- Monitors real-time hospital census: admits vs discharges by encounter type
CREATE OR REPLACE VIEW `carenet-rcm-data-platform.gold_dataset.v_realtime_adt_census` AS
SELECT 
    encounter_type AS Encounter_Type,
    admit_source AS Admit_Source,
    discharge_disposition AS Discharge_Disposition,
    drg_code AS DRG_Code,
    COUNT(encounter_id) AS Total_Encounters,
    COUNT(DISTINCT patient_id) AS Unique_Patients
FROM `carenet-rcm-data-platform.bronze_dataset.transactions_streaming`
WHERE event_type = 'ADT'
GROUP BY encounter_type, admit_source, discharge_disposition, drg_code;

--------------------------------------------------------------------------------------------------
-- 6. DLQ Monitoring View
-- Provides visibility into streaming pipeline errors for the data engineering team
CREATE OR REPLACE VIEW `carenet-rcm-data-platform.gold_dataset.v_streaming_dlq_monitor` AS
SELECT 
    error_type,
    pipeline_stage,
    COUNT(*) AS error_count,
    ARRAY_AGG(error_message LIMIT 5) AS sample_errors
FROM `carenet-rcm-data-platform.bronze_dataset.streaming_dead_letter_queue`
GROUP BY error_type, pipeline_stage
ORDER BY error_count DESC;
