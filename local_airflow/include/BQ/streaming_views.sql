-- BigQuery Script to Setup Streaming Target and Real-Time Reporting Views

-- 1. Create Streaming Target Table in Bronze
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.bronze_dataset.transactions_streaming` (
    event_timestamp STRING,
    transaction_id STRING,
    encounter_id STRING,
    patient_id STRING,
    patient_first_name STRING,
    patient_last_name STRING,
    patient_gender STRING,
    patient_address STRING,
    provider_id STRING,
    dept_id STRING,
    encounter_type STRING,
    procedure_code INT64,
    icd_code STRING,
    payor_id STRING,
    payor_type STRING,
    billed_amount FLOAT64,
    claim_id STRING,
    claim_status STRING,
    claim_paid_amount FLOAT64
);

--------------------------------------------------------------------------------------------------
-- 2. Real-Time Provider Charge View (Streaming + Batch Joins)
-- Joins the real-time transactions stream with the batch-loaded providers/departments
CREATE OR REPLACE VIEW `avd-databricks-demo.gold_dataset.v_realtime_provider_charge_summary` AS
SELECT 
    CONCAT(p.firstname, ' ', p.LastName) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(t.billed_amount) AS Billed_Amount,
    COUNT(t.transaction_id) AS Total_Transactions
FROM `avd-databricks-demo.bronze_dataset.transactions_streaming` t
LEFT JOIN `avd-databricks-demo.silver_dataset.providers` p 
    ON p.ProviderID = CONCAT(t.provider_id, '-hosa') -- Assuming Hospital A for simulation mapping
LEFT JOIN `avd-databricks-demo.silver_dataset.departments` d 
    ON d.Dept_Id = CONCAT(t.dept_id, '-hosa')
GROUP BY Provider_Name, Dept_Name;

--------------------------------------------------------------------------------------------------
-- 3. Real-Time Claim Denial Rate View
-- Dynamically monitors the denial rate from the streaming payer claims responses
CREATE OR REPLACE VIEW `avd-databricks-demo.gold_dataset.v_realtime_claim_denials` AS
SELECT 
    payor_type AS Payor_Type,
    claim_status AS Claim_Status,
    COUNT(claim_id) AS Total_Claims,
    SUM(billed_amount) AS Total_Billed,
    ROUND((COUNT(CASE WHEN claim_status = 'Denied' THEN 1 END) / NULLIF(COUNT(claim_id), 0)) * 100, 2) AS Realtime_Denial_Rate
FROM `avd-databricks-demo.bronze_dataset.transactions_streaming`
GROUP BY Payor_Type, Claim_Status;
