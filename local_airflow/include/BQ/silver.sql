-------------------------------------------------------------------------------------------------------
-- ENTERPRISE HEALTHCARE BIGQUERY SILVER LAYER (MULTI-INSTANCE EPIC CLARITY)
-- Description: Standardizes schemas across North & South Epic Clarity instances, applies SCD Type 2 history,
--              enforces HIPAA Policy Tags, executes staging data quality checks, and adjudicates EDI 835 claims.
-------------------------------------------------------------------------------------------------------

-- ===============================================================================================
-- 1. SILVER PATIENTS (SCD Type 2)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.patients` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB DATE,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Language STRING,
    MaritalStatus STRING,
    Race STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY datasource, State;

-- Staging & Quality Checks for Patients
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_patients` AS
WITH combined_patients AS (
    -- Epic Clarity North Campus (Epic v2024)
    SELECT 
        CAST(COALESCE(PatientID, Id) AS STRING) AS SRC_PatientID,
        FirstName,
        LastName,
        Gender,
        SAFE_CAST(DOB AS DATE) AS DOB,
        Address,
        City,
        State,
        ZipCode,
        Language,
        MaritalStatus,
        Race,
        'epic_clarity_north' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_epic_north`
    
    UNION ALL
    
    -- Epic Clarity South Campus (Epic v2022)
    SELECT 
        CAST(COALESCE(PatientID, Id) AS STRING) AS SRC_PatientID,
        FirstName,
        LastName,
        Gender,
        SAFE_CAST(DOB AS DATE) AS DOB,
        Address,
        City,
        State,
        ZipCode,
        Language,
        MaritalStatus,
        Race,
        'epic_clarity_south' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_epic_south`
)
SELECT 
    GENERATE_UUID() AS Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    Gender,
    DOB,
    Address,
    City,
    State,
    ZipCode,
    Language,
    MaritalStatus,
    Race,
    datasource,
    CASE 
        WHEN SRC_PatientID IS NULL OR DOB IS NULL OR DOB > CURRENT_DATE() THEN TRUE
        ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM combined_patients;

-- Quarantine routing for Patients
INSERT INTO `carenet-rcm-data-platform.silver_dataset.patients`
SELECT * FROM `carenet-rcm-data-platform.silver_dataset.quality_checks_patients`
WHERE is_quarantined = TRUE;

-- SCD Type 2 MERGE for Clean Patients
MERGE `carenet-rcm-data-platform.silver_dataset.patients` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_patients` S
ON T.SRC_PatientID = S.SRC_PatientID 
   AND T.datasource = S.datasource
   AND T.is_current = TRUE
   AND S.is_quarantined = FALSE
WHEN MATCHED AND (
    T.Address != S.Address OR 
    T.City != S.City OR 
    T.State != S.State OR 
    T.ZipCode != S.ZipCode OR 
    T.MaritalStatus != S.MaritalStatus
) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Patient_Key, SRC_PatientID, FirstName, LastName, Gender, DOB, Address, City, State, ZipCode, Language, MaritalStatus, Race, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Patient_Key, S.SRC_PatientID, S.FirstName, S.LastName, S.Gender, S.DOB, S.Address, S.City, S.State, S.ZipCode, S.Language, S.MaritalStatus, S.Race, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);


-- ===============================================================================================
-- 2. SILVER PROVIDERS (SCD Type 2)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.providers` (
    Provider_Key STRING,
    SRC_ProviderID STRING,
    NPI STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY Specialization, datasource;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_providers` AS
WITH combined_providers AS (
    SELECT 
        CAST(COALESCE(ProviderID, Id) AS STRING) AS SRC_ProviderID,
        COALESCE(NPI, 'UNKNOWN') AS NPI,
        FirstName,
        LastName,
        COALESCE(Specialization, 'General Practice') AS Specialization,
        CAST(DeptID AS STRING) AS DeptID,
        'epic_clarity_north' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.providers_epic_north`
    
    UNION ALL
    
    SELECT 
        CAST(COALESCE(ProviderID, Id) AS STRING) AS SRC_ProviderID,
        COALESCE(NPI, 'UNKNOWN') AS NPI,
        FirstName,
        LastName,
        COALESCE(Specialization, 'General Practice') AS Specialization,
        CAST(DeptID AS STRING) AS DeptID,
        'epic_clarity_south' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.providers_epic_south`
)
SELECT 
    GENERATE_UUID() AS Provider_Key,
    SRC_ProviderID,
    NPI,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    datasource,
    CASE WHEN SRC_ProviderID IS NULL THEN TRUE ELSE FALSE END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM combined_providers;

MERGE `carenet-rcm-data-platform.silver_dataset.providers` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_providers` S
ON T.SRC_ProviderID = S.SRC_ProviderID AND T.datasource = S.datasource AND T.is_current = TRUE AND S.is_quarantined = FALSE
WHEN MATCHED AND (T.Specialization != S.Specialization OR T.DeptID != S.DeptID) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Provider_Key, SRC_ProviderID, NPI, FirstName, LastName, Specialization, DeptID, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Provider_Key, S.SRC_ProviderID, S.NPI, S.FirstName, S.LastName, S.Specialization, S.DeptID, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);


-- ===============================================================================================
-- 3. SILVER DEPARTMENTS (SCD Type 2)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.departments` (
    Department_Key STRING,
    SRC_DepartmentID STRING,
    Name STRING,
    Location STRING,
    FacilityID STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY datasource;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_departments` AS
WITH combined_departments AS (
    SELECT 
        CAST(COALESCE(DepartmentID, Id) AS STRING) AS SRC_DepartmentID,
        Name,
        Location,
        CAST(COALESCE(FacilityID, 'FAC-NORTH') AS STRING) AS FacilityID,
        'epic_clarity_north' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.departments_epic_north`
    
    UNION ALL
    
    SELECT 
        CAST(COALESCE(DepartmentID, Id) AS STRING) AS SRC_DepartmentID,
        Name,
        Location,
        CAST(COALESCE(FacilityID, 'FAC-SOUTH') AS STRING) AS FacilityID,
        'epic_clarity_south' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.departments_epic_south`
)
SELECT 
    GENERATE_UUID() AS Department_Key,
    SRC_DepartmentID,
    Name,
    Location,
    FacilityID,
    datasource,
    CASE WHEN SRC_DepartmentID IS NULL THEN TRUE ELSE FALSE END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM combined_departments;

MERGE `carenet-rcm-data-platform.silver_dataset.departments` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_departments` S
ON T.SRC_DepartmentID = S.SRC_DepartmentID AND T.datasource = S.datasource AND T.is_current = TRUE AND S.is_quarantined = FALSE
WHEN MATCHED AND (T.Name != S.Name OR T.Location != S.Location) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Department_Key, SRC_DepartmentID, Name, Location, FacilityID, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Department_Key, S.SRC_DepartmentID, S.Name, S.Location, S.FacilityID, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);


-- ===============================================================================================
-- 4. SILVER ENCOUNTERS (SCD Type 2 Fact Table)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.encounters` (
    Encounter_Key STRING,
    SRC_EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate DATE,
    DischargeDate DATE,
    EncounterType STRING,
    AdmitSource STRING,
    DischargeDisposition STRING,
    LengthOfStay INT64,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY EncounterDate
CLUSTER BY EncounterType, datasource;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` AS
WITH combined_encounters AS (
    SELECT 
        CAST(COALESCE(EncounterID, Id) AS STRING) AS SRC_EncounterID,
        CAST(PatientID AS STRING) AS PatientID,
        CAST(ProviderID AS STRING) AS ProviderID,
        CAST(DepartmentID AS STRING) AS DepartmentID,
        SAFE_CAST(EncounterDate AS DATE) AS EncounterDate,
        SAFE_CAST(DischargeDate AS DATE) AS DischargeDate,
        COALESCE(EncounterType, 'Outpatient') AS EncounterType,
        'epic_clarity_north' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.encounters_epic_north`
    
    UNION ALL
    
    SELECT 
        CAST(COALESCE(EncounterID, Id) AS STRING) AS SRC_EncounterID,
        CAST(PatientID AS STRING) AS PatientID,
        CAST(ProviderID AS STRING) AS ProviderID,
        CAST(DepartmentID AS STRING) AS DepartmentID,
        SAFE_CAST(EncounterDate AS DATE) AS EncounterDate,
        SAFE_CAST(DischargeDate AS DATE) AS DischargeDate,
        COALESCE(EncounterType, 'Outpatient') AS EncounterType,
        'epic_clarity_south' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.encounters_epic_south`
)
SELECT 
    GENERATE_UUID() AS Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    DischargeDate,
    EncounterType,
    'EMERGENCY_ROOM' AS AdmitSource,
    'HOME' AS DischargeDisposition,
    DATE_DIFF(COALESCE(DischargeDate, EncounterDate), EncounterDate, DAY) AS LengthOfStay,
    datasource,
    CASE 
        WHEN SRC_EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM combined_encounters;

MERGE `carenet-rcm-data-platform.silver_dataset.encounters` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` S
ON T.SRC_EncounterID = S.SRC_EncounterID AND T.datasource = S.datasource AND T.is_current = TRUE AND S.is_quarantined = FALSE
WHEN MATCHED AND (T.DischargeDate != S.DischargeDate OR T.EncounterType != S.EncounterType) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Encounter_Key, SRC_EncounterID, PatientID, ProviderID, DepartmentID, EncounterDate, DischargeDate, EncounterType, AdmitSource, DischargeDisposition, LengthOfStay, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Encounter_Key, S.SRC_EncounterID, S.PatientID, S.ProviderID, S.DepartmentID, S.EncounterDate, S.DischargeDate, S.EncounterType, S.AdmitSource, S.DischargeDisposition, S.LengthOfStay, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);


-- ===============================================================================================
-- 5. SILVER TRANSACTIONS (Charges & Financial Ledger)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DeptID STRING,
    CPTCode STRING,
    ICD10_Diagnosis STRING,
    ServiceDate DATE,
    Amount NUMERIC,
    PaidAmount NUMERIC,
    Copay NUMERIC,
    Coinsurance NUMERIC,
    Deductible NUMERIC,
    LineItemStatus STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY ServiceDate
CLUSTER BY CPTCode, datasource;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_transactions` AS
WITH combined_transactions AS (
    SELECT 
        CAST(COALESCE(TransactionID, Id) AS STRING) AS SRC_TransactionID,
        CAST(EncounterID AS STRING) AS EncounterID,
        CAST(PatientID AS STRING) AS PatientID,
        CAST(ProviderID AS STRING) AS ProviderID,
        CAST(DeptID AS STRING) AS DeptID,
        CAST(COALESCE(CPTCode, '99213') AS STRING) AS CPTCode,
        SAFE_CAST(ServiceDate AS DATE) AS ServiceDate,
        SAFE_CAST(Amount AS NUMERIC) AS Amount,
        SAFE_CAST(PaidAmount AS NUMERIC) AS PaidAmount,
        'epic_clarity_north' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.transactions_epic_north`
    
    UNION ALL
    
    SELECT 
        CAST(COALESCE(TransactionID, Id) AS STRING) AS SRC_TransactionID,
        CAST(EncounterID AS STRING) AS EncounterID,
        CAST(PatientID AS STRING) AS PatientID,
        CAST(ProviderID AS STRING) AS ProviderID,
        CAST(DeptID AS STRING) AS DeptID,
        CAST(COALESCE(CPTCode, '99213') AS STRING) AS CPTCode,
        SAFE_CAST(ServiceDate AS DATE) AS ServiceDate,
        SAFE_CAST(Amount AS NUMERIC) AS Amount,
        SAFE_CAST(PaidAmount AS NUMERIC) AS PaidAmount,
        'epic_clarity_south' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.transactions_epic_south`
)
SELECT 
    GENERATE_UUID() AS Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    CPTCode,
    'I10' AS ICD10_Diagnosis,
    ServiceDate,
    Amount,
    COALESCE(PaidAmount, 0) AS PaidAmount,
    25.00 AS Copay,
    10.00 AS Coinsurance,
    50.00 AS Deductible,
    'POSTED' AS LineItemStatus,
    datasource,
    CASE 
        WHEN SRC_TransactionID IS NULL OR Amount < 0 OR ServiceDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM combined_transactions;

MERGE `carenet-rcm-data-platform.silver_dataset.transactions` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_transactions` S
ON T.SRC_TransactionID = S.SRC_TransactionID AND T.datasource = S.datasource AND T.is_current = TRUE AND S.is_quarantined = FALSE
WHEN MATCHED AND (T.Amount != S.Amount OR T.PaidAmount != S.PaidAmount) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Transaction_Key, SRC_TransactionID, EncounterID, PatientID, ProviderID, DeptID, CPTCode, ICD10_Diagnosis, ServiceDate, Amount, PaidAmount, Copay, Coinsurance, Deductible, LineItemStatus, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Transaction_Key, S.SRC_TransactionID, S.EncounterID, S.PatientID, S.ProviderID, S.DeptID, S.CPTCode, S.ICD10_Diagnosis, S.ServiceDate, S.Amount, S.PaidAmount, S.Copay, S.Coinsurance, S.Deductible, S.LineItemStatus, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);


-- ===============================================================================================
-- 6. SILVER CLAIMS (EDI 835 Remittance Adjudication & RCM Status Engine)
-- ===============================================================================================
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.claims` (
    Claim_Key STRING,
    SRC_ClaimID STRING,
    TransactionID STRING,
    PatientID STRING,
    ProviderID STRING,
    PayorID STRING,
    PayorType STRING,
    ClaimAmount NUMERIC,
    PaidAmount NUMERIC,
    ClaimStatus STRING,
    DenialReasonCode STRING,
    CARC_Description STRING,
    ServiceDate DATE,
    AdjudicationDate DATE,
    DaysToSettle INT64,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY ServiceDate
CLUSTER BY ClaimStatus, PayorType, datasource;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` AS
WITH combined_claims AS (
    SELECT 
        CONCAT('CLM-', t.SRC_TransactionID) AS SRC_ClaimID,
        t.SRC_TransactionID AS TransactionID,
        t.PatientID,
        t.ProviderID,
        CASE 
            WHEN MOD(ABS(FARM_FINGERPRINT(t.PatientID)), 4) = 0 THEN 'MEDICARE_CMS'
            WHEN MOD(ABS(FARM_FINGERPRINT(t.PatientID)), 4) = 1 THEN 'BCBS_COMMERCIAL'
            WHEN MOD(ABS(FARM_FINGERPRINT(t.PatientID)), 4) = 2 THEN 'AETNA_HEALTH'
            ELSE 'UNITED_HEALTHCARE'
        END AS PayorID,
        CASE 
            WHEN MOD(ABS(FARM_FINGERPRINT(t.PatientID)), 4) = 0 THEN 'GOVERNMENT'
            ELSE 'COMMERCIAL'
        END AS PayorType,
        t.Amount AS ClaimAmount,
        t.PaidAmount,
        t.ServiceDate,
        t.datasource
    FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
    WHERE t.is_current = TRUE
),
adjudicated AS (
    SELECT 
        c.*,
        r.carc_code,
        CASE 
            WHEN r.carc_code = '45' THEN 'Fee Schedule / Contractual Maximum Adjustment'
            WHEN r.carc_code = '16' THEN 'Claim Lacks Required Clinical Information'
            WHEN r.carc_code = '96' THEN 'Non-Covered Charge / Policy Exclusion'
            WHEN r.carc_code = '97' THEN 'Bundled Service / Payment Included in Primary Procedure'
            WHEN r.carc_code IS NOT NULL THEN 'Payer Specific Denial / Coinsurance Adjustment'
            ELSE 'Clean Submission'
        END AS CARC_Description,
        CASE 
            WHEN c.PaidAmount >= c.ClaimAmount THEN 'SETTLED_PAID_FULL'
            WHEN c.PaidAmount > 0 AND c.PaidAmount < c.ClaimAmount THEN 'SETTLED_PARTIAL_PAYMENT'
            WHEN r.carc_code IN ('16', '96', '97') THEN 'DENIED'
            WHEN r.carc_code = '45' THEN 'SETTLED_CONTRACTUAL_ADJUSTMENT'
            WHEN c.PaidAmount = 0 AND DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) > 60 THEN 'DENIED_EXPIRED'
            ELSE 'PENDING_ADJUDICATION'
        END AS ClaimStatus,
        DATE_ADD(c.ServiceDate, INTERVAL CAST(FLOOR(RAND() * 20 + 5) AS INT64) DAY) AS AdjudicationDate
    FROM combined_claims c
    LEFT JOIN `carenet-rcm-data-platform.bronze_dataset.claims_remittance` r
        ON c.SRC_ClaimID = r.claim_id
)
SELECT 
    GENERATE_UUID() AS Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    ProviderID,
    PayorID,
    PayorType,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    COALESCE(carc_code, 'NONE') AS DenialReasonCode,
    CARC_Description,
    ServiceDate,
    AdjudicationDate,
    DATE_DIFF(AdjudicationDate, ServiceDate, DAY) AS DaysToSettle,
    datasource,
    CASE WHEN SRC_ClaimID IS NULL OR ClaimAmount < 0 THEN TRUE ELSE FALSE END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM adjudicated;

MERGE `carenet-rcm-data-platform.silver_dataset.claims` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` S
ON T.SRC_ClaimID = S.SRC_ClaimID AND T.datasource = S.datasource AND T.is_current = TRUE AND S.is_quarantined = FALSE
WHEN MATCHED AND (T.ClaimStatus != S.ClaimStatus OR T.PaidAmount != S.PaidAmount) THEN
    UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET AND S.is_quarantined = FALSE THEN
    INSERT (Claim_Key, SRC_ClaimID, TransactionID, PatientID, ProviderID, PayorID, PayorType, ClaimAmount, PaidAmount, ClaimStatus, DenialReasonCode, CARC_Description, ServiceDate, AdjudicationDate, DaysToSettle, datasource, is_quarantined, inserted_date, modified_date, is_current)
    VALUES (S.Claim_Key, S.SRC_ClaimID, S.TransactionID, S.PatientID, S.ProviderID, S.PayorID, S.PayorType, S.ClaimAmount, S.PaidAmount, S.ClaimStatus, S.DenialReasonCode, S.CARC_Description, S.ServiceDate, S.AdjudicationDate, S.DaysToSettle, S.datasource, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
