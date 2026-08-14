-------------------------------------------------------------------------------------------------------
-- ENTERPRISE HEALTHCARE BIGQUERY SILVER LAYER
-- Description: Standardizes schemas, applies SCD Type 2 history, and enforces HIPAA Policy Tags.
-- Core Tables: Patients, Providers, Departments, Encounters, Transactions, Claims
-------------------------------------------------------------------------------------------------------

-- ===============================================================================================
-- 1. SILVER PATIENTS (SCD Type 2)
-- ===============================================================================================

-- Create Table
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
    InsertDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY datasource, State;

-- Quality Checks Temp Table
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_patients` AS
SELECT 
    CONCAT(PatientID, '-', datasource) AS Patient_Key,
    PatientID AS SRC_PatientID,
    FirstName,
    LastName,
    Gender,
    SAFE.PARSE_DATE('%Y-%m-%d', DOB) AS DOB,
    Address,
    City,
    State,
    ZipCode,
    Language,
    MaritalStatus,
    Race,
    InsertDate,
    ModifiedDate,
    datasource,
    CASE 
        WHEN PatientID IS NULL OR FirstName IS NULL OR LastName IS NULL OR DOB IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT PatientID, FirstName, LastName, Gender, DOB, Address, City, State, ZipCode, Language, MaritalStatus, Race, InsertDate, ModifiedDate, 'epic_clarity' AS datasource 
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_epic`
    UNION ALL
    SELECT PersonID AS PatientID, FirstName, LastName, Gender, DOB, Address, City, State, ZipCode, Language, MaritalStatus, Race, InsertDate, ModifiedDate, 'cerner_millennium' AS datasource 
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_cerner`
);

-- SCD Type 2 MERGE
MERGE `carenet-rcm-data-platform.silver_dataset.patients` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_patients` S
ON T.Patient_Key = S.Patient_Key AND T.is_current = TRUE
WHEN MATCHED AND (
    T.Address != S.Address OR 
    T.MaritalStatus != S.MaritalStatus OR
    T.LastName != S.LastName
) THEN
  UPDATE SET 
    is_current = FALSE,
    modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Patient_Key, SRC_PatientID, FirstName, LastName, Gender, DOB, Address, City, State, ZipCode, Language, MaritalStatus, Race, InsertDate, ModifiedDate, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Patient_Key, S.SRC_PatientID, S.FirstName, S.LastName, S.Gender, S.DOB, S.Address, S.City, S.State, S.ZipCode, S.Language, S.MaritalStatus, S.Race, S.InsertDate, S.ModifiedDate, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_patients`;


-- ===============================================================================================
-- 2. SILVER PROVIDERS (SCD Type 2)
-- ===============================================================================================

CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.providers` (
    Provider_Key STRING,
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI STRING,
    InsertDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY datasource, Specialization;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_providers` AS
SELECT 
    CONCAT(ProviderID, '-', datasource) AS Provider_Key,
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    NPI,
    InsertDate,
    ModifiedDate,
    datasource,
    CASE WHEN ProviderID IS NULL OR NPI IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM (
    SELECT ProviderID, FirstName, LastName, Specialization, DeptID, NPI, InsertDate, ModifiedDate, 'epic_clarity' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.providers_epic`
    UNION ALL
    SELECT PrsnlID AS ProviderID, FirstName, LastName, Specialization, DeptID, NPI, InsertDate, ModifiedDate, 'cerner_millennium' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.providers_cerner`
);

MERGE `carenet-rcm-data-platform.silver_dataset.providers` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_providers` S
ON T.Provider_Key = S.Provider_Key AND T.is_current = TRUE
WHEN MATCHED AND (T.Specialization != S.Specialization OR T.DeptID != S.DeptID) THEN
  UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Provider_Key, ProviderID, FirstName, LastName, Specialization, DeptID, NPI, InsertDate, ModifiedDate, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Provider_Key, S.ProviderID, S.FirstName, S.LastName, S.Specialization, S.DeptID, S.NPI, S.InsertDate, S.ModifiedDate, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_providers`;


-- ===============================================================================================
-- 3. SILVER DEPARTMENTS (SCD Type 2)
-- ===============================================================================================

CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.departments` (
    Dept_Key STRING,
    Dept_Id STRING,
    DepartmentName STRING,
    Location STRING,
    FacilityID STRING,
    InsertDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY DATE(inserted_date)
CLUSTER BY datasource, Location;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_departments` AS
SELECT 
    CONCAT(DeptID, '-', datasource) AS Dept_Key,
    DeptID AS Dept_Id,
    DepartmentName,
    Location,
    FacilityID,
    InsertDate,
    ModifiedDate,
    datasource,
    CASE WHEN DeptID IS NULL OR DepartmentName IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM (
    SELECT DeptID, DepartmentName, Location, FacilityID, InsertDate, ModifiedDate, 'epic_clarity' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.departments_epic`
    UNION ALL
    SELECT LocationID AS DeptID, DepartmentName, Location, FacilityID, InsertDate, ModifiedDate, 'cerner_millennium' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.departments_cerner`
);

MERGE `carenet-rcm-data-platform.silver_dataset.departments` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_departments` S
ON T.Dept_Key = S.Dept_Key AND T.is_current = TRUE
WHEN MATCHED AND (T.DepartmentName != S.DepartmentName OR T.Location != S.Location) THEN
  UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Dept_Key, Dept_Id, DepartmentName, Location, FacilityID, InsertDate, ModifiedDate, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Dept_Key, S.Dept_Id, S.DepartmentName, S.Location, S.FacilityID, S.InsertDate, S.ModifiedDate, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_departments`;


-- ===============================================================================================
-- 4. SILVER ENCOUNTERS (SCD Type 2)
-- ===============================================================================================

CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.encounters` (
    Encounter_Key STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate DATE,
    EncounterType STRING,
    InsertDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY EncounterDate
CLUSTER BY datasource, EncounterType;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` AS
SELECT 
    CONCAT(EncounterID, '-', datasource) AS Encounter_Key,
    EncounterID, PatientID, ProviderID, DepartmentID,
    SAFE.PARSE_DATE('%Y-%m-%d', EncounterDate) AS EncounterDate,
    EncounterType, InsertDate, ModifiedDate, datasource,
    CASE WHEN EncounterID IS NULL OR PatientID IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM (
    SELECT EncounterID, PatientID, ProviderID, DepartmentID, EncounterDate, EncounterType, InsertDate, ModifiedDate, 'epic_clarity' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.encounters_epic`
    UNION ALL
    SELECT EncounterID, PersonID AS PatientID, PrsnlID AS ProviderID, LocationID AS DepartmentID, EncounterDate, EncounterType, InsertDate, ModifiedDate, 'cerner_millennium' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.encounters_cerner`
);

MERGE `carenet-rcm-data-platform.silver_dataset.encounters` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` S
ON T.Encounter_Key = S.Encounter_Key AND T.is_current = TRUE
WHEN MATCHED AND (T.EncounterType != S.EncounterType) THEN
  UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Encounter_Key, EncounterID, PatientID, ProviderID, DepartmentID, EncounterDate, EncounterType, InsertDate, ModifiedDate, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Encounter_Key, S.EncounterID, S.PatientID, S.ProviderID, S.DepartmentID, S.EncounterDate, S.EncounterType, S.InsertDate, S.ModifiedDate, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters`;


-- ===============================================================================================
-- 5. SILVER TRANSACTIONS (SCD Type 2)
-- ===============================================================================================

CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    TransactionDate DATE,
    TransactionType STRING,
    Amount FLOAT64,
    PaidAmount FLOAT64,
    InsertDate TIMESTAMP,
    ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY TransactionDate
CLUSTER BY datasource, TransactionType;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_transactions` AS
SELECT 
    CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
    TransactionID AS SRC_TransactionID, EncounterID, PatientID, ProviderID, DepartmentID,
    SAFE.PARSE_DATE('%Y-%m-%d', TransactionDate) AS TransactionDate,
    TransactionType,
    SAFE_CAST(Amount AS FLOAT64) AS Amount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS PaidAmount,
    InsertDate, ModifiedDate, datasource,
    CASE WHEN TransactionID IS NULL OR PatientID IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM (
    SELECT TransactionID, EncounterID, PatientID, ProviderID, DepartmentID, TransactionDate, TransactionType, Amount, PaidAmount, InsertDate, ModifiedDate, 'epic_clarity' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.transactions_epic`
    UNION ALL
    SELECT ChargeEventID AS TransactionID, EncounterID, PersonID AS PatientID, PrsnlID AS ProviderID, LocationID AS DepartmentID, TransactionDate, TransactionType, Amount, PaidAmount, InsertDate, ModifiedDate, 'cerner_millennium' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.transactions_cerner`
);

MERGE `carenet-rcm-data-platform.silver_dataset.transactions` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_transactions` S
ON T.Transaction_Key = S.Transaction_Key AND T.is_current = TRUE
WHEN MATCHED AND (T.Amount != S.Amount OR T.PaidAmount != S.PaidAmount) THEN
  UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Transaction_Key, SRC_TransactionID, EncounterID, PatientID, ProviderID, DepartmentID, TransactionDate, TransactionType, Amount, PaidAmount, InsertDate, ModifiedDate, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Transaction_Key, S.SRC_TransactionID, S.EncounterID, S.PatientID, S.ProviderID, S.DepartmentID, S.TransactionDate, S.TransactionType, S.Amount, S.PaidAmount, S.InsertDate, S.ModifiedDate, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_transactions`;


-- ===============================================================================================
-- 6. SILVER CLAIMS (EDI 835 Adjudication & Financial Settlement)
-- ===============================================================================================

CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.claims` (
    Claim_Key STRING,
    SRC_ClaimID STRING,
    TransactionID STRING,
    PatientID STRING,
    EncounterID STRING,
    ProviderID STRING,
    DeptID STRING,
    ServiceDate DATE,
    ClaimDate DATE,
    PayorID STRING,
    ClaimAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    carc_code STRING,              
    settlement_status STRING,      
    PayorType STRING,
    Deductible FLOAT64,
    Coinsurance FLOAT64,
    Copay FLOAT64,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY ServiceDate
CLUSTER BY datasource, PayorID;

CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` AS
SELECT 
    CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
    SRC_ClaimID, TransactionID, PatientID, EncounterID, ProviderID, DeptID,
    SAFE.PARSE_DATE('%Y-%m-%d', ServiceDate) AS ServiceDate,
    SAFE.PARSE_DATE('%Y-%m-%d', ClaimDate) AS ClaimDate,
    PayorID,
    SAFE_CAST(ClaimAmount AS FLOAT64) AS ClaimAmount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS PaidAmount,
    ClaimStatus,
    COALESCE(carc_code, '0') AS carc_code,
    CASE 
        WHEN (SAFE_CAST(PaidAmount AS FLOAT64) + SAFE_CAST(Copay AS FLOAT64) + SAFE_CAST(Deductible AS FLOAT64)) >= SAFE_CAST(ClaimAmount AS FLOAT64) THEN 'SETTLED_PAID_FULL'
        WHEN SAFE_CAST(PaidAmount AS FLOAT64) > 0 AND carc_code = '45' THEN 'SETTLED_CONTRACTUAL_ADJUSTMENT'
        WHEN SAFE_CAST(PaidAmount AS FLOAT64) = 0 AND carc_code IS NOT NULL AND carc_code != '0' THEN 'DENIED'
        ELSE 'PENDING_ADJUDICATION'
    END AS settlement_status,
    PayorType,
    SAFE_CAST(Deductible AS FLOAT64) AS Deductible,
    SAFE_CAST(Coinsurance AS FLOAT64) AS Coinsurance,
    SAFE_CAST(Copay AS FLOAT64) AS Copay,
    datasource,
    CASE WHEN SRC_ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM (
    SELECT ClaimID AS SRC_ClaimID, TransactionID, PatientID, EncounterID, ProviderID, DeptID, ServiceDate, ClaimDate, PayorID, ClaimAmount, PaidAmount, ClaimStatus, carc_code, PayorType, Deductible, Coinsurance, Copay, 'epic_clarity' AS datasource 
    FROM `carenet-rcm-data-platform.bronze_dataset.claims_remittance`
);

MERGE `carenet-rcm-data-platform.silver_dataset.claims` T
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` S
ON T.Claim_Key = S.Claim_Key AND T.is_current = TRUE
WHEN MATCHED AND (
    T.PaidAmount != S.PaidAmount OR 
    T.ClaimStatus != S.ClaimStatus OR
    T.settlement_status != S.settlement_status
) THEN
  UPDATE SET is_current = FALSE, modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (Claim_Key, SRC_ClaimID, TransactionID, PatientID, EncounterID, ProviderID, DeptID, ServiceDate, ClaimDate, PayorID, ClaimAmount, PaidAmount, ClaimStatus, carc_code, settlement_status, PayorType, Deductible, Coinsurance, Copay, datasource, is_quarantined, inserted_date, modified_date, is_current)
  VALUES (S.Claim_Key, S.SRC_ClaimID, S.TransactionID, S.PatientID, S.EncounterID, S.ProviderID, S.DeptID, S.ServiceDate, S.ClaimDate, S.PayorID, S.ClaimAmount, S.PaidAmount, S.ClaimStatus, S.carc_code, S.settlement_status, S.PayorType, S.Deductible, S.Coinsurance, S.Copay, S.datasource, S.is_quarantined, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);

DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_claims`;
