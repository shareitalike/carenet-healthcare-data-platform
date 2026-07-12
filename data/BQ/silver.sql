-- IN THIS WE WE WILL IMPLEMENTING BOTH SCD2 AND CDM LOGIC FOR THE SILVER TABLES

-- 1. Create table departments by Merge Data from Hospital A & B  
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.departments` (
    Dept_Id STRING,
    SRC_Dept_Id STRING,
    Name STRING,
    datasource STRING,
    is_quarantined BOOLEAN
)
CLUSTER BY datasource;

-- 2. Truncate Silver Table Before Inserting 
TRUNCATE TABLE `carenet-rcm-data-platform.silver_dataset.departments`;

-- 3. full load by Inserting merged Data 
INSERT INTO `carenet-rcm-data-platform.silver_dataset.departments`
SELECT DISTINCT 
    CONCAT(deptid, '-', datasource) AS Dept_Id,
    deptid AS SRC_Dept_Id,
    Name,
    datasource,
    CASE 
        WHEN deptid IS NULL OR Name IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.departments_hb`
);

-------------------------------------------------------------------------------------------------------

-- 1. Create table providers by Merge Data from Hospital A & B  
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    DEA_Number STRING,
    Taxonomy_Code STRING,
    State_License_Number STRING,
    datasource STRING,
    is_quarantined BOOLEAN
)
CLUSTER BY datasource, Specialization;

-- 2. Truncate Silver Table Before Inserting 
TRUNCATE TABLE `carenet-rcm-data-platform.silver_dataset.providers`;

-- 3. full load by Inserting merged Data 
INSERT INTO `carenet-rcm-data-platform.silver_dataset.providers`
SELECT DISTINCT 
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    CAST(NPI AS INT64) AS NPI,
    'UNKNOWN_DEA' AS DEA_Number,
    'UNKNOWN_TAXONOMY' AS Taxonomy_Code,
    'UNKNOWN_LICENSE' AS State_License_Number,
    datasource,
    CASE 
        WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.providers_hb`
);

-------------------------------------------------------------------------------------------------------

-- 1. Create patients Table in BigQuery
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.patients` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    MRN STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    SSN_Hash STRING,
    PhoneNumber STRING,
    Gender STRING,
    DOB DATE,
    Address STRING,
    Race STRING,
    Ethnicity STRING,
    Language_Preference STRING,
    MaritalStatus STRING,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
)
CLUSTER BY datasource, is_current;

--Create a quality_checks temp table
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
    SRC_PatientID,
    CONCAT('MRN-', SRC_PatientID) AS MRN,
    FirstName,
    LastName,
    MiddleName,
    TO_HEX(SHA256(SSN)) AS SSN_Hash,
    PhoneNumber,
    Gender,
    SAFE.PARSE_DATE('%Y%m%d', CAST(DOB AS STRING)) AS DOB,
    Address,
    'Unknown' AS Race,
    'Unknown' AS Ethnicity,
    'English' AS Language_Preference,
    'Unknown' AS MaritalStatus,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_PatientID IS NULL OR DOB IS NULL OR FirstName IS NULL OR LOWER(FirstName) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        PatientID AS SRC_PatientID,
        FirstName,
        LastName,
        MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosa' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_ha`
    
    UNION ALL
    
    SELECT DISTINCT 
        ID AS SRC_PatientID,
        F_Name as FirstName,
        L_Name as LastName,
        M_Name as MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosb' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.patients_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.patients` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_PatientID <> source.SRC_PatientID OR
    target.MRN <> source.MRN OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.MiddleName <> source.MiddleName OR
    target.SSN_Hash <> source.SSN_Hash OR
    target.PhoneNumber <> source.PhoneNumber OR
    target.Gender <> source.Gender OR
    target.DOB <> source.DOB OR
    target.Address <> source.Address OR
    target.Race <> source.Race OR
    target.Ethnicity <> source.Ethnicity OR
    target.Language_Preference <> source.Language_Preference OR
    target.MaritalStatus <> source.MaritalStatus OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Patient_Key,
    SRC_PatientID,
    MRN,
    FirstName,
    LastName,
    MiddleName,
    SSN_Hash,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    Race,
    Ethnicity,
    Language_Preference,
    MaritalStatus,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Patient_Key,
    source.SRC_PatientID,
    source.MRN,
    source.FirstName,
    source.LastName,
    source.MiddleName,
    source.SSN_Hash,
    source.PhoneNumber,
    source.Gender,
    source.DOB,
    source.Address,
    source.Race,
    source.Ethnicity,
    source.Language_Preference,
    source.MaritalStatus,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------

-- 1. Create transactions Table in BigQuery
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DeptID STRING,
    VisitDate DATE,
    ServiceDate DATE,
    PaidDate DATE,
    VisitType STRING,
    Amount FLOAT64,
    AmountType STRING,
    PaidAmount FLOAT64,
    ClaimID STRING,
    PayorID STRING,
    ProcedureCode INT64,
    ICDCode STRING,
    LineOfBusiness STRING,
    MedicaidID STRING,
    MedicareID STRING,
    SRC_InsertDate INT64,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
)
PARTITION BY VisitDate
CLUSTER BY datasource, PayorID;

-- 2. Create a quality_checks temp table
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
    TransactionID AS SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    SAFE.PARSE_DATE('%Y%m%d', CAST(VisitDate AS STRING)) AS VisitDate,
    SAFE.PARSE_DATE('%Y%m%d', CAST(ServiceDate AS STRING)) AS ServiceDate,
    SAFE.PARSE_DATE('%Y%m%d', CAST(PaidDate AS STRING)) AS PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.transactions_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `carenet-rcm-data-platform.bronze_dataset.transactions_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.transactions` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks` AS source
ON target.Transaction_Key = source.Transaction_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_TransactionID <> source.SRC_TransactionID OR
    target.EncounterID <> source.EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.VisitDate <> source.VisitDate OR
    target.ServiceDate <> source.ServiceDate OR
    target.PaidDate <> source.PaidDate OR
    target.VisitType <> source.VisitType OR
    target.Amount <> source.Amount OR
    target.AmountType <> source.AmountType OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimID <> source.ClaimID OR
    target.PayorID <> source.PayorID OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.ICDCode <> source.ICDCode OR
    target.LineOfBusiness <> source.LineOfBusiness OR
    target.MedicaidID <> source.MedicaidID OR
    target.MedicareID <> source.MedicareID OR
    target.SRC_InsertDate <> source.SRC_InsertDate OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Transaction_Key,
    source.SRC_TransactionID,
    source.EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DeptID,
    source.VisitDate,
    source.ServiceDate,
    source.PaidDate,
    source.VisitType,
    source.Amount,
    source.AmountType,
    source.PaidAmount,
    source.ClaimID,
    source.PayorID,
    source.ProcedureCode,
    source.ICDCode,
    source.LineOfBusiness,
    source.MedicaidID,
    source.MedicareID,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the encounters Table in BigQuery
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.encounters` (
    Encounter_Key STRING,
    SRC_EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate DATE,
    EncounterType STRING,
    ProcedureCode INT64,
    Admit_Source STRING,
    Discharge_Disposition STRING,
    DRG STRING,
    Primary_Diagnosis_Code STRING,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
)
PARTITION BY EncounterDate
CLUSTER BY datasource;

-- 2. Create a quality_checks temp table for encounters
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` AS
SELECT DISTINCT 
    CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    SAFE.PARSE_DATE('%Y%m%d', CAST(EncounterDate AS STRING)) AS EncounterDate,
    EncounterType,
    ProcedureCode,
    'Unknown' AS Admit_Source,
    'Unknown' AS Discharge_Disposition,
    'Unknown' AS DRG,
    'Unknown' AS Primary_Diagnosis_Code,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosa' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.encounters_ha`
    
    UNION ALL
    
    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosb' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.encounters_hb`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.encounters` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters` AS source
ON target.Encounter_Key = source.Encounter_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_EncounterID <> source.SRC_EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DepartmentID <> source.DepartmentID OR
    target.EncounterDate <> source.EncounterDate OR
    target.EncounterType <> source.EncounterType OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.Admit_Source <> source.Admit_Source OR
    target.Discharge_Disposition <> source.Discharge_Disposition OR
    target.DRG <> source.DRG OR
    target.Primary_Diagnosis_Code <> source.Primary_Diagnosis_Code OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    Admit_Source,
    Discharge_Disposition,
    DRG,
    Primary_Diagnosis_Code,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Encounter_Key,
    source.SRC_EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DepartmentID,
    source.EncounterDate,
    source.EncounterType,
    source.ProcedureCode,
    source.Admit_Source,
    source.Discharge_Disposition,
    source.DRG,
    source.Primary_Diagnosis_Code,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_encounters`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the Claims Table in BigQuery
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
    Claim_Type STRING,
    Revenue_Code STRING,
    Place_of_Service STRING,
    PayorType STRING,
    Deductible FLOAT64,
    Coinsurance FLOAT64,
    Copay FLOAT64,
    SRC_InsertDate STRING,
    SRC_ModifiedDate STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
PARTITION BY ServiceDate
CLUSTER BY datasource, PayorID;

-- 2. Create a quality_checks temp table for claims
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` AS
SELECT 
    CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    SAFE.PARSE_DATE('%Y-%m-%d', ServiceDate) AS ServiceDate,
    SAFE.PARSE_DATE('%Y-%m-%d', ClaimDate) AS ClaimDate,
    PayorID,
    SAFE_CAST(ClaimAmount AS FLOAT64) AS ClaimAmount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS PaidAmount,
    ClaimStatus,
    'Professional' AS Claim_Type,
    'Unknown' AS Revenue_Code,
    '11' AS Place_of_Service,
    PayorType,
    SAFE_CAST(Deductible AS FLOAT64) AS Deductible,
    SAFE_CAST(Coinsurance AS FLOAT64) AS Coinsurance,
    SAFE_CAST(Copay AS FLOAT64) AS Copay,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR LOWER(ClaimStatus) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        ClaimID AS SRC_ClaimID,
        TransactionID,
        PatientID,
        EncounterID,
        ProviderID,
        DeptID,
        ServiceDate,
        ClaimDate,
        PayorID,
        ClaimAmount,
        PaidAmount,
        ClaimStatus,
        PayorType,
        Deductible,
        Coinsurance,
        Copay,
        InsertDate,
        ModifiedDate,
        'hosa' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.claims`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.claims` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_claims` AS source
ON target.Claim_Key = source.Claim_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_ClaimID <> source.SRC_ClaimID OR
    target.TransactionID <> source.TransactionID OR
    target.PatientID <> source.PatientID OR
    target.EncounterID <> source.EncounterID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.ServiceDate <> source.ServiceDate OR
    target.ClaimDate <> source.ClaimDate OR
    target.PayorID <> source.PayorID OR
    target.ClaimAmount <> source.ClaimAmount OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimStatus <> source.ClaimStatus OR
    target.Claim_Type <> source.Claim_Type OR
    target.Revenue_Code <> source.Revenue_Code OR
    target.Place_of_Service <> source.Place_of_Service OR
    target.PayorType <> source.PayorType OR
    target.Deductible <> source.Deductible OR
    target.Coinsurance <> source.Coinsurance OR
    target.Copay <> source.Copay OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    Claim_Type,
    Revenue_Code,
    Place_of_Service,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Claim_Key,
    source.SRC_ClaimID,
    source.TransactionID,
    source.PatientID,
    source.EncounterID,
    source.ProviderID,
    source.DeptID,
    source.ServiceDate,
    source.ClaimDate,
    source.PayorID,
    source.ClaimAmount,
    source.PaidAmount,
    source.ClaimStatus,
    source.Claim_Type,
    source.Revenue_Code,
    source.Place_of_Service,
    source.PayorType,
    source.Deductible,
    source.Coinsurance,
    source.Copay,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_claims`;

-------------------------------------------------------------------------------------------------------

-- 1. Create the CPT Codes Silver Table in BigQuery
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
)
CLUSTER BY code_status;

-- 2. Create a quality_checks temp table for CP Codes
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_cpt_codes` AS
SELECT 
    CONCAT(cpt_codes, '-', datasource) AS CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    -- Define a quarantine condition (null values in key fields)
    CASE 
        WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        'hosa' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.cpt_codes`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.cpt_codes` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_cpt_codes` AS source
ON target.CP_Code_Key = source.CP_Code_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.procedure_code_category <> source.procedure_code_category OR
    target.cpt_codes <> source.cpt_codes OR
    target.procedure_code_descriptions <> source.procedure_code_descriptions OR
    target.code_status <> source.code_status OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.CP_Code_Key,
    source.procedure_code_category,
    source.cpt_codes,
    source.procedure_code_descriptions,
    source.code_status,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_cpt_codes`;-------------------------------------------------------------------------------------------------------

-- 1. Create the ICD Codes Silver Table in BigQuery
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.silver_dataset.icd_codes` (
    ICD_Code_Key STRING,
    icd_code STRING,
    icd_code_type STRING,
    code_description STRING,
    inserted_date TIMESTAMP,
    updated_date TIMESTAMP,
    is_current_flag BOOLEAN,
    datasource STRING,
    is_quarantined BOOLEAN,
    modified_date TIMESTAMP
)
CLUSTER BY icd_code_type;

-- 2. Create a quality_checks temp table for ICD Codes
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.silver_dataset.quality_checks_icd_codes` AS
SELECT 
    CONCAT(icd_code, '-', datasource) AS ICD_Code_Key,
    icd_code,
    icd_code_type,
    code_description,
    inserted_date,
    updated_date,
    is_current_flag,
    datasource,
    CASE 
        WHEN icd_code IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        icd_code,
        icd_code_type,
        code_description,
        CAST(inserted_date AS TIMESTAMP) AS inserted_date,
        CAST(updated_date AS TIMESTAMP) AS updated_date,
        is_current_flag,
        'who_api' AS datasource
    FROM `carenet-rcm-data-platform.bronze_dataset.icd_codes`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `carenet-rcm-data-platform.silver_dataset.icd_codes` AS target
USING `carenet-rcm-data-platform.silver_dataset.quality_checks_icd_codes` AS source
ON target.ICD_Code_Key = source.ICD_Code_Key
AND target.is_current_flag = TRUE 

WHEN MATCHED AND (
    target.icd_code_type <> source.icd_code_type OR
    target.code_description <> source.code_description OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current_flag = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED 
THEN INSERT (
    ICD_Code_Key,
    icd_code,
    icd_code_type,
    code_description,
    inserted_date,
    updated_date,
    is_current_flag,
    datasource,
    is_quarantined,
    modified_date
)
VALUES (
    source.ICD_Code_Key,
    source.icd_code,
    source.icd_code_type,
    source.code_description,
    source.inserted_date,
    source.updated_date,
    source.is_current_flag,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP()
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `carenet-rcm-data-platform.silver_dataset.quality_checks_icd_codes`;
