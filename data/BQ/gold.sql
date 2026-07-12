--1. Total Charge Amount per provider by department
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.gold_dataset.provider_charge_summary` (
    Provider_Name STRING,
    Dept_Name STRING,
    Amount FLOAT64
)
CLUSTER BY Dept_Name;

# truncate table
TRUNCATE TABLE `carenet-rcm-data-platform.gold_dataset.provider_charge_summary`;

# insert data
INSERT INTO `carenet-rcm-data-platform.gold_dataset.provider_charge_summary`
SELECT 
    CONCAT(p.firstname, ' ', p.LastName) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(t.Amount) AS Amount
FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.providers` p 
    ON SPLIT(p.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.departments` d 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = p.DeptID
WHERE t.is_quarantined = FALSE AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;


--------------------------------------------------------------------------------------------------
--2. Patient History (Gold) : This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.gold_dataset.patient_history` (
    Patient_Key STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB DATE,
    Address STRING,
    EncounterDate DATE,
    EncounterType STRING,
    Admit_Source STRING,
    Discharge_Disposition STRING,
    DRG STRING,
    Primary_Diagnosis_Code STRING,
    Transaction_Key STRING,
    VisitDate DATE,
    ServiceDate DATE,
    Length_of_Stay_Days INT64,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount FLOAT64,
    ClaimPaidAmount FLOAT64,
    PayorType STRING
)
PARTITION BY VisitDate
CLUSTER BY PayorType;


# TRUNCATE TABLE
TRUNCATE TABLE `carenet-rcm-data-platform.gold_dataset.patient_history`;

# INSERT DATA
INSERT INTO `carenet-rcm-data-platform.gold_dataset.patient_history`
SELECT 
    p.Patient_Key,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    e.Admit_Source,
    e.Discharge_Disposition,
    e.DRG,
    e.Primary_Diagnosis_Code,
    t.Transaction_Key,
    t.VisitDate,
    t.ServiceDate,
    DATE_DIFF(t.ServiceDate, t.VisitDate, DAY) AS Length_of_Stay_Days,
    t.Amount AS BilledAmount,
    t.PaidAmount,
    c.ClaimStatus,
    c.ClaimAmount,
    c.PaidAmount AS ClaimPaidAmount,
    c.PayorType
FROM `carenet-rcm-data-platform.silver_dataset.patients` p
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.encounters` e 
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.transactions` t 
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;


--------------------------------------------------------------------------------------------------
-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
)
CLUSTER BY Specialization;

# TRUNCATE TABLE
TRUNCATE TABLE `carenet-rcm-data-platform.gold_dataset.provider_performance`;

# INSERT DATA
INSERT INTO `carenet-rcm-data-platform.gold_dataset.provider_performance`
SELECT 
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `carenet-rcm-data-platform.silver_dataset.providers` pr
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.encounters` e 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.transactions` t 
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;

--------------------------------------------------------------------------------------------------
-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
)
CLUSTER BY DepartmentName;

# TRUNCATE TABLE
TRUNCATE TABLE `carenet-rcm-data-platform.gold_dataset.department_performance`;

# INSERT DATA
INSERT INTO `carenet-rcm-data-platform.gold_dataset.department_performance`
SELECT 
    d.Dept_Id,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `carenet-rcm-data-platform.silver_dataset.departments` d
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.encounters` e 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.transactions` t 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;

--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- 5. Claims Analytics (Gold): Detailed claims information incorporating US standard fields.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `carenet-rcm-data-platform.gold_dataset.claims_analytics` (
    Claim_Key STRING,
    Claim_Type STRING,
    Revenue_Code STRING,
    Place_of_Service STRING,
    PayorType STRING,
    ClaimAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING
)
CLUSTER BY PayorType;

# TRUNCATE TABLE
TRUNCATE TABLE `carenet-rcm-data-platform.gold_dataset.claims_analytics`;

# INSERT DATA
INSERT INTO `carenet-rcm-data-platform.gold_dataset.claims_analytics`
SELECT 
    Claim_Key,
    Claim_Type,
    Revenue_Code,
    Place_of_Service,
    PayorType,
    ClaimAmount,
    PaidAmount,
    ClaimStatus
FROM `carenet-rcm-data-platform.silver_dataset.claims`
WHERE is_current = TRUE;
