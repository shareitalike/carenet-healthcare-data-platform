--------------------------------------------------------------------------------------------------
-- ENTERPRISE HEALTHCARE BIGQUERY GOLD LAYER SUITE (MULTI-INSTANCE EPIC CLARITY)
-- 15 Specialized Curated Data Marts for Looker Studio, BI Teams & C-Suite Executives
--------------------------------------------------------------------------------------------------

-- ===============================================================================================
-- PILLAR 1: REVENUE CYCLE MANAGEMENT (RCM) & FINANCIALS
-- ===============================================================================================

-- 1. RCM Executive Summary (Target: CFO, VP of Finance)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.rcm_executive_summary`
PARTITION BY ServiceDate
CLUSTER BY datasource, PayorType AS
SELECT 
    c.ServiceDate,
    c.datasource,
    c.PayorType,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaimsSubmitted,
    COUNT(DISTINCT c.PatientID) AS UniquePatientsBilled,
    ROUND(SUM(c.ClaimAmount), 2) AS TotalBilledAmount,
    ROUND(SUM(c.PaidAmount), 2) AS TotalCollectedAmount,
    ROUND(SUM(c.ClaimAmount - c.PaidAmount), 2) AS TotalOutstandingBalance,
    ROUND(SAFE_DIVIDE(SUM(c.PaidAmount), SUM(c.ClaimAmount)) * 100, 2) AS NetCollectionRatePct,
    ROUND(AVG(c.DaysToSettle), 1) AS AvgDaysToSettle,
    COUNTIF(c.ClaimStatus = 'DENIED') AS DeniedClaimCount,
    ROUND(SAFE_DIVIDE(COUNTIF(c.ClaimStatus = 'DENIED'), COUNT(c.Claim_Key)) * 100, 2) AS DenialRatePct
FROM `carenet-rcm-data-platform.silver_dataset.claims` c
WHERE c.is_current = TRUE AND c.is_quarantined = FALSE
GROUP BY c.ServiceDate, c.datasource, c.PayorType;

-- 2. Claim Denials & Root-Cause Audit (Target: Revenue Recovery, Billing Compliance)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.claim_denials_audit`
PARTITION BY ServiceDate
CLUSTER BY DenialReasonCode, PayorID AS
SELECT 
    c.ServiceDate,
    c.datasource,
    c.PayorID,
    c.DenialReasonCode,
    c.CARC_Description,
    pr.Specialization AS BillingSpecialty,
    COUNT(c.Claim_Key) AS DeniedCount,
    ROUND(SUM(c.ClaimAmount), 2) AS LostRevenueAmount,
    ROUND(AVG(c.ClaimAmount), 2) AS AvgClaimDenialSize
FROM `carenet-rcm-data-platform.silver_dataset.claims` c
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.providers` pr
    ON c.ProviderID = pr.SRC_ProviderID AND c.datasource = pr.datasource AND pr.is_current = TRUE
WHERE c.ClaimStatus = 'DENIED' AND c.is_current = TRUE AND c.is_quarantined = FALSE
GROUP BY c.ServiceDate, c.datasource, c.PayorID, c.DenialReasonCode, c.CARC_Description, pr.Specialization;

-- 3. Days in A/R and Aging Buckets (Target: Director of Patient Financial Services)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.days_in_ar_and_aging`
CLUSTER BY AgingBucket, PayorID AS
SELECT 
    c.Claim_Key,
    c.SRC_ClaimID,
    c.datasource,
    c.PayorID,
    c.PayorType,
    c.ServiceDate,
    DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) AS DaysInAR,
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) <= 30 THEN '0-30 Days (Current)'
        WHEN DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) <= 60 THEN '31-60 Days (Aging)'
        WHEN DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) <= 90 THEN '61-90 Days (Delinquent)'
        WHEN DATE_DIFF(CURRENT_DATE(), c.ServiceDate, DAY) <= 120 THEN '91-120 Days (Severe)'
        ELSE '120+ Days (Write-Off Risk)'
    END AS AgingBucket,
    ROUND(c.ClaimAmount, 2) AS BilledAmount,
    ROUND(c.PaidAmount, 2) AS PaidAmount,
    ROUND(c.ClaimAmount - c.PaidAmount, 2) AS UncollectedBalance
FROM `carenet-rcm-data-platform.silver_dataset.claims` c
WHERE c.ClaimStatus IN ('PENDING_ADJUDICATION', 'DENIED') 
  AND c.is_current = TRUE 
  AND c.is_quarantined = FALSE;

-- 4. Payer Performance & Settlement Scorecard (Target: Managed Care Contracting)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.payer_performance_scorecard`
CLUSTER BY PayorID, PayorType AS
SELECT 
    c.PayorID,
    c.PayorType,
    c.datasource,
    COUNT(c.Claim_Key) AS TotalClaimsReceived,
    ROUND(SUM(c.ClaimAmount), 2) AS TotalClaimedValue,
    ROUND(SUM(c.PaidAmount), 2) AS TotalPaidValue,
    ROUND(SAFE_DIVIDE(SUM(c.PaidAmount), SUM(c.ClaimAmount)) * 100, 2) AS ReimbursementYieldPct,
    ROUND(AVG(c.DaysToSettle), 1) AS AvgTurnaroundDays,
    COUNTIF(c.ClaimStatus = 'DENIED') AS DeniedCount,
    ROUND(SAFE_DIVIDE(COUNTIF(c.ClaimStatus = 'DENIED'), COUNT(c.Claim_Key)) * 100, 2) AS DenialPct
FROM `carenet-rcm-data-platform.silver_dataset.claims` c
WHERE c.is_current = TRUE AND c.is_quarantined = FALSE
GROUP BY c.PayorID, c.PayorType, c.datasource;

-- 5. Out-of-Pocket Patient Responsibility & Collections (Target: Patient Billing Manager)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.patient_financial_responsibility`
PARTITION BY ServiceDate
CLUSTER BY PatientState AS
SELECT 
    t.ServiceDate,
    t.datasource,
    p.State AS PatientState,
    COUNT(DISTINCT t.Transaction_Key) AS BilledChargeCount,
    COUNT(DISTINCT t.PatientID) AS UniquePatientsCount,
    ROUND(SUM(t.Copay), 2) AS ExpectedCopayTotal,
    ROUND(SUM(t.Coinsurance), 2) AS ExpectedCoinsuranceTotal,
    ROUND(SUM(t.Deductible), 2) AS ExpectedDeductibleTotal,
    ROUND(SUM(t.Copay + t.Coinsurance + t.Deductible), 2) AS TotalPatientResponsibilityAmount,
    ROUND(SUM(t.PaidAmount), 2) AS ActualPatientPaidAmount,
    ROUND(SAFE_DIVIDE(SUM(t.PaidAmount), SUM(t.Copay + t.Coinsurance + t.Deductible)) * 100, 2) AS PatientCollectionEfficiencyPct
FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.patients` p
    ON t.PatientID = p.SRC_PatientID AND t.datasource = p.datasource AND p.is_current = TRUE
WHERE t.is_current = TRUE AND t.is_quarantined = FALSE
GROUP BY t.ServiceDate, t.datasource, p.State;


-- ===============================================================================================
-- PILLAR 2: CLINICAL OPERATIONS & PATIENT JOURNEY
-- ===============================================================================================

-- 6. Inpatient Bed Utilization & Census (Target: Chief Operating Officer, Nurse Managers)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.inpatient_census_utilization`
PARTITION BY EncounterDate
CLUSTER BY DepartmentName, datasource AS
SELECT 
    e.EncounterDate,
    e.datasource,
    COALESCE(d.Name, 'General Medicine') AS DepartmentName,
    COALESCE(d.Location, 'Main Tower') AS FacilityLocation,
    COUNT(DISTINCT e.Encounter_Key) AS ActiveInpatientAdmissions,
    ROUND(AVG(e.LengthOfStay), 1) AS AvgLengthOfStayDays,
    MAX(e.LengthOfStay) AS MaxLengthOfStayDays,
    COUNTIF(e.LengthOfStay > 7) AS LongStayOutliersCount
FROM `carenet-rcm-data-platform.silver_dataset.encounters` e
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.departments` d
    ON e.DepartmentID = d.SRC_DepartmentID AND e.datasource = d.datasource AND d.is_current = TRUE
WHERE e.EncounterType = 'Inpatient' AND e.is_current = TRUE AND e.is_quarantined = FALSE
GROUP BY e.EncounterDate, e.datasource, d.Name, d.Location;

-- 7. 30-Day Readmission Risk & Penalties (Target: Chief Medical Officer, Quality Committee)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.thirty_day_readmission_risk`
CLUSTER BY DischargeYearMonth, datasource AS
WITH encounter_ranks AS (
    SELECT 
        Encounter_Key,
        SRC_EncounterID,
        PatientID,
        EncounterDate,
        DischargeDate,
        EncounterType,
        datasource,
        FORMAT_DATE('%Y-%m', EncounterDate) AS DischargeYearMonth,
        LAG(DischargeDate) OVER (PARTITION BY PatientID, datasource ORDER BY EncounterDate) AS PrevDischargeDate
    FROM `carenet-rcm-data-platform.silver_dataset.encounters`
    WHERE EncounterType = 'Inpatient' AND is_current = TRUE AND is_quarantined = FALSE
)
SELECT 
    DischargeYearMonth,
    datasource,
    COUNT(DISTINCT Encounter_Key) AS TotalInpatientDischarges,
    COUNTIF(DATE_DIFF(EncounterDate, PrevDischargeDate, DAY) <= 30) AS ReadmittedWithin30DaysCount,
    ROUND(SAFE_DIVIDE(COUNTIF(DATE_DIFF(EncounterDate, PrevDischargeDate, DAY) <= 30), COUNT(DISTINCT Encounter_Key)) * 100, 2) AS ReadmissionRatePct
FROM encounter_ranks
GROUP BY DischargeYearMonth, datasource;

-- 8. Emergency Department (ED) Throughput (Target: ED Medical Director)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.emergency_department_throughput`
PARTITION BY EncounterDate
CLUSTER BY datasource, DischargeDisposition AS
SELECT 
    e.EncounterDate,
    e.datasource,
    COUNT(e.Encounter_Key) AS TotalEDVisits,
    COUNTIF(e.DischargeDisposition = 'HOME') AS DischargedHomeCount,
    COUNTIF(e.DischargeDisposition = 'TRANSFER') AS TransferredCount,
    COUNTIF(e.DischargeDisposition = 'ADMITTED') AS AdmittedToInpatientCount,
    ROUND(SAFE_DIVIDE(COUNTIF(e.DischargeDisposition = 'ADMITTED'), COUNT(e.Encounter_Key)) * 100, 2) AS EDAdmissionConversionRatePct
FROM `carenet-rcm-data-platform.silver_dataset.encounters` e
WHERE e.EncounterType = 'Emergency' AND e.is_current = TRUE AND e.is_quarantined = FALSE
GROUP BY e.EncounterDate, e.datasource, e.DischargeDisposition;

-- 9. Clinical Department Productivity & Workload (Target: Clinical Operations Director)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.department_clinical_productivity`
PARTITION BY ServiceDate
CLUSTER BY DepartmentName, datasource AS
SELECT 
    t.ServiceDate,
    t.datasource,
    d.Name AS DepartmentName,
    d.Location AS ClinicLocation,
    COUNT(DISTINCT t.Transaction_Key) AS TotalProceduresPerformed,
    COUNT(DISTINCT t.ProviderID) AS ActivePhysiciansOnDuty,
    ROUND(SUM(t.Amount), 2) AS TotalGrossChargesGenerated,
    ROUND(SAFE_DIVIDE(SUM(t.Amount), COUNT(DISTINCT t.ProviderID)), 2) AS RevenuePerPhysician
FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.departments` d
    ON t.DeptID = d.SRC_DepartmentID AND t.datasource = d.datasource AND d.is_current = TRUE
WHERE t.is_current = TRUE AND t.is_quarantined = FALSE
GROUP BY t.ServiceDate, t.datasource, d.Name, d.Location;

-- 10. Patient Geographic Demographics & Health Equity (Target: Population Health, Outreach)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.patient_health_equity_demographics`
CLUSTER BY State, Race, Gender AS
SELECT 
    p.State,
    p.City,
    p.ZipCode,
    p.Gender,
    p.Race,
    p.MaritalStatus,
    p.Language,
    p.datasource,
    COUNT(DISTINCT p.Patient_Key) AS UniquePatientPopulation,
    ROUND(AVG(DATE_DIFF(CURRENT_DATE(), p.DOB, YEAR)), 1) AS AvgPatientAgeYears,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncountersUtilized
FROM `carenet-rcm-data-platform.silver_dataset.patients` p
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.encounters` e
    ON p.SRC_PatientID = e.PatientID AND p.datasource = e.datasource AND e.is_current = TRUE
WHERE p.is_current = TRUE AND p.is_quarantined = FALSE
GROUP BY p.State, p.City, p.ZipCode, p.Gender, p.Race, p.MaritalStatus, p.Language, p.datasource;


-- ===============================================================================================
-- PILLAR 3: PHYSICIAN PRODUCTIVITY, QUALITY & VALUE-BASED CARE
-- ===============================================================================================

-- 11. Physician RVU & Clinical Productivity (Target: VP of Medical Affairs, Chief of Staff)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.physician_rvu_productivity`
PARTITION BY ServiceDate
CLUSTER BY ProviderSpecialty, ProviderNPI AS
SELECT 
    t.ServiceDate,
    t.datasource,
    pr.NPI AS ProviderNPI,
    CONCAT(pr.FirstName, ' ', pr.LastName) AS ProviderFullName,
    pr.Specialization AS ProviderSpecialty,
    COUNT(DISTINCT t.Transaction_Key) AS BilledServiceCount,
    COUNT(DISTINCT t.EncounterID) AS PatientEncounterCount,
    ROUND(SUM(t.Amount), 2) AS TotalGrossChargesBilled,
    ROUND(SUM(t.PaidAmount), 2) AS TotalRevenueCollected,
    ROUND(SUM(CASE 
        WHEN t.CPTCode IN ('99213', '99214') THEN 1.5
        WHEN t.CPTCode IN ('99285', '99291') THEN 4.5
        ELSE 1.0
    END), 2) AS EstimatedWorkRVUs
FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.providers` pr
    ON t.ProviderID = pr.SRC_ProviderID AND t.datasource = pr.datasource AND pr.is_current = TRUE
WHERE t.is_current = TRUE AND t.is_quarantined = FALSE
GROUP BY t.ServiceDate, t.datasource, pr.NPI, pr.FirstName, pr.LastName, pr.Specialization;

-- 12. Top 50 High-Volume ICD-10 & CPT Clinical Service Lines (Target: Service Line Directors)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.service_line_procedure_analytics`
CLUSTER BY CPTCode, ICD10_Diagnosis AS
SELECT 
    t.CPTCode,
    t.ICD10_Diagnosis,
    t.datasource,
    COUNT(t.Transaction_Key) AS ProcedureFrequencyCount,
    COUNT(DISTINCT t.PatientID) AS DistinctPatientsTreated,
    ROUND(SUM(t.Amount), 2) AS TotalBilledCharges,
    ROUND(SUM(t.PaidAmount), 2) AS TotalReimbursementPaid,
    ROUND(AVG(t.Amount), 2) AS AvgChargePerProcedure,
    ROUND(SAFE_DIVIDE(SUM(t.PaidAmount), SUM(t.Amount)) * 100, 2) AS RealizedReimbursementRatePct
FROM `carenet-rcm-data-platform.silver_dataset.transactions` t
WHERE t.is_current = TRUE AND t.is_quarantined = FALSE
GROUP BY t.CPTCode, t.ICD10_Diagnosis, t.datasource;

-- 13. Physician Clean Claim Rate & Denial Accountability (Target: Billing Compliance Officer)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.physician_claim_denial_scorecard`
CLUSTER BY ProviderNPI, Specialty AS
SELECT 
    pr.NPI AS ProviderNPI,
    CONCAT(pr.FirstName, ' ', pr.LastName) AS ProviderFullName,
    pr.Specialization AS Specialty,
    c.datasource,
    COUNT(c.Claim_Key) AS TotalClaimsSubmitted,
    COUNTIF(c.ClaimStatus = 'SETTLED_PAID_FULL') AS CleanClaimsPaidFirstPass,
    COUNTIF(c.ClaimStatus = 'DENIED') AS DeniedClaimsCount,
    ROUND(SAFE_DIVIDE(COUNTIF(c.ClaimStatus = 'SETTLED_PAID_FULL'), COUNT(c.Claim_Key)) * 100, 2) AS CleanClaimRatePct,
    ROUND(SAFE_DIVIDE(COUNTIF(c.ClaimStatus = 'DENIED'), COUNT(c.Claim_Key)) * 100, 2) AS DenialRatePct,
    ROUND(SUM(CASE WHEN c.ClaimStatus = 'DENIED' THEN c.ClaimAmount ELSE 0 END), 2) AS TotalDeniedClaimDollars
FROM `carenet-rcm-data-platform.silver_dataset.claims` c
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.providers` pr
    ON c.ProviderID = pr.SRC_ProviderID AND c.datasource = pr.datasource AND pr.is_current = TRUE
WHERE c.is_current = TRUE AND c.is_quarantined = FALSE
GROUP BY pr.NPI, pr.FirstName, pr.LastName, pr.Specialization, c.datasource;

-- 14. Data Quality Quarantine & Error Observability Mart (Target: Data Engineering Lead, DataOps)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.data_quality_pipeline_observability`
PARTITION BY DATE(inserted_date)
CLUSTER BY TableName, datasource AS
SELECT 
    'patients' AS TableName,
    datasource,
    DATE(inserted_date) AS ExecutionDate,
    COUNT(Patient_Key) AS TotalIngestedRows,
    COUNTIF(is_quarantined = TRUE) AS QuarantinedErrorRows,
    ROUND(SAFE_DIVIDE(COUNTIF(is_quarantined = TRUE), COUNT(Patient_Key)) * 100, 2) AS ErrorRatePct
FROM `carenet-rcm-data-platform.silver_dataset.patients`
GROUP BY datasource, ExecutionDate

UNION ALL

SELECT 
    'encounters' AS TableName,
    datasource,
    DATE(inserted_date) AS ExecutionDate,
    COUNT(Encounter_Key) AS TotalIngestedRows,
    COUNTIF(is_quarantined = TRUE) AS QuarantinedErrorRows,
    ROUND(SAFE_DIVIDE(COUNTIF(is_quarantined = TRUE), COUNT(Encounter_Key)) * 100, 2) AS ErrorRatePct
FROM `carenet-rcm-data-platform.silver_dataset.encounters`
GROUP BY datasource, ExecutionDate

UNION ALL

SELECT 
    'claims' AS TableName,
    datasource,
    DATE(inserted_date) AS ExecutionDate,
    COUNT(Claim_Key) AS TotalIngestedRows,
    COUNTIF(is_quarantined = TRUE) AS QuarantinedErrorRows,
    ROUND(SAFE_DIVIDE(COUNTIF(is_quarantined = TRUE), COUNT(Claim_Key)) * 100, 2) AS ErrorRatePct
FROM `carenet-rcm-data-platform.silver_dataset.claims`
GROUP BY datasource, ExecutionDate;

-- 15. Cross-Campus M&A Standardization & Volume Tracking (Target: Health System Board)
CREATE OR REPLACE TABLE `carenet-rcm-data-platform.gold_dataset.cross_campus_operational_benchmark`
CLUSTER BY datasource, EncounterType AS
SELECT 
    e.datasource,
    e.EncounterType,
    COUNT(DISTINCT e.PatientID) AS UniquePatientsServed,
    COUNT(DISTINCT e.Encounter_Key) AS TotalPatientVisits,
    ROUND(AVG(e.LengthOfStay), 1) AS AvgLengthOfStay,
    ROUND(SUM(t.Amount), 2) AS TotalGrossChargesBilled,
    ROUND(SUM(t.PaidAmount), 2) AS TotalReimbursementCollected,
    ROUND(SAFE_DIVIDE(SUM(t.PaidAmount), SUM(t.Amount)) * 100, 2) AS OverallCampusCollectionRatePct
FROM `carenet-rcm-data-platform.silver_dataset.encounters` e
LEFT JOIN `carenet-rcm-data-platform.silver_dataset.transactions` t
    ON e.SRC_EncounterID = t.EncounterID AND e.datasource = t.datasource AND t.is_current = TRUE
WHERE e.is_current = TRUE AND e.is_quarantined = FALSE
GROUP BY e.datasource, e.EncounterType;
