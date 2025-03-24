# GCP Manual UI Learning Guide

This guide is designed for **hands-on learning**. Instead of automating the deployment with scripts or Terraform, these instructions walk you through exactly where to click in the Google Cloud Platform (GCP) Console. 

Building this manually will give you the confidence to navigate GCP during technical interviews and understand how the underlying services connect.

---

## Step 1: Enable the Required APIs (The Foundation)
Before you can use any service in GCP, you have to turn it on (enable the API).

1. Log into the [GCP Console](https://console.cloud.google.com/).
2. Make sure your project (`carenet-rcm-data-platform`) is selected in the blue drop-down menu at the very top left.
3. In the top search bar, search for **"APIs & Services"** and click on it.
4. Click the **"+ ENABLE APIS AND SERVICES"** button near the top.
5. You need to search for and click **Enable** for each of the following:
   * **Cloud Storage API** (usually enabled by default)
   * **Cloud SQL Admin API** (Allows us to create our MySQL EMR database)
   * **Secret Manager API** (For securely holding the database password)
   * **Cloud Dataproc API** (For running our PySpark clusters)
   * **Cloud Composer API** (For our Airflow orchestration)
   * **BigQuery API** (Our Data Warehouse)
   * **Cloud Pub/Sub API** (For real-time streaming)

---

## Step 2: Create the Data Lake (Cloud Storage)
We need a place to drop our raw flat files (like Claims and CPT codes) and to store our Airflow DAGs.

1. In the top search bar, type **"Cloud Storage"** and click on **Buckets**.
2. Click the **"+ CREATE"** button.
3. **Name your bucket**: Type `carenet-rcm-data-bucket-` and add a few random numbers to the end (e.g., `carenet-rcm-data-bucket-99`). *Bucket names must be globally unique across all of Google!*
4. **Location type**: Choose **Region** and select `us-central1 (Iowa)`.
5. **Storage class**: Leave as **Standard**.
6. **Access control**: Leave as **Uniform**.
7. Click **Create** (if it asks about Public Access Prevention, confirm it).
8. **Create Folders**: Once the bucket is created, click into it and click **"+ CREATE FOLDER"**. Create the following folders exactly as named:
   * `landing`
   * `configs`
   * `temp`
9. Go inside the `landing` folder and create two more sub-folders: `claims` and `cptcodes`.

---

## Step 3: Simulate the Hospital EMRs (Cloud SQL)
We need a live MySQL database to simulate the hospital's transactional databases.

1. In the top search bar, type **"SQL"** and click on it.
2. Click **"+ CREATE INSTANCE"**.
3. Choose **MySQL**.
4. **Instance ID**: Type `carenet-mysql-instance`.
5. **Password**: Type `mypassword123` (or anything you prefer, just remember it).
6. **Choose configuration**: Select **Development** (to save money on your trial).
7. **Region**: Select `us-central1 (Iowa)`.
8. Expand the **"Show configuration options"** section at the bottom:
   * Under **Machine type**, choose **Shared core** -> **db-f1-micro** (cheapest option).
   * Under **Connections**, check **Public IP** and click **Add a network**. 
   * Name the network `allow-all` and set the Network IP to `0.0.0.0/0`. *(Note: In a real company, we would never open the DB to 0.0.0.0, we would use private IPs, but this is required for our Dataproc cluster to easily reach it during this trial).*
9. Click **CREATE INSTANCE** at the very bottom. *(This will take about 5-10 minutes to spin up).*

---

### Step 3b: Setup the Databases and Users
Once the Cloud SQL instance has a green checkmark next to it:
1. Click on the instance name (`carenet-mysql-instance`).
2. On the left-hand menu, click **Databases**.
3. Click **CREATE DATABASE**, name it `hospital_a_db`, and click Create.
4. Click **CREATE DATABASE** again, name it `hospital_b_db`, and click Create.
5. On the left-hand menu, click **Users**.
6. Click **ADD USER ACCOUNT**.
7. Username: `myuser`, Password: `mypassword123` (or whatever you set earlier). Click Add.

*At this point, your Database is fully alive and ready for connections!*

---

## Step 4: Create the Secret (Secret Manager)
We need to securely store the database credentials so our PySpark code can access them without hardcoding passwords.

1. First, on your Cloud SQL instance page, look at the **Overview** tab and copy your **Public IP address**.
2. In the top search bar, type **"Secret Manager"** and click on it.
3. Click **"+ CREATE SECRET"**.
4. **Name**: Type `mysql-emr-credentials`.
5. Under **Secret value**, copy and paste this exact JSON block (make sure to replace `<YOUR_SQL_IP>` with the IP you copied, and change the password if you used a different one):
```json
{
  "username": "myuser",
  "password": "mypassword123",
  "host": "<YOUR_SQL_IP>",
  "database": "hospital_a_db"
}
```
6. Leave everything else default and click **CREATE SECRET** at the bottom.

---

## Step 5: Setup Airflow Orchestration (Cloud Composer)
1. In the top search bar, type **"Composer"** and click on it.
2. Click **"+ CREATE ENVIRONMENT"** and select **Composer 2**.
3. **Name**: `carenet-composer`.
4. **Location**: `us-central1`.
5. **Environment size**: Choose **Small** (this is very important to keep trial costs low).
6. Click **CREATE**. *(Warning: Cloud Composer takes about 20-25 minutes to build! This is totally normal).*

---

## Step 6: Seed the MySQL Database
While Composer builds, let's inject our mock patient data into the EMR database. The easiest way is using Google Cloud Shell.

1. At the top right of your GCP Console, click the **Activate Cloud Shell** icon (it looks like a little terminal `>_`). 
2. Once the terminal opens at the bottom of your screen, run this command to connect to your database (replace `<YOUR_SQL_IP>`):
   ```bash
   mysql -h <YOUR_SQL_IP> -u myuser -p
   ```
3. Type your password (`mypassword123`) and hit Enter.
4. You should see a `mysql>` prompt. Paste the following SQL script exactly as is and press Enter to run it:

```sql
USE hospital_a_db;

CREATE TABLE IF NOT EXISTS patients (
    PatientID VARCHAR(50), FirstName VARCHAR(50), LastName VARCHAR(50), MiddleName VARCHAR(50),
    SSN VARCHAR(50), PhoneNumber VARCHAR(50), Gender VARCHAR(50), DOB INT, Address VARCHAR(150), ModifiedDate VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS encounters (
    EncounterID VARCHAR(50), PatientID VARCHAR(50), ProviderID VARCHAR(50), DepartmentID VARCHAR(50),
    EncounterDate INT, EncounterType VARCHAR(50), ProcedureCode INT, ModifiedDate VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS transactions (
    TransactionID VARCHAR(50), EncounterID VARCHAR(50), PatientID VARCHAR(50), ProviderID VARCHAR(50), DeptID VARCHAR(50),
    VisitDate INT, ServiceDate INT, PaidDate INT, VisitType VARCHAR(50), Amount FLOAT, AmountType VARCHAR(50), PaidAmount FLOAT,
    ClaimID VARCHAR(50), PayorID VARCHAR(50), ProcedureCode INT, ICDCode VARCHAR(50), LineOfBusiness VARCHAR(50),
    MedicaidID VARCHAR(50), MedicareID VARCHAR(50), InsertDate INT, ModifiedDate VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS providers (
    ProviderID VARCHAR(50), FirstName VARCHAR(50), LastName VARCHAR(50), Specialization VARCHAR(50), DeptID VARCHAR(50), NPI VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS departments (
    deptid VARCHAR(50), Name VARCHAR(50)
);

INSERT INTO patients VALUES ('P001', 'John', 'Doe', 'A', '999-88-6789', '312-555-0199', 'Male', 19800101, '123 Elm St, Chicago, IL', '2026-05-01 10:00:00');
INSERT INTO providers VALUES ('101', 'Alice', 'Smith', 'Cardiology', '1', '1234567890');
INSERT INTO departments VALUES ('1', 'Cardiology');
INSERT INTO encounters VALUES ('E1001', 'P001', '101', '1', 20260501, 'Outpatient', 99213, '2026-05-01 10:30:00');
INSERT INTO transactions VALUES ('T50001', 'E1001', 'P001', '101', '1', 20260501, 20260501, NULL, 'Outpatient', 150.00, 'Charge', 0.00, 'CLM90001', 'PAY801', 99213, 'A09', 'Medicare', NULL, 'MC888999', 20260501, '2026-05-01 11:00:00');
```
5. Type `exit` to leave the MySQL prompt. Your EMR database is now populated!

