from google.cloud import storage, bigquery, secretmanager
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import datetime
import json
import argparse

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
parser.add_argument("--execution_date", default=None, help="Execution date YYYY-MM-DD")
args, unknown = parser.parse_known_args()

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = SparkSession.builder.appName("EpicClarityMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = args.bucket
HOSPITAL_NAME = "epic-clarity"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# BigQuery Configuration
BQ_PROJECT = args.project_id
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"  

def get_secret(secret_id, project_id):
    """Fetches secrets from Google Cloud Secret Manager"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error reading secret {secret_id}: {str(e)}")
        return None

# Fetch DB credentials
secret_data = get_secret("mysql-emr-credentials", BQ_PROJECT)
if secret_data:
    try:
        db_creds = json.loads(secret_data)
        db_user = db_creds.get("username", "myuser")
        db_pass = db_creds.get("password", "mypass")
        db_host = db_creds.get("host", "34.132.104.87")
    except Exception as e:
        print(f"Error parsing secret JSON: {str(e)}")
        db_user, db_pass, db_host = "myuser", "mypass", "34.132.104.87"
else:
    db_user, db_pass, db_host = "myuser", "mypass", "34.132.104.87"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": f"jdbc:mysql://{db_host}:3306/epic_clarity_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": db_user,
    "password": db_pass
}

##------------------------------------------------------------------------------------------------------------------##
# Logging Mechanism
log_entries = []  # Stores logs before writing to GCS

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")  # Print for visibility
    
def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"  
    
    json_data = json.dumps(log_entries, indent=4)

    # Get GCS bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    
    # Upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    """Save logs to BigQuery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print("✅ Logs stored in BigQuery for future analysis")
    
##------------------------------------------------------------------------------------------------------------------##

# Function to Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extract Date from File Name (from folder structure like table_YYYYMMDD)
        folder_name = file.split("/")[-2]
        date_part = folder_name.split("_")[-1]
        
        # If it's 8 digits YYYYMMDD
        if len(date_part) == 8 and date_part.isdigit():
            year, month, day = date_part[:4], date_part[4:6], date_part[6:8]
        else:
            # Fallback
            year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        # Move to Archive
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        log_event("INFO", f"Moved {file} to {archive_path}", table=table)
        
##------------------------------------------------------------------------------------------------------------------##

# Function to Get Latest Watermark from BigQuery Audit Table
def get_latest_watermark(table_name):
    try:
        query = f"""
            SELECT MAX(load_timestamp) AS latest_timestamp
            FROM `{BQ_AUDIT_TABLE}`
            WHERE tablename = '{table_name}' and data_source = "epic_clarity_db"
        """
        query_job = bq_client.query(query)
        result = query_job.result()
        for row in result:
            return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    except Exception as e:
        print(f"Error fetching watermark (table might not exist yet): {str(e)}")
    return "1900-01-01 00:00:00"

##------------------------------------------------------------------------------------------------------------------##

# Schema Registry & Drift Handling
def load_registered_schema(table_name):
    """Loads the registered schema from GCS."""
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"registry/schemas/{table_name}.json")
    if not blob.exists():
        return None
    schema_json = blob.download_as_text()
    return StructType.fromJson(json.loads(schema_json))

def save_registered_schema(table_name, schema):
    """Saves the schema registry file in GCS."""
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"registry/schemas/{table_name}.json")
    blob.upload_from_string(json.dumps(schema.jsonValue()))

def handle_schema_drift(table_name, jdbc_df):
    """Compares incoming schema with registry and logs/handles drift."""
    try:
        registered_schema = load_registered_schema(table_name)
        if registered_schema is None:
            log_event("INFO", f"First-time ingestion for {table_name}. Registering schema.", table=table_name)
            save_registered_schema(table_name, jdbc_df.schema)
            return jdbc_df
            
        incoming_fields = set(jdbc_df.schema.names)
        registered_fields = set(registered_schema.names)
        
        new_columns = incoming_fields - registered_fields
        if new_columns:
            log_event("WARNING", f"Schema Drift Detected: New columns found: {new_columns}", table=table_name)
            evolved_schema = StructType(list(registered_schema) + [jdbc_df.schema[col] for col in new_columns])
            save_registered_schema(table_name, evolved_schema)
            
        missing_columns = registered_fields - incoming_fields
        if missing_columns:
            log_event("WARNING", f"Schema Drift Detected: Missing columns: {missing_columns}", table=table_name)
            from pyspark.sql.functions import lit
            for col in missing_columns:
                jdbc_df = jdbc_df.withColumn(col, lit(None).cast(registered_schema[col].dataType))
                
        return jdbc_df
    except Exception as e:
        log_event("ERROR", f"Error during schema drift handling for {table_name}: {str(e)}", table=table_name)
        return jdbc_df

##------------------------------------------------------------------------------------------------------------------##

# Function to Extract Data from MySQL and Save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        # Use provided execution date or fetch watermark from BigQuery
        if args.execution_date:
            last_watermark = f"{args.execution_date} 00:00:00"
            log_event("INFO", f"Using parameter execution date watermark: {last_watermark}", table=table)
        else:
            last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
            log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        if load_type.lower() == "full" or last_watermark is None:
            query = f"(SELECT * FROM {table}) AS t"
        else:
            query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())

        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)

        # Run schema drift detection
        df = handle_schema_drift(table, df)

        # Determine output path partitioned by run date (YYYYMMDD for chronological sorting)
        run_day = args.execution_date.replace("-", "") if args.execution_date else datetime.datetime.today().strftime('%Y%m%d')
        PARQUET_DIR_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/{table}/{table}_{run_day}/"

        # Use PySpark native distributed Parquet writer (eliminates driver OOM risk)
        df.write.format("parquet").mode("overwrite").save(PARQUET_DIR_PATH)

        log_event("SUCCESS", f"✅ Parquet files successfully written to {PARQUET_DIR_PATH}", table=table)
        
        # Insert Audit Entry
        audit_df = spark.createDataFrame([
            ("epic_clarity_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")], 
            ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_PATH)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)
##------------------------------------------------------------------------------------------------------------------##

# Function to Read Config File from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")
    return df

# read config file
config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "epic_clarity_db": 
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)
        
save_logs_to_gcs()
try:
    save_logs_to_bigquery()
except Exception as e:
    print(f"Skipping log insertion to BigQuery (audit tables might not be ready): {str(e)}")