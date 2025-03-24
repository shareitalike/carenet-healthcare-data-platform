from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import argparse

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
parser.add_argument("--subscription_id", default="carenet-rcm-transactions-sub", help="Pub/Sub Subscription ID")
args, unknown = parser.parse_known_args()

PROJECT_ID = args.project_id
GCS_BUCKET = args.bucket
SUBSCRIPTION_ID = args.subscription_id

# Initialize Spark Session
spark = (SparkSession.builder
    .appName("GCP-PubSub-BigQuery-Streaming")
    # Set GCP Pub/Sub dependency packages configuration
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2")
    .getOrCreate())

# Define JSON payload schema
schema = StructType([
    StructField("event_timestamp", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("patient_first_name", StringType(), True),
    StructField("patient_last_name", StringType(), True),
    StructField("patient_gender", StringType(), True),
    StructField("patient_address", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("dept_id", StringType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("procedure_code", IntegerType(), True),
    StructField("icd_code", StringType(), True),
    StructField("payor_id", StringType(), True),
    StructField("payor_type", StringType(), True),
    StructField("billed_amount", DoubleType(), True),
    StructField("claim_id", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_paid_amount", DoubleType(), True)
])

# Read stream from GCP Pub/Sub
# Note: Spark uses 'pubsub' format on Dataproc
pubsub_stream_df = (spark.readStream
    .format("pubsub")
    .option("pubsub.subscription", SUBSCRIPTION_ID)
    .option("pubsub.project", PROJECT_ID)
    .load())

# Parse binary payload data to JSON string
json_stream_df = pubsub_stream_df.selectExpr("CAST(data AS STRING) as json_payload")

# Parse JSON to structured schema
parsed_df = (json_stream_df
    .withColumn("data", from_json(col("json_payload"), schema))
    .select("data.*"))

# Write Stream to BigQuery using the BigQuery Storage Write API
target_table = f"{PROJECT_ID}.bronze_dataset.transactions_streaming"
checkpoint_path = f"gs://{GCS_BUCKET}/checkpoint/streaming_transactions/"
temp_bucket = f"{GCS_BUCKET}/temp/"

print(f"📡 Launching PySpark Structured Streaming from Subscription: {SUBSCRIPTION_ID}")
print(f"Writing streaming records directly to BigQuery table: {target_table}")

query = (parsed_df.writeStream
    .format("bigquery")
    .option("table", target_table)
    .option("checkpointLocation", checkpoint_path)
    .option("temporaryGcsBucket", temp_bucket)
    .outputMode("append")
    .start())

# Wait for stream to terminate
query.awaitTermination()
