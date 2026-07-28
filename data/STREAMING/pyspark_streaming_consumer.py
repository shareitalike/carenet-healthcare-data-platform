from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, substring, when, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import argparse

# -----------------------------------------------------------
# Enterprise PySpark Structured Streaming Consumer
# Features:
#   1. PHI hashing for HIPAA compliance (defense-in-depth)
#   2. Data quality validation with DLQ routing
#   3. Checkpointed exactly-once processing
#   4. Micro-batch trigger interval for cost optimization
# -----------------------------------------------------------

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
parser.add_argument("--subscription_id", default="carenet-rcm-sub-adt", help="Pub/Sub Subscription ID")
parser.add_argument("--trigger_seconds", type=int, default=300, help="Micro-batch trigger interval in seconds")
args, unknown = parser.parse_known_args()

PROJECT_ID = args.project_id
GCS_BUCKET = args.bucket
SUBSCRIPTION_ID = args.subscription_id

# Initialize Spark Session
spark = (SparkSession.builder
    .appName("CareNet-Enterprise-Streaming-Consumer")
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2")
    .getOrCreate())

# -------------------------------------------------------
# Schema Definition for the new enterprise event payload
# Supports ADT, Claims, and Orders event types
# -------------------------------------------------------
schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("message_control_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("patient_first_name_hash", StringType(), True),
    StructField("patient_last_name_hash", StringType(), True),
    StructField("patient_gender", StringType(), True),
    StructField("patient_address_hash", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("dept_id", StringType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("admit_source", StringType(), True),
    StructField("discharge_disposition", StringType(), True),
    StructField("drg_code", StringType(), True),
    StructField("primary_icd_code", StringType(), True),
    StructField("procedure_code", IntegerType(), True),
    StructField("icd_code", StringType(), True),
    StructField("payor_id", StringType(), True),
    StructField("payor_type", StringType(), True),
    StructField("billed_amount", DoubleType(), True),
    StructField("paid_amount", DoubleType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("revenue_code", StringType(), True),
    StructField("place_of_service", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("priority", StringType(), True),
])

# Read stream from GCP Pub/Sub
pubsub_stream_df = (spark.readStream
    .format("pubsub")
    .option("pubsub.subscription", SUBSCRIPTION_ID)
    .option("pubsub.project", PROJECT_ID)
    .load())

# Parse binary payload data to JSON string, then to structured schema
json_stream_df = pubsub_stream_df.selectExpr("CAST(data AS STRING) as json_payload")
parsed_df = (json_stream_df
    .withColumn("data", from_json(col("json_payload"), schema))
    .select("data.*"))

# -------------------------------------------------------
# Data Quality Validation
# Route invalid records to a Dead Letter Queue table
# -------------------------------------------------------
valid_df = parsed_df.filter(
    col("patient_id").isNotNull() &
    col("event_timestamp").isNotNull() &
    (col("billed_amount").isNull() | (col("billed_amount") >= 0))
)

invalid_df = parsed_df.filter(
    col("patient_id").isNull() |
    col("event_timestamp").isNull() |
    ((col("billed_amount").isNotNull()) & (col("billed_amount") < 0))
).select(
    col("json_payload").alias("raw_payload") if "json_payload" in parsed_df.columns 
    else lit("UNKNOWN").alias("raw_payload"),
    lit("VALIDATION_ERROR").alias("error_type"),
    when(col("patient_id").isNull(), "Missing patient_id")
    .when(col("event_timestamp").isNull(), "Missing event_timestamp")
    .otherwise("Negative billed_amount").alias("error_message"),
    lit("PySpark-ValidateStep").alias("pipeline_stage"),
    current_timestamp().alias("error_timestamp")
)

# -------------------------------------------------------
# PHI Defense-in-Depth: Hash any unhashed PHI fields
# The producer already hashes, but this catches edge cases
# -------------------------------------------------------
secured_df = valid_df
for phi_col in ["patient_first_name", "patient_last_name", "patient_address"]:
    if phi_col in [f.name for f in valid_df.schema.fields]:
        secured_df = secured_df.withColumn(
            phi_col,
            when(col(phi_col).isNotNull(), substring(sha2(col(phi_col), 256), 1, 16))
            .otherwise(col(phi_col))
        )

# -------------------------------------------------------
# Write Valid Records to BigQuery (Main Table)
# Uses a 5-minute micro-batch trigger for cost optimization
# -------------------------------------------------------
target_table = f"{PROJECT_ID}.bronze_dataset.transactions_streaming"
checkpoint_path = f"gs://{GCS_BUCKET}/checkpoint/streaming_transactions/"
temp_bucket = f"{GCS_BUCKET}/temp/"

print(f"📡 Launching Enterprise PySpark Streaming Consumer")
print(f"  Subscription:    {SUBSCRIPTION_ID}")
print(f"  Target Table:    {target_table}")
print(f"  Trigger:         Every {args.trigger_seconds} seconds")
print(f"  Checkpoint:      {checkpoint_path}")

main_query = (secured_df.writeStream
    .format("bigquery")
    .option("table", target_table)
    .option("checkpointLocation", checkpoint_path)
    .option("temporaryGcsBucket", temp_bucket)
    .trigger(processingTime=f"{args.trigger_seconds} seconds")
    .outputMode("append")
    .start())

# -------------------------------------------------------
# Write Invalid Records to Dead Letter Queue Table
# -------------------------------------------------------
dlq_table = f"{PROJECT_ID}.bronze_dataset.streaming_dead_letter_queue"
dlq_checkpoint = f"gs://{GCS_BUCKET}/checkpoint/streaming_dlq/"

dlq_query = (invalid_df.writeStream
    .format("bigquery")
    .option("table", dlq_table)
    .option("checkpointLocation", dlq_checkpoint)
    .option("temporaryGcsBucket", temp_bucket)
    .trigger(processingTime=f"{args.trigger_seconds} seconds")
    .outputMode("append")
    .start())

print(f"  DLQ Table:       {dlq_table}")

# Wait for streams to terminate
spark.streams.awaitAnyTermination()
