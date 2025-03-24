from pyspark.sql import SparkSession
import argparse

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
args, unknown = parser.parse_known_args()

# Create Spark session
spark = SparkSession.builder \
                    .appName("CPT Codes Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = args.bucket
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = f"{args.project_id}.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from cpt
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# replace spaces with underscore
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())
print(f"✅ CPT Codes successfully loaded into: {BQ_TABLE}")