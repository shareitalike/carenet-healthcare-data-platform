from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when
import argparse

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
args, unknown = parser.parse_known_args()

# Create Spark session
spark = SparkSession.builder \
                    .appName("Healthcare Claims Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = args.bucket
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = f"{args.project_id}.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding hospital source for future reference
claims_df = (claims_df
                .withColumn("datasource", 
                              when(input_file_name().contains("hospital2"), "hosb")
                             .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))

# dropping duplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())
print(f"✅ Claims successfully loaded into BigQuery table: {BQ_TABLE}")