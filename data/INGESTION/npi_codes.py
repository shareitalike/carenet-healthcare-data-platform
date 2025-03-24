from datetime import date
import requests
from pyspark.sql import SparkSession
import argparse

# Parse Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Bucket Name")
args, unknown = parser.parse_known_args()

# Use date.today() to get the current date in a format that Spark can handle
current_date = date.today()

# Initialize Spark session
spark = SparkSession.builder.appName("NPIData").getOrCreate()

# Base URL for the NPI Registry API
base_url = "https://npiregistry.cms.hhs.gov/api/"

# Defining the parameters for the initial API request to get a list of NPIs
params = {
    "version": "2.1",  # API version
    "state": "CA",  # Example state, replace with desired state or other criteria
    "city": "Los Angeles",  # Example city, replace with desired city
    "limit": 20,  # Limit the number of results for demonstration purposes
}

# Make the initial API request to get a list of NPIs
response = requests.get(base_url, params=params)

# Check if the request was successful
if response.status_code == 200:
    npi_data = response.json()
    npi_list = [result["number"] for result in npi_data.get("results", [])]

    # Initialize a list to store detailed NPI information
    detailed_results = []

    # Loop through each NPI to get their details
    for npi in npi_list:
        detail_params = {"version": "2.1", "number": npi}
        detail_response = requests.get(base_url, params=detail_params)

        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            if "results" in detail_data and detail_data["results"]:
                for result in detail_data["results"]:
                    npi_number = result.get("number")
                    basic_info = result.get("basic", {})
                    if result["enumeration_type"] == "NPI-1":
                        fname = basic_info.get("first_name", "")
                        lname = basic_info.get("last_name", "")
                    else:
                        fname = basic_info.get("authorized_official_first_name", "")
                        lname = basic_info.get("authorized_official_last_name", "")
                    position = (
                        basic_info.get("authorized_official_title_or_position", "")
                        if "authorized_official_title_or_position" in basic_info
                        else ""
                    )
                    organisation = basic_info.get("organization_name", "")
                    last_updated = basic_info.get("last_updated", "")
                    detailed_results.append(
                        {
                            "npi_id": npi_number,
                            "first_name": fname,
                            "last_name": lname,
                            "position": position,
                            "organisation_name": organisation,
                            "last_updated": last_updated,
                            "refreshed_at": current_date,
                        }
                    )

    # Create a DataFrame
    if detailed_results:
        print(detailed_results)
        df = spark.createDataFrame(detailed_results)
        output_path = f"gs://{args.bucket}/landing/npi_extract/"
        df.write.format("parquet").mode("overwrite").save(output_path)
        print(f"✅ NPI data successfully written to GCS at: {output_path}")

    else:
        print("No detailed results found.")
else:
    print(f"Failed to fetch data: {response.status_code} - {response.text}")