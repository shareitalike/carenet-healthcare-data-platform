import json
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ParseJsonFn(beam.DoFn):
    """Parses binary Pub/Sub payload to JSON dictionary"""
    def process(self, element):
        try:
            # Decode the Pub/Sub bytes message to string and parse JSON
            payload = json.loads(element.decode('utf-8'))
            yield payload
        except Exception as e:
            logging.error(f"Failed to parse event: {str(e)}")
            # In production, you would redirect failed rows to a Dead Letter Queue (GCS bucket)
            pass

def run(argv=None):
    # Parse Command Line Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
    parser.add_argument("--subscription_id", default="carenet-rcm-transactions-sub", help="Pub/Sub Subscription ID")
    parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Temp Bucket Name")
    args, pipeline_args = parser.parse_known_args(argv)

    # Configure Dataflow Streaming Pipeline Options
    options = PipelineOptions(
        pipeline_args,
        streaming=True, # Enables streaming mode in Dataflow
        project=args.project_id,
        region="us-central1",
        temp_location=f"gs://{args.bucket}/temp/",
        staging_location=f"gs://{args.bucket}/staging/"
    )

    subscription_path = f"projects/{args.project_id}/subscriptions/{args.subscription_id}"
    target_table = f"{args.project_id}:bronze_dataset.transactions_streaming"

    # Define BigQuery Table Schema for Auto-creation
    table_schema = {
        'fields': [
            {'name': 'event_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'encounter_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_gender', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'provider_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'dept_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'encounter_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'procedure_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'icd_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payor_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billed_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'claim_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'claim_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'claim_paid_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    }

    print(f"📡 Starting Dataflow Pipeline reading from: {subscription_path}")
    print(f"Targeting BigQuery Table: {target_table}")

    # Build the Apache Beam Pipeline
    with beam.Pipeline(options=options) as p:
        (p
         | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)
         | "Parse Byte String" >> beam.ParDo(ParseJsonFn())
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
             table=target_table,
             schema=table_schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
