import json
import hashlib
import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

# -----------------------------------------------------------
# Enterprise Dataflow Streaming Consumer
# Features:
#   1. Dead Letter Queue (DLQ) for failed/invalid messages
#   2. Data Quality Validation transforms
#   3. 5-minute Tumbling Windows for cost-efficient BQ writes
#   4. PHI hashing for HIPAA compliance
#   5. Autoscaling configuration
# -----------------------------------------------------------


class ParseJsonFn(beam.DoFn):
    """Parses binary Pub/Sub payload to JSON dictionary.
    Failed messages are tagged and routed to the Dead Letter Queue."""

    def process(self, element):
        try:
            payload = json.loads(element.decode('utf-8'))
            yield beam.pvalue.TaggedOutput('parsed', payload)
        except Exception as e:
            logging.error(f"Failed to parse event: {str(e)}")
            # Route to DLQ instead of silently dropping
            yield beam.pvalue.TaggedOutput('dlq', {
                'raw_payload': element.decode('utf-8', errors='replace'),
                'error_type': 'PARSE_ERROR',
                'error_message': str(e),
                'pipeline_stage': 'ParseJsonFn'
            })


class ValidateEventFn(beam.DoFn):
    """Data Quality Validation transform.
    Checks for null required fields, invalid codes, and negative amounts.
    Invalid records are diverted to the Dead Letter Queue."""

    REQUIRED_FIELDS = ['event_timestamp', 'patient_id']

    def process(self, element):
        errors = []

        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if not element.get(field):
                errors.append(f"Missing required field: {field}")

        # Validate billed_amount (Claims events)
        billed = element.get('billed_amount')
        if billed is not None and billed < 0:
            errors.append(f"Negative billed_amount: {billed}")

        # Validate paid_amount (Claims events)
        paid = element.get('paid_amount')
        if paid is not None and paid < 0:
            errors.append(f"Negative paid_amount: {paid}")

        # Validate ICD code format (should start with letter)
        icd = element.get('icd_code') or element.get('primary_icd_code')
        if icd and not icd[0].isalpha():
            errors.append(f"Invalid ICD code format: {icd}")

        if errors:
            yield beam.pvalue.TaggedOutput('dlq', {
                'raw_payload': json.dumps(element),
                'error_type': 'VALIDATION_ERROR',
                'error_message': '; '.join(errors),
                'pipeline_stage': 'ValidateEventFn'
            })
        else:
            yield beam.pvalue.TaggedOutput('valid', element)


class HashPhiFn(beam.DoFn):
    """HIPAA Compliance: Hash any remaining PHI fields before writing to BigQuery.
    The producer already hashes most PHI, but this is a defense-in-depth layer."""

    PHI_FIELDS = ['patient_first_name', 'patient_last_name', 'patient_address']

    def _hash(self, value):
        return hashlib.sha256(value.encode()).hexdigest()[:16] if value else None

    def process(self, element):
        for field in self.PHI_FIELDS:
            if field in element and element[field]:
                element[field] = self._hash(element[field])
        yield element


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
    parser.add_argument("--subscription_id", default="carenet-rcm-sub-claims", help="Pub/Sub Subscription ID")
    parser.add_argument("--bucket", default="carenet-rcm-data-bucket", help="GCS Temp Bucket Name")
    parser.add_argument("--window_size_seconds", type=int, default=300, help="Tumbling window size in seconds (default: 5 min)")
    args, pipeline_args = parser.parse_known_args(argv)

    # -------------------------------------------------------
    # Autoscaling Configuration
    # THROUGHPUT_BASED autoscaling automatically adjusts the
    # number of Dataflow workers based on message backlog.
    # -------------------------------------------------------
    options = PipelineOptions(
        pipeline_args,
        streaming=True,
        project=args.project_id,
        region="us-central1",
        temp_location=f"gs://{args.bucket}/temp/",
        staging_location=f"gs://{args.bucket}/staging/",
        max_num_workers=10,
        autoscaling_algorithm='THROUGHPUT_BASED',
        save_main_session=True
    )

    subscription_path = f"projects/{args.project_id}/subscriptions/{args.subscription_id}"
    target_table = f"{args.project_id}:bronze_dataset.transactions_streaming"
    dlq_table = f"{args.project_id}:bronze_dataset.streaming_dead_letter_queue"

    # BigQuery schema for the main streaming table
    table_schema = {
        'fields': [
            {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'event_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'message_control_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'encounter_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'claim_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_first_name_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_last_name_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_gender', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'patient_address_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'provider_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'dept_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'encounter_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'admit_source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'discharge_disposition', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'drg_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'primary_icd_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'procedure_code', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'icd_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payor_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'billed_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'paid_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'claim_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'claim_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'revenue_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'place_of_service', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'priority', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    # BigQuery schema for the Dead Letter Queue table
    dlq_schema = {
        'fields': [
            {'name': 'raw_payload', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'error_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'error_message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'pipeline_stage', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    print(f"📡 Starting Enterprise Dataflow Pipeline")
    print(f"  Subscription: {subscription_path}")
    print(f"  Target Table: {target_table}")
    print(f"  DLQ Table:    {dlq_table}")
    print(f"  Window Size:  {args.window_size_seconds}s")
    print(f"  Autoscaling:  THROUGHPUT_BASED (max 10 workers)")

    with beam.Pipeline(options=options) as p:
        # Step 1: Read from Pub/Sub
        raw_messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)
        )

        # Step 2: Parse JSON (with DLQ for parse failures)
        parse_results = (
            raw_messages
            | "Parse JSON" >> beam.ParDo(ParseJsonFn()).with_outputs('parsed', 'dlq')
        )

        # Step 3: Validate data quality (with DLQ for validation failures)
        validation_results = (
            parse_results.parsed
            | "Validate Events" >> beam.ParDo(ValidateEventFn()).with_outputs('valid', 'dlq')
        )

        # Step 4: Hash any remaining PHI for defense-in-depth
        clean_events = (
            validation_results.valid
            | "Hash PHI Fields" >> beam.ParDo(HashPhiFn())
        )

        # Step 5: Apply 5-minute Tumbling Window for cost-efficient BigQuery writes
        # Instead of writing every single event immediately (expensive),
        # we batch events into 5-minute windows before flushing to BQ.
        windowed_events = (
            clean_events
            | "Apply 5min Window" >> beam.WindowInto(window.FixedWindows(args.window_size_seconds))
        )

        # Step 6: Write valid events to BigQuery
        (
            windowed_events
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=target_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # Step 7: Merge all DLQ streams and write to DLQ table
        dlq_combined = (
            (parse_results.dlq, validation_results.dlq)
            | "Flatten DLQ Streams" >> beam.Flatten()
        )

        (
            dlq_combined
            | "Window DLQ" >> beam.WindowInto(window.FixedWindows(args.window_size_seconds))
            | "Write DLQ to BigQuery" >> beam.io.WriteToBigQuery(
                table=dlq_table,
                schema=dlq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
