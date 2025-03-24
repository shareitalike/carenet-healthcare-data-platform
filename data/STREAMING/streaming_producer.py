import time
import json
import random
import argparse
from datetime import datetime
from google.cloud import pubsub_v1

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
parser.add_argument("--topic_id", default="carenet-rcm-transactions-topic", help="GCP Pub/Sub Topic ID")
args = parser.parse_known_args()[0]

PROJECT_ID = args.project_id
TOPIC_ID = args.topic_id

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Mock Lookup Lists for generation
first_names = ["John", "Jane", "Robert", "Emily", "Michael", "Sarah", "William", "Jessica"]
last_names = ["Doe", "Smith", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson"]
addresses = ["123 Elm St, Chicago, IL", "456 Oak St, Chicago, IL", "789 Pine St, Chicago, IL", "101 Maple St, Chicago, IL"]
genders = ["Male", "Female"]
encounter_types = ["Inpatient", "Outpatient", "Emergency", "Ambulatory"]
cpt_codes = [99213, 99214, 99283, 99284, 99203]
icd_codes = ["A09", "I10", "E11", "J45", "M54"]
payor_ids = ["PAY801", "PAY802", "PAY803"]
payor_types = ["Medicare", "Medicaid", "Commercial"]
claim_statuses = ["Approved", "Denied", "Pending"]

def generate_mock_event():
    """Generates a random EMR transaction and claim event payload"""
    patient_id = f"P{random.randint(100, 999)}"
    encounter_id = f"E{random.randint(1000, 9999)}"
    transaction_id = f"T{random.randint(50000, 99999)}"
    claim_id = f"CLM{random.randint(90000, 99999)}"
    
    amount = round(random.uniform(50.0, 5000.0), 2)
    claim_status = random.choice(claim_statuses)
    paid_amount = round(amount * random.choice([0.8, 0.0, 0.5]), 2) if claim_status != "Denied" else 0.0
    
    # Combined transaction/encounter payload for streaming simplicity
    event = {
        "event_timestamp": datetime.now().isoformat(),
        "transaction_id": transaction_id,
        "encounter_id": encounter_id,
        "patient_id": patient_id,
        "patient_first_name": random.choice(first_names),
        "patient_last_name": random.choice(last_names),
        "patient_gender": random.choice(genders),
        "patient_address": random.choice(addresses),
        "provider_id": str(random.choice([101, 102, 103])),
        "dept_id": str(random.choice([1, 2, 3])),
        "encounter_type": random.choice(encounter_types),
        "procedure_code": random.choice(cpt_codes),
        "icd_code": random.choice(icd_codes),
        "payor_id": random.choice(payor_ids),
        "payor_type": random.choice(payor_types),
        "billed_amount": amount,
        "claim_id": claim_id,
        "claim_status": claim_status,
        "claim_paid_amount": paid_amount
    }
    return event

print(f"🚀 Starting Real-time EMR Event Generator...")
print(f"Targeting Pub/Sub Topic: {topic_path}")
print("Press Ctrl+C to stop.")

try:
    while True:
        event = generate_mock_event()
        data = json.dumps(event).encode("utf-8")
        
        # Publish event
        future = publisher.publish(topic_path, data)
        message_id = future.result()
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent transaction {event['transaction_id']} for Patient {event['patient_first_name']} {event['patient_last_name']} - Status: {event['claim_status']} (ID: {message_id})")
        
        # Sleep for 2 seconds before publishing next event
        time.sleep(2.0)

except KeyboardInterrupt:
    print("\n🛑 Generator stopped.")
except Exception as e:
    print(f"\n🚨 Error in publisher: {str(e)}")
