import time
import json
import random
import hashlib
import argparse
from datetime import datetime
from google.cloud import pubsub_v1

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--project_id", default="carenet-rcm-data-platform", help="GCP Project ID")
parser.add_argument("--events_per_second", type=float, default=0.5, help="Events generated per second")
args = parser.parse_known_args()[0]

PROJECT_ID = args.project_id

# -------------------------------------------------------
# Topic-per-Event-Type Architecture (Enterprise Standard)
# Instead of a single monolith topic, each clinical domain
# has its own topic for independent scaling and monitoring.
# -------------------------------------------------------
TOPICS = {
    "adt":    f"projects/{PROJECT_ID}/topics/carenet-rcm-topic-adt",       # Admit/Discharge/Transfer
    "claims": f"projects/{PROJECT_ID}/topics/carenet-rcm-topic-claims",    # Claims & Remittance
    "orders": f"projects/{PROJECT_ID}/topics/carenet-rcm-topic-orders",    # Lab/Pharmacy Orders
}

# -------------------------------------------------------
# Enable Message Ordering on the Publisher Client
# This guarantees that messages with the same ordering_key
# (e.g., same patient_id) are delivered in FIFO order.
# -------------------------------------------------------
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)

# Mock Lookup Lists for realistic US healthcare data generation
first_names = ["John", "Jane", "Robert", "Emily", "Michael", "Sarah", "William", "Jessica", "David", "Maria"]
last_names = ["Doe", "Smith", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson", "Martinez", "Anderson"]
addresses = [
    "123 Elm St, Chicago, IL 60601",
    "456 Oak Ave, Houston, TX 77001",
    "789 Pine Blvd, Phoenix, AZ 85001",
    "101 Maple Dr, Philadelphia, PA 19101",
    "202 Cedar Ln, San Antonio, TX 78201"
]
genders = ["Male", "Female"]
encounter_types = ["Inpatient", "Outpatient", "Emergency", "Ambulatory", "Observation"]
cpt_codes = [99213, 99214, 99283, 99284, 99203, 99232, 99223, 99291]
icd_codes = ["A09", "I10", "E11.9", "J45.909", "M54.5", "K21.0", "N39.0", "R10.9"]
payor_ids = ["PAY801", "PAY802", "PAY803", "PAY804"]
payor_types = ["Medicare", "Medicaid", "Commercial", "Self-Pay"]
claim_statuses = ["Approved", "Denied", "Pending", "Partially Approved"]
drg_codes = ["470", "871", "392", "690", "291", "065"]
admit_sources = ["Physician Referral", "Emergency Room", "Transfer", "Walk-In"]
discharge_dispositions = ["Home", "SNF", "AMA", "Expired", "Rehab Facility"]
order_types = ["Laboratory", "Radiology", "Pharmacy", "Pathology"]
order_statuses = ["Ordered", "In Progress", "Completed", "Cancelled"]


def hash_phi(value):
    """HIPAA-compliant: Hash PHI fields before they leave the producer.
    In a real environment, this would use a KMS-managed key for deterministic encryption."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def generate_adt_event():
    """Generates an ADT (Admit/Discharge/Transfer) event — the most common HL7v2 message type."""
    patient_id = f"P{random.randint(100, 999)}"
    encounter_id = f"E{random.randint(1000, 9999)}"
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)

    return {
        "event_type": "ADT",
        "event_timestamp": datetime.now().isoformat(),
        "message_control_id": f"MCN-{random.randint(100000, 999999)}",
        "encounter_id": encounter_id,
        "patient_id": patient_id,
        "patient_first_name_hash": hash_phi(first_name),
        "patient_last_name_hash": hash_phi(last_name),
        "patient_gender": random.choice(genders),
        "patient_address_hash": hash_phi(random.choice(addresses)),
        "provider_id": str(random.choice([101, 102, 103, 104, 105])),
        "dept_id": str(random.choice([1, 2, 3, 4, 5])),
        "encounter_type": random.choice(encounter_types),
        "admit_source": random.choice(admit_sources),
        "discharge_disposition": random.choice(discharge_dispositions),
        "drg_code": random.choice(drg_codes),
        "primary_icd_code": random.choice(icd_codes),
        "procedure_code": random.choice(cpt_codes),
    }, patient_id  # ordering_key = patient_id for FIFO ordering


def generate_claims_event():
    """Generates a Claims/Remittance event (835/837 transaction equivalent)."""
    patient_id = f"P{random.randint(100, 999)}"
    transaction_id = f"T{random.randint(50000, 99999)}"
    claim_id = f"CLM{random.randint(90000, 99999)}"
    amount = round(random.uniform(50.0, 5000.0), 2)
    claim_status = random.choice(claim_statuses)
    paid_amount = round(amount * random.choice([0.8, 0.0, 0.5, 0.95]), 2) if claim_status != "Denied" else 0.0

    return {
        "event_type": "CLAIM",
        "event_timestamp": datetime.now().isoformat(),
        "transaction_id": transaction_id,
        "claim_id": claim_id,
        "patient_id": patient_id,
        "provider_id": str(random.choice([101, 102, 103, 104, 105])),
        "encounter_id": f"E{random.randint(1000, 9999)}",
        "procedure_code": random.choice(cpt_codes),
        "icd_code": random.choice(icd_codes),
        "payor_id": random.choice(payor_ids),
        "payor_type": random.choice(payor_types),
        "billed_amount": amount,
        "paid_amount": paid_amount,
        "claim_status": claim_status,
        "claim_type": random.choice(["Professional", "Institutional"]),
        "revenue_code": random.choice(["0120", "0250", "0450", "0301"]),
        "place_of_service": random.choice(["11", "21", "22", "23"]),
    }, claim_id  # ordering_key = claim_id


def generate_orders_event():
    """Generates a Lab/Pharmacy Order event (ORM/ORU message equivalent)."""
    patient_id = f"P{random.randint(100, 999)}"
    order_id = f"ORD{random.randint(10000, 99999)}"

    return {
        "event_type": "ORDER",
        "event_timestamp": datetime.now().isoformat(),
        "order_id": order_id,
        "patient_id": patient_id,
        "provider_id": str(random.choice([101, 102, 103, 104, 105])),
        "order_type": random.choice(order_types),
        "order_status": random.choice(order_statuses),
        "procedure_code": random.choice(cpt_codes),
        "icd_code": random.choice(icd_codes),
        "priority": random.choice(["Routine", "Stat", "Urgent"]),
    }, order_id  # ordering_key = order_id


# Map event generators to their topics
EVENT_GENERATORS = {
    "adt": generate_adt_event,
    "claims": generate_claims_event,
    "orders": generate_orders_event,
}

print(f"🚀 Starting Enterprise EMR Event Generator...")
print(f"Publishing to {len(TOPICS)} domain-specific Pub/Sub topics:")
for domain, topic in TOPICS.items():
    print(f"  → {domain.upper()}: {topic}")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        # Randomly select an event type to simulate real-world traffic mix
        event_domain = random.choices(
            population=["adt", "claims", "orders"],
            weights=[0.4, 0.4, 0.2],  # 40% ADT, 40% Claims, 20% Orders
            k=1
        )[0]

        event, ordering_key = EVENT_GENERATORS[event_domain]()
        data = json.dumps(event).encode("utf-8")
        topic_path = TOPICS[event_domain]

        # Publish with ordering_key for FIFO guarantees per entity
        future = publisher.publish(
            topic_path,
            data,
            ordering_key=ordering_key
        )
        message_id = future.result()

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"[{event_domain.upper():6s}] "
            f"Sent {event.get('transaction_id') or event.get('encounter_id') or event.get('order_id')} "
            f"for Patient {event['patient_id']} "
            f"(ordering_key={ordering_key}, msg_id={message_id})"
        )

        time.sleep(1.0 / args.events_per_second)

except KeyboardInterrupt:
    print("\n🛑 Generator stopped.")
except Exception as e:
    print(f"\n🚨 Error in publisher: {str(e)}")
