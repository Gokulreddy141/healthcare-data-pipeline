import json
import time
import random
from faker import Faker
from kafka import KafkaProducer
from datetime import datetime

# Initialize Faker for synthetic PII generation
fake = Faker()

# Kafka configuration (Matches your docker-compose setup)
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'hospital-admissions'

# Setup Kafka Producer
# value_serializer ensures our Python dictionaries are converted to JSON bytes before sending
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Hospital metadata pools
DEPARTMENTS = ['Cardiology', 'Neurology', 'Oncology', 'Emergency', 'Pediatrics', 'Orthopedics']
DIAGNOSIS_CODES = ['I21.9', 'G43.909', 'C34.90', 'J01.90', 'S82.899A', 'R07.9']

def generate_patient_record():
    """Generates a single synthetic hospital admission record."""
    return {
        "patient_id": f"P-{random.randint(10000, 99999)}",
        "name": fake.name(),
        "ssn": fake.ssn(),
        "diagnosis_code": random.choice(DIAGNOSIS_CODES),
        "department": random.choice(DEPARTMENTS),
        "admission_time": datetime.utcnow().isoformat() + "Z",
        "wait_time_minutes": random.randint(0, 180) # Simulating wait times up to 3 hours
    }

if __name__ == "__main__":
    print(f"🏥 Starting Hospital Data Generator...")
    print(f"📡 Streaming to Kafka topic: '{TOPIC_NAME}'\n")
    
    try:
        while True:
            # 1. Generate the fake data
            record = generate_patient_record()
            
            # 2. Send data to Kafka
            producer.send(TOPIC_NAME, record)
            
            # 3. Print to console so we can see it working
            print(f"Sent: [ID: {record['patient_id']}] -> {record['department']} | Wait: {record['wait_time_minutes']} mins")
            
            # 4. Wait 2 seconds before the next admission
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Streaming stopped by user.")
    finally:
        producer.close()