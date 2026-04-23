import json
import os
from kafka import KafkaConsumer
from datetime import datetime

# Setup our simulated HDFS / Data Lake directory
DATA_LAKE_DIR = 'raw_data_lake'
os.makedirs(DATA_LAKE_DIR, exist_ok=True)

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'hospital-admissions'

# Setup Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest', # If we restart, start from the oldest unread message
    enable_auto_commit=True,
    group_id='cold-storage-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"🗄️ Starting Cold Storage Consumer...")
print(f"🎧 Listening to '{TOPIC_NAME}' and saving to ./{DATA_LAKE_DIR}/\n")

try:
    for message in consumer:
        record = message.value
        
        # 1. Create a daily partition folder (Standard Big Data Practice)
        today = datetime.utcnow().strftime('%Y-%m-%d')
        partition_dir = os.path.join(DATA_LAKE_DIR, f"date={today}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # 2. Create a unique filename for the raw JSON dump
        timestamp = int(datetime.utcnow().timestamp() * 1000)
        file_name = f"{record['patient_id']}_{timestamp}.json"
        file_path = os.path.join(partition_dir, file_name)
        
        # 3. Write the raw, immutable data to our "Lake"
        with open(file_path, 'w') as f:
            json.dump(record, f, indent=4)
            
        print(f"💾 Saved raw record to: {file_path}")

except KeyboardInterrupt:
    print("\n🛑 Cold storage dump stopped by user.")
finally:
    consumer.close()