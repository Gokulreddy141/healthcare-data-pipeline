# Real-Time Healthcare Compliance ETL Pipeline

An enterprise-grade, distributed data pipeline designed to ingest, process, and anonymize high-velocity healthcare data in real-time. 

## 🏗️ Architecture & Tech Stack
* **Infrastructure Orchestration:** Docker & Docker Compose
* **Ingestion Layer:** Apache Kafka (Python Producer)
* **Cold Storage / Audit Trail:** Simulated HDFS (Partitioned JSON dumps)
* **Processing & ETL Engine:** Apache Spark (PySpark Structured Streaming)
* **Serving Database:** MongoDB (NoSQL)
* **API Backend:** FastAPI

## 🚀 System Data Flow
1. **Streaming:** A simulated hospital network continuously publishes patient admission records to a Kafka topic.
2. **Auditing:** A consumer constantly dumps the raw, immutable Kafka stream into partitioned local storage for compliance auditing.
3. **Real-Time Masking:** Apache Spark intercepts the live stream, applies a strict schema, and masks Personally Identifiable Information (PII) such as Names and Social Security Numbers on the fly.
4. **Serving:** The anonymized records are batch-loaded into MongoDB.
5. **Retrieval:** A FastAPI endpoint allows frontend applications to query the safely redacted patient data instantly.

## 🛠️ How to Run Locally
1. Start the Big Data cluster: `docker-compose up -d`
2. Start the Kafka Producer: `python data_generator/producer.py`
3. Start the HDFS Dump: `python hdfs_dump/consumer.py`
4. Start the Spark ETL Engine: `python spark_processor/processor.py`
5. Boot the API: `uvicorn api.main:app --reload`
