import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- THE WINDOWS FIX ---
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] += os.pathsep + 'C:\\hadoop\\bin'
# -----------------------

# 1. Initialize Spark Session (Java MongoDB Connector Removed)
spark = SparkSession.builder \
    .appName("HealthcareComplianceETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("⚡ Spark Engine Initialized! Connecting to Kafka...")

# 2. Define the schema
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("department", StringType(), True),
    StructField("admission_time", StringType(), True),
    StructField("wait_time_minutes", IntegerType(), True)
])

# 3. Read the Live Stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital-admissions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 4. DATA MASKING
clean_stream = parsed_stream \
    .withColumn("ssn_masked", expr("concat('***-**-', substring(ssn, 8, 4))")) \
    .withColumn("name", expr("'[REDACTED]'")) \
    .drop("ssn")

# 5. Output to MongoDB via Pure Python
def write_to_mongo_python(df, epoch_id):
    # Convert the Spark DataFrame row objects into standard Python dictionaries
    records = [row.asDict() for row in df.collect()]
    
    if records:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/")
        db = client["healthcare_db"]
        collection = db["anonymized_records"]
        
        # Insert the records directly
        collection.insert_many(records)
        print(f"✅ Successfully wrote {len(records)} anonymized records to MongoDB!")

print("🛡️ Processing stream, masking PII, and routing live data to MongoDB...")

query = clean_stream.writeStream \
    .foreachBatch(write_to_mongo_python) \
    .start()

query.awaitTermination()