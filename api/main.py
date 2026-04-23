from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI(title="Healthcare Compliance Data API")

# Connect to the local MongoDB container
client = MongoClient("mongodb://localhost:27017/")
db = client["healthcare_db"]
collection = db["anonymized_records"]

@app.get("/")
def read_root():
    return {"message": "🏥 Healthcare ETL API is online. Data is secured."}

@app.get("/api/v1/patients/recent")
def get_recent_patients(limit: int = 5):
    """
    Fetches the most recent anonymized patient records from MongoDB.
    """
    try:
        # We exclude the internal MongoDB '_id' because it is not JSON serializable by default
        records = list(collection.find({}, {"_id": 0}).limit(limit))
        
        return {
            "status": "success",
            "records_returned": len(records),
            "data": records
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}