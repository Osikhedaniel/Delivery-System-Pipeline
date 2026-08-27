import os
from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader
from generator import generate_synthetic_data
from dotenv import load_dotenv 

load_dotenv()

app = FastAPI(title="Delivery System API", version="1.0.0")

api_key_header = APIKeyHeader(name="X-API-Key")

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != os.getenv("API_KEY"):
        raise HTTPException(
            status_code=401,
            detail="Invalid API Key"
        )

@app.get("/")
def health_check():
    return {"status":"running"}

@app.get("/gps")
def generator_function(api_key: str = Security(verify_api_key)):
    return generate_synthetic_data()

@app.get("/gps/bulk")
def get_bulk_gps_data(records:int=Query(150,gt=0,le=1000),api_key: str = Security(verify_api_key)):
   return [generate_synthetic_data() for _ in range(records)] 
