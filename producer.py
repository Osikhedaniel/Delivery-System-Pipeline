from kafka import KafkaProducer
import requests  
import json 
import time 
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_TOPIC = "gps_updates"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

# API_URL = "http://127.0.0.1:8000/docs#/"
API_URL = "https://delivery-system-pipeline-2.onrender.com/gps/bulk"
API_KEY = os.getenv("API_KEY")

def extract_data():
    while True:
        try:
            response = requests.get(API_URL,headers={"X-API-Key": API_KEY})

            if response.status_code == 200:
                data = response.json()
                # print(type(data))

                # producer.send(KAFKA_TOPIC,value=data)
                for record in data:
                       producer.send(KAFKA_TOPIC, value=record)
                producer.flush()

                print(f"Sent:{data}")

                time.sleep(5)
        
        except Exception as e:
            print(f"Error:{e}")

if __name__ == "__main__":
    extract_data() 