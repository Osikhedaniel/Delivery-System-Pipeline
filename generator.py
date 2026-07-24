from datetime import datetime, UTC
from faker import Faker 
import random 
import uuid
import pandas as pd


fake = Faker()

STATUS = ['Delivered','In Transit','Cancelled','Arriving','Delayed']

def generate_synthetic_data():
    return{
        'vehicle_id':f"VEH-{uuid.uuid4().hex[:6]}",
        'delivery_id':f"DEL-{uuid.uuid4().hex[:4]}",
        'driver_name':fake.name(),
        'latitude':round(random.uniform(6.4500,6.7000),6),
        'longitude':round(random.uniform(3.2000,3.5000),6),
        'speed':round(random.uniform(20,100),2),
        'status':random.choices(STATUS,weights=[65,15,5,10,5])[0],
        'timestamp':datetime.now(UTC).isoformat()
    }

if __name__ == "__main__":
    # data = [generate_synthetic_data() for _ in range(1000)]

    # df = pd.DataFrame(data)

    # df.to_csv(r"C:\Users\HP\Desktop\Delivery System Pipeline\generator_test_data.csv")
    generate_synthetic_data() 