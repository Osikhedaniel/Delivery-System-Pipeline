from pydantic import BaseModel 
from datetime import datetime 

class GPSData(BaseModel):
    vehicle_id:str
    delivery_id:str
    driver_name:str
    latitude:float
    longitude:float
    speed:float 
    status:str
    timestamp:datetime 