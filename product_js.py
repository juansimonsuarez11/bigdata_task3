import time
import json
import random
from kafka import KafkaProducer

def create_data():
    return {
        "device_id": random.randint(1, 10),
        "wind_speed": round(random.uniform(0, 100), 2),  # Velocidad del viento en km/h
        "pressure": round(random.uniform(950, 1050), 2),  # Presión atmosférica en hPa
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    data_record = create_data()
    producer.send('sensor_data', value=data_record)
    print(f"Data Sent: {data_record}")
    time.sleep(10)  # Espera de 10 segundos entre envíos
