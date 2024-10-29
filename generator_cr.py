from kafka import KafkaProducer
import time
import json
import random

# 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Función para generar datos simulados
def generate_data():
    return {
        'sensor_id': random.randint(1, 100),
        'temperature': round(random.uniform(20.0, 30.0), 2),
        'humidity': round(random.uniform(30.0, 50.0), 2),
        'timestamp': time.time()
    }

# Enviar datos al topic en intervalos regulares
topic_name = 'tarea3'
while True:
    data = generate_data()
    producer.send(topic_name, value=data)
    print(f'Enviado: {data}')
    time.sleep(1)
