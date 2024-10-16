from kafka import KafkaProducer
import json
import random
import time

# Iniciar una sesión de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generateData(count):
    return {
        "ID": count,
        "Price": round(random.uniform(0, 300), 2),
        "OpenPrice": round(random.uniform(0, 300), 2),
        "HighPrice": round(random.uniform(0, 300), 2),
        "LowPrice": round(random.uniform(0, 300), 2),
        "ClosePrice": round(random.uniform(0, 300), 2),
        "Volume": round(random.uniform(100, 10**7), 2)
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    count = 0
    while True:
        count += 1
        data = generateData(count=count)
        producer.send("topic-task3", data)
        producer.flush()
        print(data)
        time.sleep(5)

if __name__ == "__main__":
    main()