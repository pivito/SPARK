import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_fake_sale():
    return {
        "Date": fake.date_between(start_date='-10y', end_date='today').isoformat(),
        "Store ID": random.randint(1, 100),
        "Product ID": random.choice(["P-1001", "P-1002", "BADID", "LU-7126"]),
        "Quantity Sold": random.randint(1, 20),
        "Revenue": round(random.uniform(5.0, 500.0), 2),
        "Tratados": True,
        "Fecha Inserción": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Generando datos infinitamente para Kafka. Pulsa Ctrl+C para detener.")
    try:
        while True:
            sale = generate_fake_sale()
            producer.send("sales_stream", sale)
            print(f"Enviado: {sale}")
            time.sleep(1)  # puedes ajustar el tiempo si quieres más o menos frecuencia
    except KeyboardInterrupt:
        print("\n Generación detenida por el usuario.")
