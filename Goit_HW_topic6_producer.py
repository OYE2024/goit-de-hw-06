from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from Goit_HW_topic6_configs import kafka_config, building_sensors
import json
import uuid
import time
import random

# Створення Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        #security_protocol=kafka_config['security_protocol'],
        #sasl_mechanism=kafka_config['sasl_mechanism'],
        #sasl_plain_username=kafka_config['username'],
        #sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print("Kafka Producer created successfully.")
except NoBrokersAvailable:
    print("No Kafka brokers are available. Please check the connection settings.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit()

sensor_building_id = random.randint(1, 10)

for i in range(100):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "sensor_building_id": sensor_building_id,  # Випадковий ідентифікатор будівлі
            "temperature": round(random.uniform(25.0, 45.0), 2),  # Випадкова температура
            "humidity": round(random.uniform(15.0, 85.0), 2)  # Випадкова вологість
        }
        key_bytes = str(sensor_building_id).encode('utf-8')
        producer.send(building_sensors, key=key_bytes, value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{building_sensors}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer