from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from Goit_HW_topic6_configs import kafka_config, temperature_alerts, humidity_alerts
import json

# Створення Kafka Consumer
try:
    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        #security_protocol=kafka_config['security_protocol'],
        #sasl_mechanism=kafka_config['sasl_mechanism'],
        #sasl_plain_username=kafka_config['username'],
        #asl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8'),
        auto_offset_reset='earliest',  # Зчитування повідомлень з початку
        enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
        group_id='monitoring_group'   # Ідентифікатор групи споживачів
    )
    print("Kafka Consumer created successfully.")
except NoBrokersAvailable:
    print(f"Error: Cannot connect to Kafka brokers at {kafka_config['bootstrap_servers']}.")
    print("Please check connection settings or broker availability.")
    exit()
except Exception as e:  
    print(f"An unexpected error occurred: {e}")
    exit()  

# Підписка на тему
consumer.subscribe([temperature_alerts, humidity_alerts])
print(f"Subscribed to topics '{temperature_alerts}' and '{humidity_alerts}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        
        data = message.value

        # Загальні дані
        window = data.get('window', {})
        window_start = window.get('start', 'N/A')
        window_end = window.get('end', 'N/A')
        sensor_id = data.get('sensor_id', 'N/A')
        avg_temp = data.get('avg_temperature', 'N/A')
        avg_hum = data.get('avg_humidity', 'N/A')
        
        alert_data = data.get('alert', {})
        alert_code = alert_data.get('code', 'N/A')
        alert_message = alert_data.get('message', 'N/A')

        # Форматування в залежності від топіка
        if message.topic == temperature_alerts:
            print("\nALERT: High temperature")
        elif message.topic == humidity_alerts:
            print("\nALERT: Humidity out of range")
        
        # друк інформації
        print(f"Topic: {message.topic}")
        print(f"Window: [{window_start}, {window_end}]")
        print(f"Sensor ID: {sensor_id}")
        print(f"Average Temperature: {avg_temp}°C")
        print(f"Average Humidity: {avg_hum}%")
        print(f"Alert Code: {alert_code}")
        print(f"Message: {alert_message}")
        print("-" * 40)

except KeyboardInterrupt:
    print("\nStopping consumer...")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    print("Closing consumer...")
    consumer.close()  # Закриття consumer