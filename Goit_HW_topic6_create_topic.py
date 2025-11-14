from kafka.admin import KafkaAdminClient, NewTopic
from Goit_HW_topic6_configs import kafka_config, building_sensors, temperature_alerts, humidity_alerts

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    #security_protocol=kafka_config['security_protocol'],
    #sasl_mechanism=kafka_config['sasl_mechanism'],
    #sasl_plain_username=kafka_config['username'],
    #sasl_plain_password=kafka_config['password'],
    #api_version=(2, 0, 2)
)

# Визначення нового топіку
num_partitions = 3
replication_factor = 1

new_topic1 = NewTopic(name=building_sensors, num_partitions=num_partitions, replication_factor=replication_factor)
new_topic2 = NewTopic(name=temperature_alerts, num_partitions=num_partitions, replication_factor=replication_factor)
new_topic3 = NewTopic(name=humidity_alerts, num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[new_topic1, new_topic2, new_topic3], validate_only=False)
    print(f"Topics '{building_sensors}', '{temperature_alerts}' and '{humidity_alerts}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
    
# Перевіряємо список існуючих топіків
#list = admin_client.list_topics()
#list.sort()
#print("\n".join(map(str, list)))

print("\n--- My topics: ---")
my_name = "oie"
[print(topic) for topic in admin_client.list_topics() if my_name in topic]
print("--------------------")

# Закриття зв'язку з клієнтом
admin_client.close()