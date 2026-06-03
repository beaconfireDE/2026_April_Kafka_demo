from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'employee_data',                          #  topic
    bootstrap_servers='localhost:29092',       # Kafka service address
    auto_offset_reset='earliest',             # start from earliest message
    enable_auto_commit=True,                  # auto commit offset
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # deserialize data
)

print("Start connect Kafka topic:employee_data")
for message in consumer:
    print("Recieved message:", message.value)
