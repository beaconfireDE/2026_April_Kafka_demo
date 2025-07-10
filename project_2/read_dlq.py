from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'dlq-inspector',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['bf_employee_cdc_dlq'])

print("📥 Reading from DLQ:")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    print("🔴 From DLQ:", json.loads(msg.value().decode('utf-8')))
