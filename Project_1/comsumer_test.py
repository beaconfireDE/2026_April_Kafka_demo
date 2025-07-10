from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'employee_data',                          # 要监听的 topic
    bootstrap_servers='localhost:29092',       # Kafka 服务器地址
    auto_offset_reset='earliest',             # 从最早的消息开始读
    enable_auto_commit=True,                  # 自动提交 offset
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # 解码消息
)

print("开始监听 Kafka topic:employee_data")
for message in consumer:
    print("收到消息：", message.value)
