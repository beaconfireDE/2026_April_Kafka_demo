# scan emp_cdc 
#save offset
# sent to kafka topic "employee_cdc_topic"

import psycopg2
from confluent_kafka import Producer
import json
import time

kafka_config = {'bootstrap.servers': 'localhost:29092', 'acks' : 'all'}
db_config = { 'dbname':'postgres',
        'user':'postgres',
        'password':'postgres',
        'host':'localhost',
        'port': 5432}

producer = Producer(kafka_config)
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()
print("🚀 CDC Producer started ")

def save_offset(offset):
    with open('offset.txt', 'w') as f:
        f.write(str(offset))

def load_offset():
    try:
        with open('offset.txt', 'r') as f:
            return int(f.read().strip())
    except:
        return 0

offset = load_offset()

try:
    while True:
        cursor.execute(""" select * from emp_cdc where cdc_id > %s order by cdc_id """,(offset,))
        rows = cursor.fetchall()

        if not rows: 
            print("No new message, Waiting...")
            time.sleep(5)
            continue

        print("send message to Kafka: ")
        for row in rows:
            data = {
                "cdc_id": row[0],
                "emp_id": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "dob": str(row[4]),
                "city": row[5],
                "salary": row[6],
                "action": row[7]
            }
            print(f"fetched: {data}")
            value = json.dumps(data, default=str).encode('utf-8')
            producer.produce(
                topic='employee_cdc_topic',
                value=value
            )
            producer.flush()
            print(f"ROW: {row[0]}")
            offset = row[0]
            save_offset(offset)
            

except Exception as e:
    print(f"Error message: {e}")

finally:
    cursor.close()
    conn.close()
    producer.flush()




# class cdcProducer():
#     def __init__(self,db_config,kafka_config, topic):
#         pass

#     def connect_db(self):
#         pass


#     def scan(self):
#         pass
           
# if __name__ == '__main__':
    
    
    
