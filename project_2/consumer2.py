from confluent_kafka import Consumer
from confluent_kafka import Producer

import json
import psycopg2

kafka_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'employee_cdc_group',
    'auto.offset.reset': 'earliest'
}


db_conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port= 5433 
)

cursor = db_conn.cursor()
consumer = Consumer(kafka_conf)
dlq_producer = Producer({ 'bootstrap.servers': 'localhost:29092'})
consumer.subscribe(['employee_cdc_topic'])

print("start consuming data from Kafka")
try:
    while True:
        msg = consumer.poll(timeout = 1.0)
        if msg is None: continue
        if msg.error():
            print(f"Consumer error {msg.error()}")
            continue
        json_str = msg.value().decode('utf-8')
        try:
            data = json.loads(json_str)
            print(data)
            emp_id = data.get('emp_id')
            first_name = data.get('first_name')
            last_name = data.get('last_name')
            dob = data.get('dob')
            city = data.get('city')
            salary = data.get('salary')
            action = data.get('action')
            if action == 'INSERT':
                query = '''
                        INSERT INTO employees (
                            emp_id, first_name, last_name, dob, city, salary
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    '''
                cursor.execute(query, (emp_id, first_name, last_name, dob, city, salary))
            elif action == 'UPDATE':
                query = '''
                        Update employees set first_name = %s, last_name = %s, dob =%s, city = %s, salary = %s
                        where emp_id = %s;
                    '''
                cursor.execute(query, (first_name, last_name, dob, city, salary,emp_id))
            elif action == 'DELETE':
                query = '''
                        delete from employees where emp_id = %s;
                    '''
                cursor.execute(query,(emp_id,))
            else:
                raise ValueError(f"Unknow action type:{action}")

            db_conn.commit()
            # print(f"Applied {action} for emp_id = {emp_id}")

        except Exception as e:
            print(f"Process error, sending to DLQ: {e}")
            dlq_producer.produce(
                topic = 'bf_employee_cdc_dlq',
                value = msg.value()
            )
            dlq_producer.flush()
        
except Exception as e:
    print(f"Consumer Error{e}")

finally:
    consumer.close()
    cursor.close()
    db_conn.close()
    dlq_producer.flush()

