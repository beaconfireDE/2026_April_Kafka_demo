from confluent_kafka import Consumer
from confluent_kafka import Producer

import json
import psycopg2

class cdcConsumer():
    def __init__(self,address,kafkaGroup,KafkaAutoReset,dbname,dbuser,dbpassword,dbhost,dbport,topic):
        kafka_conf = {
        'bootstrap.servers':address,
        'group.id': kafkaGroup,
        'auto.offset.reset':KafkaAutoReset
        }
        dlqConf = { 'bootstrap.servers': address}
        dbConfig = {
            'dbname':dbname,
            'user': dbuser,
            'password': dbpassword,
            'host':dbhost,
            'port': dbport
            }
        
        self.conn = psycopg2.connect(**dbConfig)
        
        self.cursor = self.conn.cursor()
        self.consumer =Consumer(kafka_conf)
        self.dlqProducer = Producer(dlqConf)
        self.consumer.subscribe([topic])
    
    def pollMessage(self):
        return self.consumer.poll(timeout = 1.0)

    def kafkaConsume(self,msg):
        print("Raw Kafka message:", msg.value().decode('utf-8'))
        print("start consuming data from Kafka")
        json_str = msg.value().decode('utf-8')
        data = json.loads(json_str)
        return data
    
    #consumer processes each message and applies insert, update, or delete based on the action field
    def loadDB(self,data):
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
            self.cursor.execute(query, (emp_id, first_name, last_name, dob, city, salary))
        elif action == 'UPDATE':
            query = '''
                    Update employees set first_name = %s, last_name = %s, dob =%s, city = %s, salary = %s
                    where emp_id = %s;
                '''
            self.cursor.execute(query, (first_name, last_name, dob, city, salary,emp_id))
        elif action == 'DELETE':
            query = '''
                    delete from employees where emp_id = %s;
                '''
            self.cursor.execute(query,(emp_id,))
        else:
            raise ValueError(f"Unknow action type:{action}")
        self.conn.commit()

    def sendTodlq(self,msg):
        print("Process error, sending to DLQ")
        self.dlqProducer.produce(
            topic = 'bf_employee_cdc_dlq',
            value = msg.value()
        )
        self.dlqProducer.flush()

    def close(self):
        self.consumer.close()
        self.cursor.close()
        self.conn.close()
        self.dlqProducer.flush()

if __name__ =='__main__':
    cdcconsumer = cdcConsumer('localhost:29092','employee_cdc_group','latest','postgres','postgres','postgres','localhost',5433,'employee_cdc_topic')
    try:
        while True:
            msg = cdcconsumer.pollMessage()
            if msg is None: 
                print("No message received, waiting...")
                continue
            if msg.error():
                cdcconsumer.sendTodlq(msg)
                continue
            try:
                data = cdcconsumer.kafkaConsume(msg)
                cdcconsumer.loadDB(data)
            except Exception as e:
                print(f"Error processing message:{e}")
                cdcconsumer.sendTodlq(msg)  
    except KeyboardInterrupt:
            print("Stopping consumer....")
    finally:
        cdcconsumer.close()
            
    


