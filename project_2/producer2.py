# scan emp_cdc 
#save offset
# sent to kafka topic "employee_cdc_topic"

import psycopg2
from confluent_kafka import Producer
import json
import time

class cdcProducer():
    def __init__(self,address,dbname,dbuser,dbpassword,dbhost,dbport):
        kafkaConfig = {'bootstrap.servers': address}
        dbConfig = { 'dbname':dbname,
        'user': dbuser,
        'password': dbpassword,
        'host':dbhost,
        'port': dbport}
        conn = psycopg2.connect(**dbConfig)
        self.producer = Producer(kafkaConfig)
        self.cursor = conn.cursor()


    def saveOffset(self,offset):
        with open('offset.txt','w') as f:
            f.write(str(offset))


    def loadOffset(self):
        try:
            with open('offset.txt', 'r') as f:
                return int(f.read().strip())
        except:
            return 0
        

    def readDbToKafka(self):
        offset = self.loadOffset()
        try:
            while True:
                self.cursor.execute(""" select * from emp_cdc where cdc_id > %s order by cdc_id """,(offset,))
                rows = self.cursor.fetchall()

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
                    self.producer.produce(
                        topic='employee_cdc_topic',
                        value=value
                    )
                    self.producer.flush()
                    print(f"ROW: {row[0]}")
                    offset = row[0]
                    self.saveOffset(offset)
                    

        except Exception as e:
            print(f"Error message: {e}")

        finally:
            self.cursor.close()
            self.conn.close()
            self.producer.flush()



if __name__ =='__main__':
    cdcproducer = cdcProducer('localhost:29092','postgres','postgres','postgres','localhost',5432)
    cdcproducer.readDbToKafka()





