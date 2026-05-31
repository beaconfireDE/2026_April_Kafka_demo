### File: producer.py
# This file is used to do extract, transform data and then send it to Kafka.
import csv
import json
import math
from datetime import datetime
from confluent_kafka import Producer
import psycopg2

class MyProducer():
    def __init__(self, address, dbname, user, password, host, port):
        conf = {
            'bootstrap.servers': address
        }
        self.producer = Producer(conf)
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = conn.cursor()

    def read_csv(self, file_path):
        with open(file_path, newline ='') as csvfile:
            reader  = csv.DictReader(csvfile)
            for row in reader:
                if row['Department'] == 'ECC' or row['Department'] == 'CIT' or row['Department'] == 'EMS':
                    try:
                        hire_date = datetime.strptime(row['Initial Hire Date'], '%d-%b-%Y').year
                        # print("hire_date:", hire_date)
                        if hire_date >=2010:
                            row['Salary'] = math.floor(float(row['Salary']))
                            self.producer.produce(
                            topic = 'employee_data', 
                            value = json.dumps(row).encode('utf-8')
                            )
                    except ValueError as e:
                        print(f"Error parsing date for row {row}: {e}")
        self.producer.flush()
        print("All message sent")



if __name__ =="__main__":
    proj1Producter = MyProducer('localhost:29092','postgres','postgres','postgres','localhost', 5432)
    proj1Producter.read_csv('Employee_Salaries.csv')




              
       