### File: producer.py
# This file is used to do extract, transform data and then send it to Kafka.
import csv
import json
import math
from datetime import datetime
from confluent_kafka import Producer


conf = {
    'bootstrap.servers': 'localhost:29092'
}

producer = Producer(conf)


def read_csv(file_path):
    with open(file_path, newline ='') as csvfile:
       reader  = csv.DictReader(csvfile)
       for row in reader:
          if row['Department'] == 'ECC' or row['Department'] == 'CIT' or row['Department'] == 'EMS':
            try:
                hire_date = datetime.strptime(row['Initial Hire Date'], '%d-%b-%Y').year
                # print("hire_date:", hire_date)
                if hire_date >=2010:
                    row['Salary'] = math.floor(float(row['Salary']))
                    producer.produce(
                    topic = 'employee_data', 
                    value = json.dumps(row).encode('utf-8')
                    )
            except ValueError as e:
                print(f"Error parsing date for row {row}: {e}")

            
read_csv('Employee_Salaries.csv')

producer.flush()
print("sent all data to Consumer")





              
       