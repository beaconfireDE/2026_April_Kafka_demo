# load data from a file


from confluent_kafka import Consumer
import json
import psycopg2

conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'group1',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['employee_data'])

# PostgreSQL connection
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port= 5432 
)
cursor = conn.cursor()
# Create tables first if they do not already exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS department_employee (
        id SERIAL PRIMARY KEY,
        department VARCHAR(50),
        department_division VARCHAR(255),
        position_title VARCHAR(255),
        hire_date VARCHAR(50),
        salary INTEGER
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS department_employee_salary (
        department VARCHAR(50) PRIMARY KEY,
        total_salary INTEGER
    );
""")

conn.commit()

print("Tables are ready")
print("Start consuming data from Kafka")



print("start consuming data from Kafka")

while True:
    msg = consumer.poll(timeout = 1.0)
    if msg is None:
        continue
    if msg.error():
        print("consumer error:{msg.error()}")
        continue
    try:
        data = json.loads(msg.value().decode('utf-8'))
        print(data)
        # Insert data into PostgreSQL
        cursor.execute("""
            INSERT INTO department_employee (department, department_division, position_title, hire_date, salary)
            VALUES (%s, %s, %s, %s, %s);
        """, (data['Department'], data['Department Division'], data['Position Title'], data['Initial Hire Date'], data['Salary']))

        # Update total salary for each department
        cursor.execute("""
            INSERT INTO department_employee_salary (department, total_salary)
            VALUES (%s, %s)
            ON CONFLICT (department) DO UPDATE SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
        """, (data['Department'], data['Salary']))
        conn.commit()
        print("successful")
    except Exception as e:
        print(f"Error: {e}")
        break

consumer.close()
cursor.close()
conn.close()
