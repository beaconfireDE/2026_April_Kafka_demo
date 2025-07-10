import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)

cursor = conn.cursor()
cursor.execute("""
               CREATE TABLE IF NOT EXISTS test_table (
               id INT, name VARCHAR(100)
               );
               """)

cursor.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test1');")

cursor.execute("SELECT * FROM test_table;")
row = cursor.fetchone() # fetchone is to retrieve out all the values store in row
print("Fetched row:", row)
#cursor.execute("drop table test_table;")  
conn.commit() 
cursor.close()
conn.close()