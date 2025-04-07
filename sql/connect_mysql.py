import mysql.connector

db_config = {
    "host": "localhost",  # Chạy trong container
    "port": 3307,         # Cổng trong container
    "user": "root",
    "password": "=4n5&Fq,++A6Ai&0utyH6MR6,Z0&opA4",
}

database_name = "github_data"
sql_file_path = "schema.sql"

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
print(f"Database {database_name} created")
connection.database = database_name

with open(sql_file_path, 'r') as file:
    sql_script = file.read()
    
sql_commands = sql_script.split(';')

for command in sql_commands:
    if command.strip():
        cursor.execute(command)
        print(f"Executed: {command.strip()[:50]}...")
        
connection.commit()
print("================Schema.sql file executed sucessfully================")