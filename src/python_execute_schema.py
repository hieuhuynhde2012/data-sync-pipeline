import mysql.connector
import sys

from mysql.connector import Error

# Chèn thư mục gốc vào sys.path bằng đường dẫn tuyệt đối
sys.path.append("C:/Users/PC/Desktop/Sysnc-Data")

from config.databaseconfig import get_database_config

from pathlib import Path

SQL_FILE_PATH = Path(r"C:\Users\PC\Desktop\Sysnc-Data\sql\schema.sql")
DATABASE_NAME = "github_data"
def connect_to_mysql(config):
    #connect to mysql
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")

def create_database(cursor, database_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"Database {database_name} created")
    
def execute_sql_file(cursor, sql_file_path):
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()
        # print(sql_script)
        
    commands = [command.strip() for command in sql_script.split(";") if command.strip()]
    
    for cm in commands:
        try:
            cursor.execute(cm)
            print(f"Executed: {cm.strip()[:50]}...")
        except Error as e:
            print(f"Error executing command: {cm.strip()[:50]}... Error: {e}")
            
def main():
    connection = None
    cursor = None
    try: 
        #get config from file .env            
        db_config = get_database_config()
        
        # remove key: datavase from config
        config_removedb = {k: v for k,v in db_config.items() if k != "database"}
        
        connection = connect_to_mysql(config_removedb)
        cursor = connection.cursor()
        
        
        #create database github_data
        create_database(cursor, DATABASE_NAME)
        
        #change to created database
        connection.database = DATABASE_NAME
        
        #execute file sql
        execute_sql_file(cursor, SQL_FILE_PATH)
        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        if connection and connection.is_connected():
            connection.rollback()
    
    
if __name__ == "__main__":
    main()