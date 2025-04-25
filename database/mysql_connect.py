import mysql.connector
from mysql.connector import Error

class MySQLConnect:
    def __init__(self, host, port, user, password,database=None):
        self.config = {
            'host': host,
            'user': user,
            'port': port,
            'password': password,
        }
        if database:
            self.config['database'] = database
        self.connection = None
        self.cursor = None
        
    def connect(self):
        try: 
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"Connected to MySQL")
            return self.connection, self.cursor
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None, None
        
    def close(self):
        if self.cursor:
            self.cursor.close()
            
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")
            
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()