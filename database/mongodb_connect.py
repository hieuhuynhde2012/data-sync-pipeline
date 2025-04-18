from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import sys
import os

# Append the root directory to sys.path (1 level up from current file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from config.database_config import get_database_config
#step 1: def(get mongo config)
#Step 2: def(connect)
#step 3: def(disconnect)
#step 4: def(reconnect)
#step 5: def(exit)

class MongoDBConnect:
    #step 1
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None
        
    def connect(self):
        try: 
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info()
            self.db = self.client[self.db_name]
            print(f"Connected to MongoDB database: {self.db_name}")
            return self.db
        except ConnectionFailure as e:
            print(f"Could not connect to MongoDB: {e}")
            return None
        
    def __enter__(self):
        return self.connect()
    
    def close(self):
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
        

        
        