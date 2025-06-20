from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoDBConnect:
    def __init__(self, mongo_uri: str, db_name: str):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def _connect(self):
        self.client = MongoClient(self.mongo_uri)
        self.client.server_info()
        self.db = self.client[self.db_name]

    def __enter__(self):
        try:
            self._connect()
            print(f"Connected to MongoDB: {self.db_name}")
            return self.db
        except ConnectionFailure as e:
            print(f"Could not connect to MongoDB: {e}")
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")

    def reconnect(self):
        if self.client:
            self.client.close()
        self._connect()
