import os
from dotenv import load_dotenv

load_dotenv()
def get_mongodb_config():
    return {
        "mongo_user": os.getenv("MONGO_USER"),
        "mongo_password": os.getenv("MONGO_PASSWORD"),
        "mongo_uri": os.getenv("MONGO_URI"),
        "mongo_db": os.getenv("MONGO_DB"),
    }