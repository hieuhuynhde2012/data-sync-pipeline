import os
from dotenv import load_dotenv

load_dotenv()

def get_redis_config():
    return {
        "redis_host": os.getenv("REDIS_HOST"),
        "redis_port": os.getenv("REDIS_PORT"),
        "redis_password": os.getenv("REDIS_PASSWORD"),
        "redis_url": os.getenv("REDIS_URL"),
    }