from dotenv import load_dotenv
from urllib.parse import urlparse
import os
def get_database_config():
    #load file .env
    load_dotenv()
    
    jdbc_url = os.getenv("DB_URL")
    
    if not jdbc_url:
        raise ValueError("DB_URL environment variable is not set.")
    
    parsed_url = urlparse(jdbc_url.replace("jdbc:", "", 1))
    
    host = parsed_url.hostname
    port = parsed_url.port
    database = parsed_url.path.strip("/") if parsed_url.path else None
    
    # print(f"Host: {host}")
    # print(f"Port: {port}")
    # print(f"Database: {database}")
    
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }
    
    
    
    