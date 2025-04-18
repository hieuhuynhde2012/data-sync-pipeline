from dotenv import load_dotenv
import os
from typing import Dict
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"Missing environment variable for {key}")
        
@dataclass
class MongoDBConfig(DatabaseConfig):
    uri: str
    db_name: str

@dataclass    
class MySQLConfig(DatabaseConfig):
    host: str
    user: str
    password: str
    port: int
    # database: str
    
@dataclass
class RedisConfig(DatabaseConfig):
    host: str
    port: int
    password: str
    database: str
    user: str
    



def get_database_config() -> Dict[str, DatabaseConfig]:
        load_dotenv()
        
        config = {
    "mongodb": MongoDBConfig(
        uri=os.getenv("MONGO_URI"),
        db_name=os.getenv("MONGO_DB_NAME"),
    ),
    "mysql": MySQLConfig(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        port=int(os.getenv("MYSQL_PORT")),
        # database=os.getenv("MYSQL_DATABASE"),
    ),
    "redis": RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        password=os.getenv("REDIS_PASSWORD"),
        database=os.getenv("REDIS_DB"),
        user=os.getenv("REDIS_USER"),
    )
}

        
        for db, setting in config.items():
            print(f"Config for {db}: {setting}")
            setting.validate()
        
        return config
    
    
a = get_database_config()
print(a)    