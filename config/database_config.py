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
    database: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_properties(self) -> Dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver",
        }
    
@dataclass
class RedisConfig(DatabaseConfig):
    host: str
    port: int
    password: str
    database: str
    user: str
    



def get_database_config() -> Dict[str, DatabaseConfig]:
        load_dotenv()

        mysql_config = MySQLConfig(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            port=int(os.getenv("MYSQL_PORT")),
            database=os.getenv("MYSQL_DATABASE"),
        )

        config = {
    "mongodb": MongoDBConfig(
        uri=os.getenv("MONGO_URI"),
        db_name=os.getenv("MONGO_DB_NAME"),
    ),
    "mysql": mysql_config,
    "redis": RedisConfig(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        user=os.getenv("REDIS_USER"),
        password=os.getenv("REDIS_PASSWORD"),
        database=os.getenv("REDIS_DB"),
    ),
    "jdbc": {
        "url": mysql_config.jdbc_url,
        "properties": mysql_config.connection_properties,
    }
}

        
        for db, setting in config.items():
            if isinstance(setting, DatabaseConfig):
                print(f"Config for {db}: {setting}")
                setting.validate()
        
        return config
    
    
a = get_database_config()
print(a)    