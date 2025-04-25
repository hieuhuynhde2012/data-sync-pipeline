import sys
import os

# Append the root directory to sys.path (1 level up from current file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema, create_mysql_schema, validate_mysql_schema, create_redis_schema, validate_redis_schema
from config.database_config import get_database_config
from database.mongodb_connect import MongoDBConnect
from database.mysql_connect import MySQLConnect
from database.redis_connect import RedisConnect

def main(config):
   
    # with MongoDBConnect(config["mongodb"].uri, config["mongodb"].db_name) as mongo_client:
    #     if mongo_client is None:
    #         print("Failed to connect to MongoDB. Exiting.")
    #         return
    #     # Create MongoDB schema
    #     create_mongodb_schema(mongo_client)
    #     mongo_client.Users.insert_one({
    #         "user_id": 1,
    #         "login": "test_user",
    #         "gravatar_id": "test_gravatar",
    #         "avatar_url": "http://example.com/avatar.jpg",
    #         "url": "http://example.com/user"
    #     })
    #     print("Sample data inserted into MongoDB.")
    #     validate_mongodb_schema(mongo_client)
    #
    
    # #MySQL
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute("INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (%s, %s, %s, %s, %s)", (1, "test_user", "test_gravatar", "http://example.com/avatar.jpg", "http://example.com/user"))
        connection.commit()
        print("Sample data inserted into MySQL.")
        validate_mysql_schema(cursor)
    
    with RedisConnect(config["redis"].host, config["redis"].port, config["redis"].user, config["redis"].password, config["redis"].database) as redis_client:
        # print(config["redis"])
        # redis_client.connect()
        create_redis_schema(redis_client.connect())
        validate_redis_schema(redis_client.connect())

    
if __name__ == "__main__":
    config  = get_database_config()
    main(config)