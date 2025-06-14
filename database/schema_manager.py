from pathlib import Path
def create_mongodb_schema(db):
    db.drop_collection("Users")
    db.create_collection("Users", validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties": {
                "user_id": {
                    "bsonType": "int",
                },
                "login": {
                    "bsonType": "string",
                },
                "gravatar_id": {
                    "bsonType": "string",
                },
                "avatar_url": {
                    "bsonType": "string",
                },
                "url": {
                    "bsonType": "string",
                },
            }
        }
    })
    db.Users.create_index("user_id", unique=True)
    
def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    print(collections)
    
    if "Users" not in collections:
        print("Users collection does not exist.")
        return False 
    user = db.Users.find_one({"user_id": 1})
    if user is None:
        print("Users collection is empty.")
        return False   
 
SQL_FILE_PATH = Path(r"C:\Users\PC\Desktop\data-sync-pipeline\sql\schema.sql")    
def create_mysql_schema(connection, cursor):

    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    connection.commit()
    connection.database = database
    try:
        with open(SQL_FILE_PATH, "r") as sql_file:
            sql_script = sql_file.read()
            sql_comands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_comands:
                cursor.execute(cmd)
                print(f"Executed command: {cmd}")
            connection.commit()
            print("MySQL schema created successfully.")
            
    except Exception as e:
        connection.rollback()
        print(f"Error executing SQL script: {e}")            
        # Execute the SQL script
        
        
def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    if "Users" not in tables or "Repositories" not in tables:
        print("Table does not exist.")
        return False
    cursor.execute("SELECT * FROM Users WHERE user_id = 1")
    
    # print(cursor.fetchone())
    
    user = cursor.fetchone()
    if not user:
        print("Users table is empty.")
        return False
    
    print("Validated shema in MySQL.")
    

#Redis
def create_redis_schema(client):
    try: 
        client.flushdb()
        client.set("user:1:login", "test_user")
        client.set("user:1:gravatar_id", "test_gravatar")
        client.set("user:1:avatar_url", "http://example.com/avatar.jpg")
        client.set("user:1:url", "http://example.com/user")
        client.sadd("user_id", "user:1")
        print("Redis schema created successfully.")
    except Exception as e:
        raise Exception(f"Error creating Redis schema: {e}")
    
    
def validate_redis_schema(client):
    if not client.get("user:1:login") == "test_user":
        raise ValueError("Value login not found in Redis")  
    
    if not client.sismember("user_id", "user:1"):
        raise ValueError("user_id not found in Redis")
    
    print("Validated schema in Redis.")
    
     