
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
    user = db.Users.find_one({"user_id": 2})
    if user is None:
        print("Users collection is empty.")
        return False   
    