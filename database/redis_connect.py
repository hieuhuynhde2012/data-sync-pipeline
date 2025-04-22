import redis
from redis.exceptions import ConnectionError

class RedisConnect:
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.client = None

    def connect(self):
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                username=self.user,  
                password=self.password,
                db=self.db,
                decode_responses=True
            )
            self.client.ping()
            print("Connected to Redis")
            return self.client
        except ConnectionError as e:
            raise Exception(f"Redis connection error: {e}")

    def close(self):
        if self.client:
            self.client.close()
            print("Redis connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
