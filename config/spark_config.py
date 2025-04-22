from typing import Optional, List, Dict

from pyspark.sql import SparkSession
import os
import sys
# Chèn thư mục gốc vào sys.path bằng đường dẫn tuyệt đối
sys.path.append("C:/Users/PC/Desktop/Sysnc-Data/src")

from database_config import get_database_config
from pymongo import MongoClient
import redis


class SparkConnect:
    def __init__ (
        self,
        app_name: str,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = '4g',
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = '2g',
        num_executors: Optional[int] = 3,
        jars: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "ERROR"
    ) : 
        self.app_name = app_name
        self.spark = self.create_spark_session(
            app_name,
            master_url,
            executor_memory,
            executor_cores,
            driver_memory,
            num_executors,
            jars,
            spark_conf,
            log_level
        )
    def create_spark_session (
        # app_name: str,
        self,
        master_url : str = "local[*]",
        executor_memory: Optional[str] = '4g',
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = '2g',
        num_executors: Optional[int] = 3,
        jars: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "Worker1-Log"
    ) -> SparkSession:
        
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)
        
        if executor_memory:    
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)
        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars])
            builder.config("spark.jars", jars_path)
            
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)
                
        # Set the log level
        spark = builder.getOrCreate()
        
        spark.sparkContext.setLogLevel(log_level)
        
        return spark
    
    def stop(self):
        if self.spark:
            self.spark.stop()
            print(f"Spark session {self.app_name} stopped.")
            
    
            
        
    # spark = create_spark_session(app_name="Worker1",driver_memory='2g', executor_memory='4g', executor_cores=2, num_executors=3, jars= None, spark_conf={'spark.sql.shuffle.partitions': '10'}, master_url="local[*]", log_level="ERROR")

    # data = [["Alice", 1], ["Bob", 2], ["Cathy", 3]]

    # df = spark.createDataFrame(data, ["Name", "Id"])
    # df.show()

    def connect_to_mysql(spark: SparkSession, config : Dict[str, str], table_name):
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3307/github_data") \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .load()
        return df

    def connect_to_mongoddb(cfg):
        try:
            client = MongoClient(cfg["mongo_uri"])
            print("MongoDB connection successful!")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return None

    def connect_to_redis(cfg):
        try:
            r = redis.StrictRedis(
                host=cfg["redis_host"],
                port=cfg["redis_port"],
                password=cfg["redis_password"],
                decode_responses=True
            )
            pong = r.ping()
            print("Redis connection successful!")
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            return None
        
        
        
        
    jar_path = r"C:\Users\PC\Desktop\data-sync-pipeline\src\lib\mysql-connector-j-9.2.0.jar"

    spark = create_spark_session(
        app_name="LoadtoMySQL",
        driver_memory='2g',
        executor_memory='4g',
        executor_cores=2,
        num_executors=3,
        jars=[jar_path],
        spark_conf={'spark.sql.shuffle.partitions': '10'},
        master_url="local[*]",
        log_level="ERROR"
    )

db_config = get_database_config()

table_name = "Repositories"

# df = connect_to_mysql(spark, db_config, table_name)

# df.show()
