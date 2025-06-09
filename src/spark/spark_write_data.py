from pyspark.sql import DataFrame, SparkSession
import sys
import os

# Append the root directory to sys.path (1 level up from current file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema, create_mysql_schema, validate_mysql_schema, create_redis_schema, validate_redis_schema
from typing import Dict
from database.mysql_connect import MySQLConnect
class SparkWriteDatabases:
    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_config = db_config
        self.mysql_config = db_config["mysql"]
        self.jdbc_config = db_config["jdbc"]
        self.mongodb_config = db_config["mongodb"]

    def spark_write_mysql(self, df: DataFrame, table_name: str, mode: str = "append", primary_key: str = None,
                          ignore_duplicates: bool = False):
        # try:
        #     mysql_client = MySQLConnect(**self.mysql_config.__dict__)
        #     mysql_client.connect()
        #     mysql_client.close()
        # except Exception as e:
        #     raise Exception(f"Error connecting to MySQL: {e}")
        try:
            with MySQLConnect(**self.mysql_config.__dict__) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                mysql_client.close()
                print("Added column spark_temp to MySQL.")
        except Exception as e:
            raise Exception(f"-----------------Fail to connect Mysql database: {e}")

        if ignore_duplicates and primary_key:
            # Load existing keys từ MySQL
            existing_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", self.mysql_config.user) \
                .option("password", self.mysql_config.password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load() \
                .select(primary_key)

            # Lọc các dòng chưa tồn tại
            df = df.join(existing_df, on=primary_key, how="left_anti")

        if df.rdd.isEmpty():
            print(f"No new data to write to {table_name}.")
            return

        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", self.mysql_config.user) \
            .option("password", self.mysql_config.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()

        print(f"Data written to MySQL table {table_name} successfully.")

    def validate_spark_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config: Dict):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver","com.mysql.cj.jdbc.Driver") \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subq") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        # df_read.show()
        # print(f"Data validated successfully with {df_read.count()} rows.")
        if df_write.count() == df_read.count():
            print(f"Vaidation record successfully for {table_name} with {df_read.count()} rows.")

    def spark_write_mongodb(self, df: DataFrame, database_name: str, collection_name: str, mode: str = "append"):
        try:
            df.write \
                .format("mongo") \
                .mode(mode) \
                .option("uri", self.mongodb_config.uri) \
                .option("database", database_name) \
                .option("collection", collection_name) \
                .save()
            print(f"Data written to MongoDB collection {collection_name} successfully.")
        except Exception as e:
            raise Exception(f"Error connecting to MongoDB: {e}")


