from dns.e164 import query
from pyspark.sql import DataFrame, SparkSession
import sys
import os

from pyspark.sql.functions import col

# Append the root directory to sys.path (1 level up from current file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema, create_mysql_schema, validate_mysql_schema, create_redis_schema, validate_redis_schema
from typing import Dict
from database.mysql_connect import MySQLConnect
from config.database_config import get_database_config
from database.mongodb_connect import MongoDBConnect

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

    def validate_spark_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config: Dict, mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subq") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()

        # print("Schema from MySQL:")
        # df_read.printSchema()
        # print("Schema from Spark write:")
        # df_write.printSchema()
        #
        # print("Sample data from MySQL:")
        # df_read.show(truncate=False)
        # print("Sample data from Spark write:")
        # df_write.show(truncate=False)

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame):
            df_read_aligned = df_read_database.select(df_spark_write.columns)
            result = df_spark_write.exceptAll(df_read_aligned)
            result.show()
            print(f"finding {result.count()} rows missing ")
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_config["url"]) \
                    .option("dbtable", table_name) \
                    .option("user", self.mysql_config.user) \
                    .option("password", self.mysql_config.password) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()

        if df_write.count() == df_read.count():
            print(f"Validation record successfully for {table_name} with {df_read.count()} rows.")
            subtract_dataframe(df_write, df_read)
            print("Validation record successfully completed.")
        else:
            subtract_dataframe(df_write, df_read)
            print(f"Insert missing records successfully by using spark")
        try:
            with MySQLConnect(**self.mysql_config.__dict__) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                connection.commit()
                mysql_client.close()
                print("Drop column spark_temp to MySQL.")
        except Exception as e:
            raise Exception(f"-----------------Fail to connect Mysql database: {e}")



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

    def validate_spark_mongodb(self, df_write: DataFrame, uri: str, database_name: str, collection_name: str,
                               mode: str = "append"):
        query = {"spark_temp": "sparkwrite"}

        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database_name) \
            .option("collection", collection_name) \
            .option("pipeline", str([{"$match": query}])) \
            .load() \
            .select(
            col("user_id"),
            col("login"),
            col("gravatar_id"),
            col("url"),
            col("avatar_url"),
            col("spark_temp"),
        )

        df_write = df_write.cache()
        df_read = df_read.cache()

        df_read_aligned = df_read.select(df_write.columns)
        result = df_write.exceptAll(df_read_aligned)
        result.show()
        missing_count = result.count()

        print(f"---df_write count : {df_write.count()} --- df_read_count : {df_read.count()}")
        df_read.printSchema()
        df_write.printSchema()

        if missing_count == 0:
            print(f"Validation record successfully for {collection_name} with {df_read.count()} rows.")
        else:
            print(f"finding {missing_count} rows missing ")
            result.write \
                .format("mongo") \
                .option("uri", uri) \
                .option("database", database_name) \
                .option("collection", collection_name) \
                .mode(mode) \
                .save()
            print("Validation record successfully completed.")
        self.spark.stop()

        #drop column spark_temp in mongodb use python
        config = get_database_config()
        with MongoDBConnect(config["mongodb"].uri,
                            config["mongodb"].db_name) as db:
            db["Users"].update_many({}, {"$unset": {"spark_temp": ""}})
        print(f"Completed validation after drop spark_temp")



