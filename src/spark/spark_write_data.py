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
        
    def spark_write_mysql(self, df: DataFrame, table_name: str, mode: str = "append"):
        try:
            mysql_client = MySQLConnect(**self.mysql_config.__dict__)
            mysql_client.connect()
            mysql_client.close()
        except Exception as e:
            raise Exception(f"Error connecting to MySQL: {e}")

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

    def spark_write_mysql(self, df: DataFrame, table_name: str, mode: str = "append", primary_key: str = None,
                          ignore_duplicates: bool = False):
        try:
            mysql_client = MySQLConnect(**self.mysql_config.__dict__)
            mysql_client.connect()
            mysql_client.close()
        except Exception as e:
            raise Exception(f"Error connecting to MySQL: {e}")

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
    #
    # def spark_write_mysql(self, df: DataFrame, table_name: str, mode: str = "append", primary_key: str = None,
    #                       ignore_duplicates: bool = False):
    #     try:
    #         mysql_client = MySQLConnect(**self.mysql_config.__dict__)
    #         mysql_client.connect()
    #         mysql_client.close()
    #     except Exception as e:
    #         raise Exception(f"Error connecting to MySQL: {e}")
    #
    #     if ignore_duplicates and primary_key:
    #         # Thực hiện ghi dữ liệu trực tiếp với chế độ "append"
    #         # MySQL sẽ xử lý ON DUPLICATE KEY UPDATE
    #         df.write \
    #             .format("jdbc") \
    #             .option("url", self.jdbc_config["url"]) \
    #             .option("dbtable", table_name) \
    #             .option("user", self.mysql_config.user) \
    #             .option("password", self.mysql_config.password) \
    #             .option("driver", "com.mysql.cj.jdbc.Driver") \
    #             .mode("append") \
    #             .save()
    #
    #         print(f"Data written to MySQL table {table_name} successfully.")
    #     else:
    #         # Nếu không cần xử lý trùng lặp, ghi dữ liệu vào MySQL
    #         df.write \
    #             .format("jdbc") \
    #             .option("url", self.jdbc_config["url"]) \
    #             .option("dbtable", table_name) \
    #             .option("user", self.mysql_config.user) \
    #             .option("password", self.mysql_config.password) \
    #             .option("driver", "com.mysql.cj.jdbc.Driver") \
    #             .mode(mode) \
    #             .save()
    #
    #         print(f"Data written to MySQL table {table_name} successfully.")

