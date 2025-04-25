# import sys
# import os
#
# # Append the root directory to sys.path (1 level up from current file)
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# import sys
# print(sys.path)
from config.spark_config import SparkConnect
from config.database_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import col

from src.spark.spark_write_data import SparkWriteDatabases


def main():
    db_config = get_database_config()

    jars = [
        r"C:\Users\PC\Desktop\data-sync-pipeline\lib\mysql-connector-j-9.2.0.jar"

    ]

    spark_conf = {
        "spark.jar.packages": "mysql:mysql-connector-java:9.2.0",
    }

    spark_write_databases = SparkConnect(
        app_name="MyApp",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="1g",
        num_executors=1,
        jars=jars,
        spark_conf=spark_conf,
        log_level="ERROR"
    )
    
    schema = StructType([
        StructField('actor', StructType([
            StructField('id', IntegerType(), False),
            StructField('login', StringType(), True),
            StructField('gravatar_id', StringType(), True),
            StructField('url', StringType(), True),
            StructField('avatar_url', StringType(), True),
        ]), True),
        StructField('repo', StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), True),
            StructField('url', StringType(), True),
        ]), True),
    ])
    
    df = spark_write_databases.read.schema(schema).json(r"C:\Users\PC\Desktop\data-sync-pipeline\data\2015-03-01-17.json")
    # df.show()
    # df_write_table_Users = df.select(
    #     col("actor.id").alias("user_id"),
    #     col("actor.login").alias("login"),
    #     col("actor.gravatar_id").alias("gravatar_id"),
    #     col("actor.avatar_url").alias("avatar_url"),
    #     col("actor.url").alias("url"),
    # )

    df_write_table_Respository = df.select(
        col("repo.id").alias("repo_id"),
        col("repo.name").alias("name"),
        col("repo.url").alias("url"),
    ).distinct().repartition(1)

    df_write = SparkWriteDatabases(spark_write_databases, db_config)
    # Prepare DataFrame with columns matching Users table schema
    df_write_table_Users = df.select(
        df.actor.id.alias("user_id"),  # Rename to match table
        df.actor.login.alias("login"),
        df.actor.gravatar_id.alias("gravatar_id"),
        df.actor.url.alias("url"),
        df.actor.avatar_url.alias("avatar_url")
    ).distinct().repartition(1)  # Ensure unique rows and single partition

    # Verify DataFrame schema
    print("DataFrame schema for Users:")
    # df_write_table_Users.printSchema()
    # df_write_table_Users.show()
    df_write_table_Respository.printSchema()
    df_write_table_Respository.show()

    df_write.spark_write_mysql(
        df_write_table_Respository,
        table_name="Repositories",
        mode="append",
        primary_key="repo_id",
        ignore_duplicates=True
    )




if __name__ == "__main__":
    main()