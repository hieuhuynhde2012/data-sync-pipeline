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
from pyspark.sql.functions import col, lit

from src.spark.spark_write_data import SparkWriteDatabases


def main():
    db_config = get_database_config()

    jars = [
        r"C:\Users\PC\Desktop\data-sync-pipeline\lib\mysql-connector-j-9.2.0.jar",
        r"C:\Users\PC\Desktop\data-sync-pipeline\lib\mongo-spark-connector_2.12-3.0.1.jar"

    ]

    spark_conf = {
        "spark.jar.packages": "mysql:mysql-connector-java:9.2.0",
        "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
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
            StructField('id', LongType(), False),  # Đổi từ IntegerType() sang LongType() cho khớp với BIGINT MySQL
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

    df = spark_write_databases.read.schema(schema).json(
        r"C:\Users\PC\Desktop\data-sync-pipeline\data\2015-03-01-17.json")
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

    # Prepare DataFrame with columns matching Users table schema
    df_write_table_Users = df.withColumn('spark_temp', lit('sparkwrite')).select(
        col("actor.id").cast("int").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        col("spark_temp"),
    ).distinct().repartition(1)

    # Verify DataFrame schema
    # print("DataFrame schema for Users:")
    # df_write_table_Users.printSchema()
    # df_write_table_Users.show()
    # df_write_table_Respository.printSchema()
    # df_write_table_Respository.show()

    # df_write.spark_write_mysql(
    #     df_write_table_Respository,
    #     table_name="Repositories",
    #     mode="append",
    #     primary_key="repo_id",
    #     ignore_duplicates=True
    # )
    #
    df_write = SparkWriteDatabases(spark_write_databases, db_config)
    df_write.spark_write_mysql(
        df_write_table_Users,
        table_name="Users",
        mode="append",
        primary_key="user_id",
        ignore_duplicates=True
    )

    # write_count = df_write_table_Users.count()
    # print(f"Tổng số bản ghi đã ghi vào MySQL: {write_count}")
    df_validate = SparkWriteDatabases(spark_write_databases, db_config)
    df_validate_spark_mysql = df_validate.validate_spark_mysql(
        table_name="Users",
        df_write=df_write_table_Users,
        jdbc_url=db_config["jdbc"]["url"],
        config=db_config["jdbc"]["properties"]
    )

    # Ghi vào MongoDB
    df_write.spark_write_mongodb(
        df_write_table_Users,
        database_name="github_data",
        collection_name="Users",
        mode="append"
    )
    df_validate_spark_mongodb = df_validate.validate_spark_mongodb(
        df_write=df_write_table_Users,
        uri=db_config["mongodb"].uri,
        database_name=db_config["mongodb"].db_name,  # sửa đúng tên DB từ config
        collection_name="Users",
        mode = "append"


    )


if __name__ == "__main__":
    main()
