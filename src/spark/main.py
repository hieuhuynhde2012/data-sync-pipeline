from spark_write_data import SparkWriteDatabases
from config.database_config import get_database_config
from config.spark_config import SparkConnect
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
    