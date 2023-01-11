from os import environ

# Hadoop Env Var
HDFS_URL = environ.get('HDFS_URL', 'http://localhost:9870')
HDFS_MASTER = environ.get('HDFS_MASTER', 'http://localhost:9000')
DATALAKE_PATH = 'user/datalake'

# Spark Env Var
APP_NAME = 'dvdrental_app'
JAR_PACKAGES = []
SPARK_FILES = [
    'configs/etl_config.json'
]
SPARK_CONFIGS = {
    'spark.sql.warehouse.dir': f'{HDFS_MASTER}/user/hive/warehouse'
}
JDBC_URL = environ.get('JDBC_URL', 'jdbc:postgresql://localhost:5432/dvdretal')
JDBC_USER = environ.get('JDBC_USER', 'user')
JDBC_PASSWORD = environ.get('JDBC_PASSWORD', 'pass')
