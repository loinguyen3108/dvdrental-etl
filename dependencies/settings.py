# Hadoop Env Var
HDFS_URL = 'http://localhost:9870'
HDFS_MASTER = 'hdfs://localhost:9000'
DATALAKE_PATH = 'user/datalake'

# Spark Env Var
APP_NAME = 'dvdrental_app'
JAR_PACKAGES = []
SPARK_FILES = [
    'configs/etl_config.json'
]
SPARK_CONFIGS = {
    'spark.jars': '/home/loinguyen/Downloads/postgresql-42.5.1.jar',
    'spark.sql.warehouse.dir': f'{HDFS_MASTER}/user/hive/warehouse'
}
JDBC_URL = 'jdbc:postgresql://localhost:5432/dvdretal'
JDBC_USER = 'loinguyen'
JDBC_PASSWORD = 'tanloi3108'

# DVDRental Env Var
TABLES = ['film_actor', 'address', 'city', 'customer', 'actor', 'film_category', 'inventory',
          'category', 'country', 'language', 'rental', 'staff', 'store', 'payment', 'film']
