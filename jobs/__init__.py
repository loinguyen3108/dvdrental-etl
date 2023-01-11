from dependencies.settings import APP_NAME, JAR_PACKAGES, SPARK_CONFIGS, SPARK_FILES
from dependencies.spark import start_spark


class BaseETL:
    def __init__(self, enable_hive: bool = False) -> None:
        self.start_session(enable_hive=enable_hive)

    def start_session(self, enable_hive: bool = False):
        self.spark, self.logger, self.etl_config = start_spark(
            app_name=APP_NAME, jar_packages=JAR_PACKAGES,
            files=SPARK_FILES, spark_config=SPARK_CONFIGS,
            enable_hive=enable_hive)

    def is_exists_table(self, tb_nane: str):
        return True if tb_nane in self.spark.catalog.listTables() else False

    def stop(self):
        return self.spark.stop()
