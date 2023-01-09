from dependencies.settings import APP_NAME, JAR_PACKAGES, SPARK_CONFIGS, SPARK_FILES
from dependencies.spark import start_spark


class BaseETL:
    def __init__(self) -> None:
        self.start_session()

    def start_session(self):
        self.spark, self.logger, self.etl_config = start_spark(
            app_name=APP_NAME, jar_packages=JAR_PACKAGES,
            files=SPARK_FILES, spark_config=SPARK_CONFIGS)

    def extract(self):
        raise NotImplementedError

    def transform(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def stop(self):
        return self.spark.stop()
