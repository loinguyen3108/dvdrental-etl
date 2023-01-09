from argparse import ArgumentParser
from datetime import datetime
from functools import cached_property

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, max, lit

from dependencies.settings import DATALAKE_PATH, JDBC_URL, JDBC_USER, JDBC_PASSWORD, HDFS_MASTER, HDFS_URL
from jobs import BaseETL
from services.hdfs.hdfs import HDFSSerivce


class Ingestion(BaseETL):
    def __init__(self) -> None:
        super().__init__()

    @cached_property
    def hdfs_service(self):
        return HDFSSerivce()

    def extract(self, table_name: str, p_key: str = None):
        if self.hdfs_service.is_exists(f'/{DATALAKE_PATH}/{table_name}'):
            df_exists = self.spark.read \
                .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}')
            max_id = df_exists.select(
                max(col(p_key)).alias('max_id')).collect()[0]['max_id']
            tb_query = f'(select * from public.{table_name} where {p_key} > {max_id}) as tmp'
        else:
            tb_query = f'public.{table_name}'
        self.logger.info('Extracting to postgres...')
        return self.spark.read.format('jdbc') \
            .option('url', JDBC_URL) \
            .option('user', JDBC_USER) \
            .option('password', JDBC_PASSWORD) \
            .option('dbtable', tb_query) \
            .option('driver', 'org.postgresql.Driver') \
            .option('dateFormat', 'yyyy-MM-dd') \
            .option('fetchsize', 1000) \
            .load()

    def transform(self, df: DataFrame, exec_date: datetime):
        # add exec date into df
        return df.withColumn('exec_year', lit(exec_date.year)) \
            .withColumn('exec_month', lit(exec_date.month)) \
            .withColumn('exec_day', lit(exec_date.day))

    def load(self, df: DataFrame, table_name: str):
        row_exec = df.select(count('film_id')).collect()[0][0]
        file_path = f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}'
        df.write.option('header', True) \
            .partitionBy('exec_year', 'exec_month', 'exec_day') \
            .mode('overwrite') \
            .parquet(file_path)
        self.logger.info(
            f'Update data success in DataLake: {file_path} with {row_exec} row(s).')

    def run(self, table_name: str):
        arg_parser = ArgumentParser(description='Data Ingestion')
        arg_parser.add_argument(
            '--execution-date', dest='execution_date',
            type=str, help='Date Format: YYYY-MM-DD')
        args = arg_parser.parse_args()
        exec_date = datetime.fromisoformat(args.execution_date)

        df = self.extract(table_name=table_name, p_key=f'{table_name}_id')
        df = self.transform(df=df, exec_date=exec_date)
        self.load(df=df, table_name=table_name)
        self.stop()
