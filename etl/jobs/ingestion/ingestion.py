from datetime import datetime
from functools import cached_property

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, lit, max, to_date

from etl.dependencies.settings import DATALAKE_PATH, HDFS_MASTER, \
    JDBC_PASSWORD, JDBC_URL, JDBC_USER
from etl.jobs import BaseETL
from etl.services.hdfs.hdfs import HDFSSerivce

BY_DATE = 'by_date'
BY_ID = 'by_id'


class Ingestion(BaseETL):

    def __init__(self) -> None:
        super().__init__()

    @cached_property
    def hdfs_service(self):
        return HDFSSerivce()

    def _extract_by_date(self, df: DataFrame, table_name: str, p_key: str = None):
        max_date = df.select(
            max(to_date(col(p_key))).alias('max_date')).collect()[0]['max_date']
        return f'(select * from public.{table_name} where date({p_key}) > date({max_date})) as tmp'

    def _extract_by_id(self, df: DataFrame, table_name: str, p_key: str = None):
        max_id = df.select(
            max(col(p_key)).alias('max_id')).collect()[0]['max_id']
        return f'(select * from public.{table_name} where {p_key} > {max_id}) as tmp'

    def extract(self, table_name: str, loading_type: str = None, p_key: str = None):
        if self.hdfs_service.is_exists(f'/{DATALAKE_PATH}/{table_name}'):
            df_exists = self.spark.read \
                .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}')
            loading_type = loading_type or BY_DATE
            p_key = loading_type or 'last_update'
            if loading_type == BY_ID:
                tb_query = self._extract_by_id(
                    df_exists, table_name=table_name, p_key=p_key)
            elif loading_type == BY_DATE:
                tb_query = self._extract_by_date(
                    df_exists, table_name=table_name, p_key=p_key)
            else:
                raise Exception(
                    f'Not support for loading type: {loading_type}')
        else:
            tb_query = f'public.{table_name}'
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
        row_exec = df.select(count('*')).collect()[0][0]
        file_path = f'{HDFS_MASTER}/{DATALAKE_PATH}/{table_name}'
        df.write.option('header', True) \
            .partitionBy('exec_year', 'exec_month', 'exec_day') \
            .mode('overwrite') \
            .parquet(file_path)
        self.logger.info(
            f'Ingest data success in DataLake: {file_path} with {row_exec} row(s).')

    def run(self, table_name: str, exec_date: datetime, loading_type: str = None, p_key: str = None):
        self.logger.info(f'Ingest data from (table={table_name}, exec_date={exec_date}), '
                         f'loading_type={loading_type}, p_key={p_key})...')
        df = self.extract(table_name=table_name,
                          loading_type=loading_type, p_key=p_key)
        if len(df.head(1)) == 0:
            return
        df = self.transform(df=df, exec_date=exec_date)
        self.load(df=df, table_name=table_name)
        self.stop()
