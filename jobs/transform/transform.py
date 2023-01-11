from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, concat_ws, lit, to_date, when
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType

from dependencies.settings import DATALAKE_PATH, HDFS_MASTER
from jobs import BaseETL

CUSTOMER = 'dim_customer'
DATE = 'dim_date'
MOIVE = 'dim_moive'
SALE = 'sale'
STAFF = 'dim_staff'
STORE = 'dim_store'


class TransformETL(BaseETL):
    def __init__(self, exec_date: datetime = None) -> None:
        super().__init__(enable_hive=True)
        self.exec_date = exec_date or datetime.utcnow().date()

    def extract(self):
        self.address_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/address') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.city_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/city') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.country_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/country') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.customer_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/customer') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.film_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/film') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.inventory_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/inventory') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.language_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/language') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.rental_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/rental') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.payment_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/payment') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.staff_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/staff') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.store_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/store') \
            .load() \
            .drop('exec_year', 'exec_month', 'exec_day')

        self.location_df = self._extract_location()

    def _extract_location(self):
        return self.address_df \
            .join(self.city_df, self.address_df.city_id == self.city_df.city_id, 'inner') \
            .join(self.country_df, self.city_df.country_id == self.country_df.country_id, 'inner')

    def transform(self):
        self.logger.info('Transforming...')
        self.dim_movie = self._transform_movie()

    def _transform_movie(self):
        # join dataframe
        join_df = self.film_df.alias('f') \
            .join(self.language_df.alias('lang'), self.film_df.language_id == self.language_df.language_id, 'left') \
            .filter(to_date(col('f.last_update')) == self.exec_date)

        # transform
        movie_trans_df = join_df.withColumnRenamed('f.film_id', 'f.movie_id') \
            .withColumn('f.release_year',
                        when(col('f.release_year').isNull(), -1)
                        .otherwise(col('f.release_year').cast(IntegerType()))) \
            .withColumn('f.rental_duration', col('f.rental_duration').cast(IntegerType())) \
            .withColumn('f.rental_rate', col('f.rental_rate').cast(FloatType())) \
            .withColumn('f.length',
                        when(col('f.length').isNull(), -1)
                        .otherwise(col('f.length').cast(IntegerType()))) \
            .withColumn('f.replacement_cost', col('f.replacement_cost').cast(FloatType())) \
            .withColumn('f.rating', col('f.rating').cast(FloatType())) \
            .withColumn('f.last_update', col('f.last_update').cast(DateType())) \
            .withColumn('f.special_features', concat_ws(col('f.special_features'))) \
            .withColumn('f.language', when(col('lang.name').isNull(), 'unknow')
                        .otherwise(col('lang.name'))) \
            .select('f.movie_id', 'f.title', 'f.description', 'f.language', 'f.release_year', 'f.rental_duration', 'f.rental_rate',
                    'f.length', 'f.replacement_cost', 'f.rating', 'f.last_update', 'f.special_features', 'f.fulltext')
        return movie_trans_df

    def _transform_date(self):
        year = self.exec_date.year
        month = self.exec_date.month
        day = self.exec_date.day
        quarter = (self.exec_date - 1)/3 + 1
        day_of_week = self.exec_date.strftime('%A')
        date_id = int(f'{year}{month}{year}')
        df = self.spark.createDataFrame(
            data=[date_id, year, month, day, quarter, day_of_week],
            schema=('date_id', 'year', 'month', 'day', 'quarter', 'day_of_week'))
        return df

    def _transform_store(self):
        # join dataframe
        join_df = self.store_df.alias('sto') \
            .join(self.staff_df.alias('sta'), self.store_df.manager_staff_id == self.staff_df.staff_id, 'inner') \
            .join(self.location_df.alias('loc'), self.store_df.address_id == self.location_df.address_id) \
            .filter(to_date(col('sto.last_update')) == self.exec_date)

        # transform
        store_trans_df = join_df \
            .withColumn('manager_first_name', col('sta.first_name')) \
            .withColumn('manager_last_name', col('sta.last_name')) \
            .select('sto.store_id', 'manager_first_name', 'manager_last_name', 'sto.last_update',
                    'loc.address', 'loc.address2', 'loc.district', 'loc.city', 'loc.country')
        return store_trans_df

    def _transfrom_staff(self):
        # join dataframe
        join_df = self.staff_df.alias('sta') \
            .join(self.location_df.alias('loc'), self.staff_df.address_id == self.location_df.address_id, 'left') \
            .filter(to_date(col('sta.last_update')) == self.exec_date)

        # transform
        staff_trans_df = join_df \
            .withColumn('active', col('active').cast(BooleanType())) \
            .select('sto.staff_id', 'sto.first_name', 'sto.last_name', 'sto.email', 'sto.active',
                    'loc.address', 'loc.address2', 'loc.district', 'loc.city', 'loc.country')
        return staff_trans_df

    def _transfrom_customer(self):
        # join dataframe
        join_df = self.customer_df.alias('cus') \
            .join(self.location_df.alias('loc'), self.customer_df.address_id == self.location_df.address_id, 'left') \
            .filter(to_date(col('cus.last_update')) == self.exec_date)

        # transform
        customer_trans_df = join_df \
            .withColumn('active', col('active').cast(BooleanType())) \
            .select('cus.customer_id', 'cus.first_name', 'cus.last_name', 'cus.email', 'cus.active',
                    'loc.address', 'loc.address2', 'loc.district', 'loc.city', 'loc.country')
        return customer_trans_df

    def load(self, df: DataFrame, tb_name: str, partition_keys: list):
        row_exec = df.select(count('*')).collect()[0][0]
        df.write.option('header', True) \
            .partitionBy([col(key) for key in partition_keys]) \
            .mode('append') \
            .saveAsTable(f'dvd_rental.{tb_name}')
        self.logger.info(
            f'Load data success in DWH: dvd_rental.{tb_name} with {row_exec} row(s).')

    def run(self, tb_name: str, partition_keys: list):
        self.extract()
        self.transform()
        self.load(self.dim_movie, tb_name=tb_name,
                  partition_keys=partition_keys)
