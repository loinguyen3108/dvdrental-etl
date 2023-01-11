from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, count, dayofmonth, \
    dayofweek, dayofyear, month, quarter, to_date, when, year
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType

from etl.dependencies.settings import DATALAKE_PATH, HDFS_MASTER
from etl.jobs import BaseETL

CUSTOMER = 'dim_customer'
DATE = 'dim_date'
MOVIE = 'dim_movie'
SALES = 'sales'
STAFF = 'dim_staff'
STORE = 'dim_store'


class TransformETL(BaseETL):
    def __init__(self, exec_date: datetime = None) -> None:
        super().__init__(enable_hive=True)
        self.exec_date = exec_date or datetime.utcnow().date()

    def extract(self):
        self.address_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/address') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.city_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/city') \
            .drop('exec_year', 'exec_month', 'exec_day', 'last_update')
        self.country_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/country') \
            .drop('exec_year', 'exec_month', 'exec_day', 'last_update')
        self.customer_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/customer') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.film_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/film') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.inventory_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/inventory') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.language_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/language') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.rental_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/rental') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.payment_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/payment') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.staff_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/staff') \
            .drop('exec_year', 'exec_month', 'exec_day')
        self.store_df = self.spark.read \
            .parquet(f'{HDFS_MASTER}/{DATALAKE_PATH}/store') \
            .drop('exec_year', 'exec_month', 'exec_day')

        self.location_df = self._extract_location()

    def _extract_location(self):
        return self.address_df \
            .join(self.city_df, self.address_df.city_id == self.city_df.city_id, 'inner') \
            .join(self.country_df, self.city_df.country_id == self.country_df.country_id, 'inner') \
            .select('address_id', 'address', 'address2', 'district', 'city', 'country')

    def transform(self):
        self.logger.info('Transforming...')
        self.dim_customer = self._transfrom_customer()
        self.dim_date = self._transform_date()
        self.dim_movie = self._transform_movie()
        self.dim_store = self._transform_store()
        self.dim_staff = self._transfrom_staff()
        self.sales_df = self._transform_sales()

    def _transform_movie(self):
        # join dataframe
        join_df = self.film_df.alias('f') \
            .join(self.language_df.alias('lang'), self.film_df.language_id == self.language_df.language_id, 'left') \
            .filter(to_date(col('f.last_update')) == self.exec_date) \
            .drop(col('lang.last_update'))

        # transform
        movie_trans_df = join_df.withColumnRenamed('film_id', 'movie_id') \
            .withColumn('release_year',
                        when(col('release_year').isNull(), -1)
                        .otherwise(col('release_year').cast(IntegerType()))) \
            .withColumn('rental_duration', col('rental_duration').cast(IntegerType())) \
            .withColumn('rental_rate', col('rental_rate').cast(FloatType())) \
            .withColumn('length',
                        when(col('length').isNull(), -1)
                        .otherwise(col('length').cast(IntegerType()))) \
            .withColumn('replacement_cost', col('replacement_cost').cast(FloatType())) \
            .withColumn('rating', col('rating').cast(FloatType())) \
            .withColumn('last_update', col('last_update').cast(DateType())) \
            .withColumn('special_features', concat_ws(', ', col('special_features'))) \
            .withColumn('language', when(col('name').isNull(), 'unknow')
                        .otherwise(col('name'))) \
            .select('movie_id', 'title', 'description', 'language', 'release_year', 'rental_duration', 'rental_rate',
                    'length', 'replacement_cost', 'rating', 'last_update', 'special_features', 'fulltext')
        return movie_trans_df

    def _transform_date(self):
        beign_date = '1750-01-01'
        df = self.spark.sql(
            f"select explode(sequence(to_date('{beign_date}'), to_date('{self.exec_date}'), interval 1 day)) as date")
        df = df.withColumn('date_id',
                           (year('date') * 10000 + month('date') * 100 + dayofmonth('date')).cast(IntegerType())) \
            .withColumn('year', year('date')) \
            .withColumn('month', year('date')) \
            .withColumn('day', year('date')) \
            .withColumn('quarter', quarter('date')) \
            .withColumn('day_of_week', dayofweek('date')) \
            .withColumn('day_of_month', dayofmonth('date')) \
            .withColumn('day_of_year', dayofyear('date'))

        return df

    def _transform_store(self):
        # join dataframe
        join_df = self.store_df.alias('sto') \
            .join(self.staff_df.alias('sta'), self.store_df.manager_staff_id == self.staff_df.staff_id, 'inner') \
            .join(self.location_df.alias('loc'), self.store_df.address_id == self.location_df.address_id) \
            .filter(to_date(col('sto.last_update')) == self.exec_date) \
            .drop(col('sta.last_update')) \
            .drop(col('loc.last_update')) \
            .drop(col('sta.store_id'))

        # transform
        store_trans_df = join_df \
            .withColumn('manager_first_name', col('first_name')) \
            .withColumn('manager_last_name', col('last_name')) \
            .select('store_id', 'manager_first_name', 'manager_last_name', 'last_update',
                    'address', 'address2', 'district', 'city', 'country')
        return store_trans_df

    def _transfrom_staff(self):
        # join dataframe
        join_df = self.staff_df.alias('sta') \
            .join(self.location_df.alias('loc'), self.staff_df.address_id == self.location_df.address_id, 'left') \
            .filter(to_date(col('sta.last_update')) == self.exec_date) \
            .drop('loc.last_update')

        # transform
        staff_trans_df = join_df \
            .withColumn('active', col('active').cast(BooleanType())) \
            .select('staff_id', 'first_name', 'last_name', 'email', 'active',
                    'address', 'address2', 'district', 'city', 'country', 'last_update')
        return staff_trans_df

    def _transfrom_customer(self):
        # join dataframe
        join_df = self.customer_df.alias('cus') \
            .join(self.location_df.alias('loc'), self.customer_df.address_id == self.location_df.address_id, 'left') \
            .filter(to_date(col('cus.last_update')) == self.exec_date) \
            .drop(col('loc.last_update'))

        # transform
        customer_trans_df = join_df \
            .withColumn('active', col('active').cast(BooleanType())) \
            .select('customer_id', 'first_name', 'last_name', 'email', 'active',
                    'address', 'address2', 'district', 'city', 'country', 'last_update')
        return customer_trans_df

    def _transform_sales(self):
        # join dataframe
        join_df = self.inventory_df.alias('inv') \
            .join(self.rental_df.alias('ren'), self.inventory_df.inventory_id == self.rental_df.inventory_id, 'right') \
            .join(self.payment_df.alias('pay'), self.rental_df.rental_id == self.payment_df.rental_id, 'left') \
            .filter(to_date(col('ren.rental_date')) == self.exec_date) \
            .drop(col('pay.customer_id')) \
            .drop(col('pay.staff_id')) \
            .drop(col('pay.rental_id'))

        # transform
        sales_trans_df = join_df \
            .withColumnRenamed('rental_id', 'sale_id') \
            .withColumnRenamed('film_id', 'movie_id') \
            .withColumn('amount', col('amount').cast(IntegerType())) \
            .withColumn('rental_year', year('rental_date').cast(IntegerType())) \
            .withColumn('rental_month', month('rental_date').cast(IntegerType())) \
            .withColumn('rental_day', dayofmonth('rental_date').cast(IntegerType())) \
            .withColumn('rental_date_id',
                        (year('rental_date') * 10000 + month('rental_date') * 100 + dayofmonth('rental_date')).cast(IntegerType())) \
            .withColumn('return_date_id',
                        (year('return_date') * 10000 + month('return_date') * 100 + dayofmonth('return_date')).cast(IntegerType())) \
            .withColumn('payment_date_id',
                        (year('payment_date') * 10000 + month('payment_date') * 100 + dayofmonth('payment_date')).cast(IntegerType())) \
            .select('sale_id', 'movie_id', 'customer_id', 'store_id', 'staff_id', 'rental_year', 'rental_month', 'rental_day', 'rental_date_id',
                    'return_date_id', 'payment_date_id', 'amount')

        return sales_trans_df

    def load(self):
        self._load(self.dim_movie, tb_name=MOVIE,
                   partition_keys=['last_update'])
        self._load(self.dim_customer, tb_name=CUSTOMER,
                   partition_keys=['last_update'])
        self._load(self.dim_date, tb_name=DATE, save_mode='overwrite')
        self._load(self.dim_staff, tb_name=STAFF,
                   partition_keys=['last_update'])
        self._load(self.dim_store, tb_name=STORE,
                   partition_keys=['last_update'])
        self._load(self.sales_df, tb_name=SALES,
                   partition_keys=['rental_year', 'rental_month', 'rental_day'])

    def _load(self, df: DataFrame, tb_name: str, save_mode: str = 'append', partition_keys: list = []):
        self.logger.info('Loading...')
        row_exec = df.select(count('*')).collect()[0][0]
        df.write.option('header', True) \
            .format('hive') \
            .partitionBy(partition_keys) \
            .mode(save_mode) \
            .saveAsTable(f'dvd_rental.{tb_name}')
        self.logger.info(
            f'Load data success in DWH: dvd_rental.{tb_name} with {row_exec} row(s).')

    def run(self):
        self.extract()
        self.transform()
        self.load()
