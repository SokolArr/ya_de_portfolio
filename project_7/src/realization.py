from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime
from settings import *

import logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def get_current_timestamp_utc() -> int:
    return int(round(datetime.utcnow().timestamp()))

def create_spark_session(app_name: str, spark_jars_packages) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate())

def read_kafka_stream(spark: SparkSession, kafka_settings, schema) -> DataFrame:    
    df = (spark.read
            .format('kafka')
            .options(**kafka_settings)
            .load())
    return df.select(f.from_json(f.col("value").cast(StringType()), schema).alias("parsed_key_value"))

def filter_stream_data(df, current_timestamp_utc) -> DataFrame:
    return df.select(f.col("parsed_key_value.*"))\
             .where((f.col("adv_campaign_datetime_start") < current_timestamp_utc) & (f.col("adv_campaign_datetime_end") > current_timestamp_utc))
  
def read_subscribers_data(spark: SparkSession, pg_settings) -> DataFrame:
    return (spark.read
                 .format('jdbc')
                 .options(**pg_settings)
                 .load())

def join_and_transform_data(filtered_data, subscribers_data) -> DataFrame:
    return filtered_data.join(subscribers_data, ['restaurant_id'])\
                    .withColumn('trigger_datetime_created', f.unix_timestamp(f.current_date(),"MM-dd-yyyy").cast(IntegerType()))\
                    .dropDuplicates()\
                    .drop('id')
                    
def write_to_postgresql(df, pg_settings):
    try:
        df = df.withColumn('feedback', f.lit(None).cast(StringType()))
        df.write.format('jdbc')\
            .options(**pg_settings) \
            .mode('append')\
            .save()
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")

def write_to_kafka(df, kafka_settings):
    try:
        df = df.select(f.to_json(f.struct(f.col('*'))).alias('value')).select('value')
        df.write.format('kafka')\
            .options(**kafka_settings)\
            .save()
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")
        
def save_to_postgresql_and_kafka(df, pg_settings, kafka_settings):
    df.persist()
    write_to_postgresql(df, pg_settings)
    write_to_kafka(df, kafka_settings)
    df.unpersist()
    
if __name__ == "__main__":
    spark = create_spark_session('Sprint_8_project', spark_jars_packages)
    restaurant_read_stream_df = read_kafka_stream(spark, kafka_read_settings, restaurant_schema)
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(spark, postgresql_read_settings)
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    save_to_postgresql_and_kafka(result_df, postgresql_write_settings, kafka_write_settings)
    spark.stop() 