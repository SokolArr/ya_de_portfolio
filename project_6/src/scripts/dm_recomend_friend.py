import findspark
findspark.init()
findspark.find()
import pyspark.sql.functions as F 
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window
import sys
import os
import pyspark
from pyspark.sql import SparkSession
import datetime

path_geo = sys.argv[1]
path_geo_events = sys.argv[2]

# funcs
def get_spark_session(name=""):
    return SparkSession \
        .builder \
        .master("yarn")\
        .appName(f"{name}") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def get_city(events_geo, geo_city) -> pyspark.sql.DataFrame:

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
            F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
            F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
        )
    )
    
    window = Window().partitionBy('event_id').orderBy(F.col('diff').asc())
    events_city = events_geo \
        .crossJoin(geo_city) \
        .withColumn('diff', calculate_diff)\
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number') \
        .persist()

    return events_city

def get_user_list_distance_between(user_list) -> pyspark.sql.DataFrame:

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat_user1")) - F.radians(F.col("lat_user2"))) / 2), 2) +
            F.cos(F.radians(F.col("lat_user1"))) * F.cos(F.radians(F.col("lat_user2"))) *
            F.pow(F.sin((F.radians(F.col("lon_user1")) - F.radians(F.col("lon_user2"))) / 2), 2)
        )
    )
    # kilometers
    user_list = user_list \
        .withColumn('diff', calculate_diff)\
        .filter(F.col('diff') < 1) \
        .drop('diff')\
        .persist()

    return user_list
        
def write_dm(df: pyspark.sql.DataFrame, dm_name: str) -> None:
    print(f'/user/axindri/analitics/{dm_name} - START')
    df.write.mode("overwrite") \
            .parquet(f'/user/axindri/analitics/{dm_name}')
    print(f'/user/axindri/analitics/{dm_name} - DONE!')
    show_df = spark.read.parquet(f'/user/axindri/analitics/{dm_name}').show(10)

# --------------------------------------------------------------------------------------------------------------
#  spark init
spark = get_spark_session(name='calculate dm_recomend_friend')

#  get events with geo
events_geo = spark.read.parquet(path_geo_events) \
    .sample(1.0) \
    .withColumn('user_id', F.col('event.message_from'))\
    .withColumnRenamed('lat', 'msg_lat')\
    .withColumnRenamed('lon', 'msg_lon')\
    .withColumn('event_id', F.monotonically_increasing_id())

#  get city with geo
geo_city = spark.read.csv(path_geo, sep=';',inferSchema=True, header=True)
geo_city = geo_city.withColumn('lat', F.regexp_replace('lat', ',', '.')) \
    .withColumn('lat', F.col('lat').cast(FloatType())) \
    .withColumn('lng', F.regexp_replace('lng', ',', '.')) \
    .withColumn('lng', F.col('lng').cast(FloatType())) \
    .withColumnRenamed('lat', "city_lat")\
    .withColumnRenamed('lng', "city_lon")

#  calculate event city
events = get_city(
    events_geo=events_geo,
    geo_city=geo_city
)

w_l = Window.partitionBy('user_id').orderBy(F.col('event.message_ts').desc())
view_last = events.where('msg_lon is not null') \
    .withColumn("rn",F.row_number().over(Window().partitionBy('user_id').orderBy(F.col('event.message_ts').desc()))) \
    .filter(F.col('rn') == 1) \
    .drop(F.col('rn')) \
    .withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
    .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
    .selectExpr('user_id', 'msg_lon as lon', 'msg_lat as lat')

view_last_channel = events.select(
    F.col('event.subscription_channel').alias('channel'),
    F.col('event.user').alias('user_id')
).distinct()

new = view_last_channel.join(view_last_channel.withColumnRenamed('user_id', 'user_id2'), ['channel'], 'inner') \
    .filter('user_id < user_id2')
    
user_list = new.join(view_last,['user_id'],'inner') \
    .withColumnRenamed('lon','lon_user1') \
    .withColumnRenamed('lat','lat_user1') \
    .drop('id').drop('local_time')

user_list = user_list\
    .join(view_last, view_last['user_id'] == user_list['user_id2'], 'inner').drop(view_last['user_id']) \
    .withColumnRenamed('lon','lon_user2') \
    .withColumnRenamed('lat','lat_user2')\
    .withColumn("processed_dttm", F.current_timestamp())

user_list = get_user_list_distance_between(user_list)

events = events.filter('event_type = "message"')\
               .select('event.message_from','event.message_to')
contacters = events.unionByName(events["event.message_from"], events["event.message_to"]).alias("user_id")   
                   
user_list = contacters\
            .join(user_list, user_list['user_id'] == contacters['user_id'], 'left_anti').drop(contacters['user_id'])\
            .selectExpr('user_id as user_left', 'user_id2 as user_right', 'processed_dttm', 'id as zone_id', 'local_time')     

write_dm(df = user_list, dm_name = 'dm_recomend_friend')

spark.stop()
