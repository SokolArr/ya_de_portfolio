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
    
    window = Window().partitionBy('event.message_id').orderBy(F.col('diff').asc())
    events_city = events_geo \
        .crossJoin(geo_city) \
        .withColumn('diff', calculate_diff)\
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number') \
        .persist()

    return events_city
        
def write_dm(df: pyspark.sql.DataFrame, dm_name: str) -> None:
    print(f'/user/axindri/analitics/{dm_name} - START')
    df.write.mode("overwrite") \
            .parquet(f'/user/axindri/analitics/{dm_name}')
    print(f'/user/axindri/analitics/{dm_name} - DONE!')
    show_df = spark.read.parquet(f'/user/axindri/analitics/{dm_name}').show(10)

# --------------------------------------------------------------------------------------------------------------
#  spark init
spark = get_spark_session(name='calculate dm_users')

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
#  last msg send
window_act_city = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
act_city_travel_count_df = events \
    .withColumn("row_number", F.row_number().over(window_act_city)) \
    .filter(F.col('row_number')==1) \
    .drop('row_number')
      
#  get travels
window_travel = Window().partitionBy('event.message_from', 'id').orderBy(F.col('date'))
travels = events \
    .withColumn("dense_rank", F.dense_rank().over(window_travel))\
    .withColumn("date_diff", F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'dd'))) \
    .selectExpr('date_diff', 'event.message_from as user_id', 'date', "id", "city" ) \
    .groupBy("user_id", "date_diff", "id", "city") \
    .agg(F.countDistinct(F.col('date')).alias('cnt_days'))

#  get travel list of city
travels_array = travels.groupBy("user_id") \
    .agg(F.collect_list('city').alias('travel_array')) \
    .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))

#  find lst city of user travel (travel days > 27)
user_home_cities_df = travels.filter((F.col('cnt_days')>27)) \
    .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id')))\
    .where(F.col('date_diff') == F.col('max_dt')) \
    .selectExpr('user_id', 'city as home_city')

#  calculate local time
local_time = events.withColumn("TIME",F.col("event.datetime").cast("Timestamp"))\
                   .withColumn("local_time",F.from_utc_timestamp(F.col("TIME"),F.col('timezone')))\
                   .select("TIME", "local_time", 'city', 'user_id')

#  get alias
events_df = events.alias('events')
act_city = act_city_travel_count_df.selectExpr('user_id', 'city as act_city', 'id as city_id').alias('act_city')
user_home = user_home_cities_df.alias('user_home')
travels_df = travels_array.alias('travels_df')
local_time_df = local_time.alias('local_time_df')

#  calculate dm
df_with_local_time = events.join(travels_df, ['user_id'], 'left') \
    .join(local_time_df, ['user_id'], 'left') \
    .join(act_city, ['user_id'], 'left') \
    .join(user_home, ['user_id'], 'left') \
    .selectExpr('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')

write_dm(df = df_with_local_time, dm_name = 'dm_users')

spark.stop()