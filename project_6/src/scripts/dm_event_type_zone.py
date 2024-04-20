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
        
def write_dm(df: pyspark.sql.DataFrame, dm_name: str) -> None:
    print(f'/user/axindri/analitics/{dm_name} - START')
    df.write.mode("overwrite") \
            .parquet(f'/user/axindri/analitics/{dm_name}')
    print(f'/user/axindri/analitics/{dm_name} - DONE!')
    show_df = spark.read.parquet(f'/user/axindri/analitics/{dm_name}').show(10)

# --------------------------------------------------------------------------------------------------------------
#  spark init
spark = get_spark_session(name='calculate dm_event_type_zone')

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
w_week = Window.partitionBy(['city', F.trunc(F.col("date"), "week")])
w_month = Window.partitionBy(['city', F.trunc(F.col("date"), "month")])

dm_event_type_zone = events\
        .withColumn('week', F.trunc(F.col("date"), "week"))\
        .withColumn('month', F.trunc(F.col("date"), "month"))\
        .withColumn('week_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_week))\
        .withColumn('month_message', F.count(F.when(events.event_type == 'message','event_id')).over(w_month))\
        .withColumn('week_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_week))\
        .withColumn('month_reaction', F.count(F.when(events.event_type == 'reaction','event_id')).over(w_month))\
        .withColumn('week_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_week))\
        .withColumn('month_subscription', F.count(F.when(events.event_type == 'subscription','event_id')).over(w_month))\
        .withColumn('week_user', F.size(F.collect_set("event.message_from").over(w_week)))\
        .withColumn('month_user', F.size(F.collect_set("event.message_from").over(w_month)))\
        .selectExpr('date', 'id as zone_id',
                    'week_message','week_reaction','week_subscription', 'week_user',
                    'month_message','month_reaction','month_subscription', 'month_user')\
        .dropDuplicates()

write_dm(df = dm_event_type_zone, dm_name = 'dm_event_type_zone')

spark.stop()