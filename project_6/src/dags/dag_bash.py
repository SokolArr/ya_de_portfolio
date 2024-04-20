import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime
from airflow.models.baseoperator import chain
from airflow.operators.bash_operator import BashOperator
 
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'
 
default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}
 
dag_spark = DAG(
    dag_id = "dag_bash",
    default_args=default_args,
    schedule_interval=None,
)

dm_users = SparkSubmitOperator(
    task_id='dm_users',
    dag=dag_spark,
    application ='/home/nickname/dm_users.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/nickname/geo.csv", "/user/nickname/data/events"],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

dm_event_type_zone = SparkSubmitOperator(
    task_id='dm_event_type_zone',
    dag=dag_spark,
    application ='/home/nickname/dm_event_type_zone.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/nickname/geo.csv", "/user/nickname/data/events"],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)
dm_recomend_friend = SparkSubmitOperator(
    task_id='dm_recomend_friend',
    dag=dag_spark,
    application ='/home/nickname/dm_recomend_friend.py' ,
    conn_id= 'yarn_spark',
    application_args = ["/user/nickname/geo.csv", "/user/nickname/data/events"],
    conf={
    "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

dm_users >> dm_event_type_zone >> dm_recomend_friend