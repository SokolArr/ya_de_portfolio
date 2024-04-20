import requests
import json
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger('airflow.task')

# подключение к ресурсам
# api_conn = BaseHook.get_connection('api_conn')
postgres_conn = 'PG_DWH'
dwh_hook = PostgresHook(postgres_conn)

# параметры API
nickname = ''
cohort = ''
api_key = ''
base_url = ''

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key,
         }
         
def upload_couriers(pg_schema, pg_table, **context):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

   
    offset = 0
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break
        
        content = []
        for i in range(len(couriers_rep)):
            value_to_ins = [json.dumps(couriers_rep[i], ensure_ascii=False), context['ts']]
            content.append(value_to_ins)

        sql = f""" INSERT INTO {pg_schema}.{pg_table} (content, load_ts) VALUES %s"""
        execute_values(cursor, sql, content)
        
        offset += len(couriers_rep)  

def upload_restaurants(pg_schema, pg_table, **context):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")

   
    offset = 0
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/restaurants/?sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break
        
        content = []
        for i in range(len(couriers_rep)):
            value_to_ins = [json.dumps(couriers_rep[i], ensure_ascii=False), context['ts']]
            content.append(value_to_ins)

        sql = f""" INSERT INTO {pg_schema}.{pg_table} (content, load_ts) VALUES %s"""
        execute_values(cursor, sql, content)
        
        offset += len(couriers_rep)

def upload_deliveries(pg_schema, pg_table, **context):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()
    
    # идемпотентность
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table}")
    
    offset = 0
    dag_start_dt = context['ds']
    while True:    
        couriers_rep = requests.get(f'https://{base_url}/deliveries/?from={dag_start_dt} 00:00:00&to={dag_start_dt} 23:59:59&sort_field=_id&sort_direction=asc&offset={offset}',
                            headers = headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break
        
        content = []
        for i in range(len(couriers_rep)):
            value_to_ins = [json.dumps(couriers_rep[i], ensure_ascii=False), context['ts']]
            content.append(value_to_ins)

        sql = f""" INSERT INTO {pg_schema}.{pg_table} (content, load_ts) VALUES %s"""
        execute_values(cursor, sql, content)
        
        offset += len(couriers_rep)

default_args = {
    'owner':'airflow',
    'retries':0,
    'retry_delay': timedelta (seconds = 60)
}


dag = DAG('dwh_dag',
        start_date=datetime(2023, 11, 30),
        catchup=True,
        schedule_interval='@daily',
        max_active_runs=1,
        default_args=default_args)

with TaskGroup(group_id = 'init_stg_dds_cdm', dag=dag) as init_stg_dds_cdm:
    init_stg = PostgresOperator(
                task_id = 'init_stg',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/init/init_stg.sql",
                dag = dag
    )
    init_dds = PostgresOperator(
                task_id = 'init_dds',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/init/init_dds.sql",
                dag = dag
    )
    init_cdm = PostgresOperator(
                task_id = 'init_cdm',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/init/init_cdm.sql",
                dag = dag
    )

with TaskGroup(group_id = 'upload_stg', dag=dag) as upload_stg:
    upload_couriers = PythonOperator(
                task_id = 'stg_couriers',
                python_callable = upload_couriers,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'api_couriers'
                },
                dag = dag
    )
    upload_restaurants  = PythonOperator(
                task_id = 'stg_restaurants',
                python_callable = upload_restaurants,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'api_restaurants'
                },
                dag = dag
    )
    upload_deliveries  = PythonOperator(
                task_id = 'stg_deliveries',
                python_callable = upload_deliveries,
                op_kwargs = {
                    'pg_schema' : 'stg',
                    'pg_table' : 'api_deliveries'
                },
                dag = dag
    )

with TaskGroup(group_id = 'upload_dds_1', dag=dag) as upload_dds_1:
    dm_api_couriers = PostgresOperator(
                task_id = 'dm_api_couriers',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__dm_api_couriers.sql",
                dag = dag
    )
    dm_api_address = PostgresOperator(
                task_id = 'dm_api_address',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__dm_api_address.sql",
                dag = dag
    )
    dm_api_orders = PostgresOperator(
                task_id = 'dm_api_orders',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__dm_api_orders.sql",
                dag = dag
    )
    dm_api_restaurants = PostgresOperator(
                task_id = 'dm_api_restaurants',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__dm_api_restaurants.sql",
                dag = dag
    )
    
with TaskGroup(group_id = 'upload_dds_2', dag=dag) as upload_dds_2:
    dm_api_delivery_details = PostgresOperator(
                task_id = 'dm_api_delivery_details',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__dm_api_delivery_details.sql",
                dag = dag
    )
    
with TaskGroup(group_id = 'upload_dds_3', dag=dag) as upload_dds_3:
    fct_api_sales = PostgresOperator(
                task_id = 'fct_api_sales',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/dds/dds__fct_api_sales.sql",
                dag = dag
    )
    
with TaskGroup(group_id = 'upload_cdm', dag=dag) as upload_cdm:
    dm_courier_ledger = PostgresOperator(
                task_id = 'dm_courier_ledger',
                postgres_conn_id = 'PG_DWH',
                sql = "sql/cdm/cdm__dm_courier_ledger.sql",
                dag = dag
    )


init_stg_dds_cdm >> upload_stg >> upload_dds_1  >> upload_dds_2 >> upload_dds_3 >> upload_cdm